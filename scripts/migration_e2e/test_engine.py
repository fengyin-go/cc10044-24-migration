#!/usr/bin/env python3
"""迁移引擎单元测试：纯函数解析边界 + 门禁/对账/双读状态机。

运行：python3 -I scripts/migration_e2e/test_engine.py
不依赖 pytest，用标准库 unittest，方便在最小环境直接跑。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2] / "backend"
sys.path.insert(0, str(BACKEND))

from app.migration import MigrationEngine  # noqa: E402
from app.migration.catalog_defs import (  # noqa: E402
    GLUE_COLUMNS_V2,
    HIVE_COLUMNS_V1,
    HISTORICAL_ROWS,
    TABLE,
)
from app.migration.timestamp import parse_event_timestamp  # noqa: E402


class TimestampParserTest(unittest.TestCase):
    def test_positive_offsets_normalize_to_utc(self):
        r1 = parse_event_timestamp("2026-11-23 08:15:30.123456+08:00")
        r2 = parse_event_timestamp("2026-11-23T05:45:30.654321+05:30")
        self.assertTrue(r1.ok and r2.ok)
        self.assertEqual(r1.iso_utc, "2026-11-23T00:15:30.123456Z")
        self.assertEqual(r2.iso_utc, "2026-11-23T00:15:30.654321Z")
        self.assertEqual(r1.offset, "+08:00")
        self.assertEqual(r2.offset, "+05:30")

    def test_negative_and_compact_offsets(self):
        self.assertEqual(parse_event_timestamp(
            "2026-11-23T08:15:30-03:00").iso_utc, "2026-11-23T11:15:30Z")
        self.assertEqual(parse_event_timestamp(
            "2026-11-23T08:15:30+0530").iso_utc, "2026-11-23T02:45:30Z")

    def test_subsecond_precision_preserved_and_nanos_truncated(self):
        ms = parse_event_timestamp("2026-11-23T00:15:30.123Z")
        self.assertEqual(ms.iso_utc, "2026-11-23T00:15:30.123Z")
        ns = parse_event_timestamp("2026-11-23T00:15:30.123456789Z")
        self.assertEqual(ns.iso_utc, "2026-11-23T00:15:30.123456Z")
        self.assertTrue(ns.truncated_nanos)
        no_frac = parse_event_timestamp("2026-11-23T08:15:30Z")
        self.assertEqual(no_frac.iso_utc, "2026-11-23T08:15:30Z")

    def test_null_variants_preserved_as_null(self):
        for raw in (None, "", "  ", "NULL", "null"):
            r = parse_event_timestamp(raw)
            self.assertTrue(r.ok and r.is_null and r.iso_utc is None, raw)

    def test_unconvertible_text_rejected_not_guessed(self):
        for raw in ("not-a-time", "2026-02-30T00:00:00Z",
                    "2026-13-99 25:61:61 bad-tz"):
            r = parse_event_timestamp(raw)
            self.assertFalse(r.ok)
            self.assertIsNotNone(r.error)
            self.assertEqual(r.raw, raw)  # 原文留痕


class MigrationEngineTest(unittest.TestCase):
    def _new_engine(self) -> MigrationEngine:
        engine = MigrationEngine(TABLE)
        engine.register_version("v1_hive_string", "hive_metastore_prod", HIVE_COLUMNS_V1)
        engine.register_version("v2_glue_timestamp", "aws_glue_catalog",
                                GLUE_COLUMNS_V2, parent_version="v1_hive_string")
        engine.load_source_rows(HISTORICAL_ROWS)
        return engine

    def test_unverified_version_cannot_reconcile(self):
        engine = self._new_engine()
        with self.assertRaises(PermissionError):
            engine.reconcile_schema("v1_hive_string", "v2_glue_timestamp")

    def test_verification_classifies_all_rows_and_quarantines_bad_one(self):
        engine = self._new_engine()
        report = engine.verify_version("v2_glue_timestamp", "event_timestamp")
        self.assertTrue(report.passed)
        self.assertEqual((report.total_rows, report.convertible_rows,
                          report.null_rows, report.unconvertible_rows), (7, 4, 2, 1))
        self.assertEqual(engine.versions["v2_glue_timestamp"].status, "verified")
        self.assertEqual({q["row_id"] for q in engine.quarantine.values()}, {"r7"})

    def test_reconcile_eligible_and_backlog(self):
        engine = self._new_engine()
        engine.verify_version("v2_glue_timestamp", "event_timestamp")
        engine.verify_version("v1_hive_string", "event_timestamp")
        result = engine.reconcile_schema("v1_hive_string", "v2_glue_timestamp")
        self.assertIn("event_timestamp", result["eligible_columns"])
        self.assertEqual(
            [i["column"] for i in result["backlog"]], ["ingest_run_id"])
        self.assertTrue(engine.reconciled)

    def test_cutover_requires_reconciliation(self):
        engine = self._new_engine()
        engine.verify_version("v2_glue_timestamp", "event_timestamp")
        engine.verify_version("v1_hive_string", "event_timestamp")
        with self.assertRaises(PermissionError):
            engine.cutover("v2_glue_timestamp")

    def test_dual_reads_and_cutover_rollback_cycle(self):
        engine = self._new_engine()
        engine.verify_version("v2_glue_timestamp", "event_timestamp")
        engine.verify_version("v1_hive_string", "event_timestamp")
        engine.reconcile_schema("v1_hive_string", "v2_glue_timestamp")

        legacy = {r["row_id"]: r for r in engine.legacy_read()}
        glue = {r["row_id"]: r for r in engine.glue_read("v2_glue_timestamp")}
        self.assertEqual(len(legacy), 7)          # 旧报表看全量原文
        self.assertEqual(len(glue), 6)            # 隔离行不进 Glue
        self.assertIsNone(glue["r5"]["event_timestamp"])
        self.assertEqual(glue["r1"]["event_timestamp__glue_type"], "TIMESTAMP")

        engine.cutover("v2_glue_timestamp")
        self.assertEqual(engine.active_version_id, "v2_glue_timestamp")
        self.assertEqual(engine.versions["v1_hive_string"].status, "retired")

        back = engine.rollback("v1_hive_string")
        self.assertTrue(back["all_match"])
        self.assertEqual(engine.active_version_id, "v1_hive_string")
        self.assertEqual(
            {t["column"]: t["restored_type"] for t in back["type_checks"]}["event_timestamp"],
            "STRING")
        # 回切后两条时区偏移数据逐字符还原
        legacy_after = {r["row_id"]: r for r in engine.legacy_read()}
        self.assertEqual(legacy_after["r1"]["event_timestamp"],
                         HISTORICAL_ROWS[0]["event_timestamp"])
        self.assertEqual(legacy_after["r2"]["event_timestamp"],
                         HISTORICAL_ROWS[1]["event_timestamp"])

    def test_rejected_version_blocks_cutover(self):
        engine = self._new_engine()
        engine.verify_version("v2_glue_timestamp", "event_timestamp")
        engine.verify_version("v1_hive_string", "event_timestamp")
        engine.reconcile_schema("v1_hive_string", "v2_glue_timestamp")
        engine.versions["v2_glue_timestamp"].status = "rejected"
        engine.versions["v2_glue_timestamp"].verification_ref = None
        with self.assertRaises((ValueError, PermissionError)):
            engine.cutover("v2_glue_timestamp")


if __name__ == "__main__":
    unittest.main(verbosity=2)
