#!/usr/bin/env python3
"""Hive Metastore → Glue 行为日志字段迁移：端到端切换/回切演练。

演练顺序严格对应任务要求：
  1. 登记可回切目录字段版本（v1 Hive STRING / v2 Glue TIMESTAMP）
  2. 历史数据逐行核验：旧文本表现、新类型解析、无法转换行分别留痕
  3. 核验门禁：只有 verified 版本能进入变更对账
  4. Schema 变更对账：已核验版本给出可用字段列，差异列带差异留待对账清单
  5. 两套读取比对：旧报表输入（原时间文本）vs 新作业输入（时间类型）
  6. 用两条带不同时区偏移的行为数据发起一次切换
  7. 回切并逐项核验字段类型与实际值

脚本只依赖标准库，使用独立引擎实例（与平台服务同一份引擎代码）。
运行：python3 -I scripts/migration_e2e/run_e2e.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# 允许直接从仓库根目录运行，复用后端引擎代码
BACKEND = Path(__file__).resolve().parents[2] / "backend"
sys.path.insert(0, str(BACKEND))

from app.migration import MigrationEngine  # noqa: E402
from app.migration.catalog_defs import (  # noqa: E402
    GLUE_COLUMNS_V2,
    HIVE_COLUMNS_V1,
    HISTORICAL_ROWS,
    TABLE,
)

PASS = "PASS"
FAIL = "FAIL"
checks: list[tuple[str, str, str]] = []


def check(name: str, condition: bool, evidence: str = "") -> None:
    checks.append((PASS if condition else FAIL, name, evidence))
    mark = "✅" if condition else "❌"
    print(f"  {mark} {name}" + (f" —— {evidence}" if evidence else ""))
    if not condition:
        raise AssertionError(f"核验失败: {name} | {evidence}")


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def main() -> int:
    engine = MigrationEngine(TABLE)

    # ------------------------------------------------------------------
    section("步骤1 登记可回切的目录字段版本（v1 Hive STRING / v2 Glue TIMESTAMP）")
    v1 = engine.register_version(
        "v1_hive_string", "hive_metastore_prod", HIVE_COLUMNS_V1,
        note="Hive 现状：event_timestamp STRING，旧报表依赖原文本")
    v2 = engine.register_version(
        "v2_glue_timestamp", "aws_glue_catalog", GLUE_COLUMNS_V2,
        parent_version="v1_hive_string",
        note="Glue 目标：event_timestamp TIMESTAMP，新作业要求时间类型")
    engine.load_source_rows(HISTORICAL_ROWS)
    check("两个字段版本均已登记", set(engine.versions) == {"v1_hive_string", "v2_glue_timestamp"})
    check("新版本登记初始状态为 registered（未核验）",
          v1.status == "registered" and v2.status == "registered")
    check("v2 挂接父版本 v1，回切链完整", v2.parent_version == "v1_hive_string")
    hive_ts = next(c for c in v1.columns if c.name == "event_timestamp")
    glue_ts = next(c for c in v2.columns if c.name == "event_timestamp")
    check("完整性校验差异：event_timestamp 两侧为 TIMESTAMP vs STRING",
          hive_ts.type_name == "STRING" and glue_ts.type_name == "TIMESTAMP",
          f"Hive={hive_ts.type_name} / Glue={glue_ts.type_name}")

    # ------------------------------------------------------------------
    section("步骤2 历史数据逐行核验：旧文本 / 新类型解析 / 不可转换行")
    # 门禁前置证明：未核验版本不得进入对账
    try:
        engine.reconcile_schema("v1_hive_string", "v2_glue_timestamp")
        check("未核验版本被对账门禁拒绝", False, "未抛出 PermissionError")
    except PermissionError as exc:
        check("未核验版本被对账门禁拒绝（「已同步」不构成证据）", True, str(exc)[:48])

    report = engine.verify_version("v2_glue_timestamp", "event_timestamp")
    print("\n逐行核验明细：")
    print(f"  {'row':4} {'旧文本表现':<34} {'新类型解析(UTC)':<28} 结论")
    for r in report.row_results:
        if r.null_preserved:
            verdict = "空值->NULL保留"
        elif not r.convertible:
            verdict = f"不可转换隔离: {r.parse_error[:24]}"
        else:
            verdict = "可转换"
        print(f"  {r.row_id:4} {str(r.raw_text):<34} {str(r.parsed_iso):<28} {verdict}")

    check("核验报告状态 passed（所有行都有确定结论）", report.passed, report.detail)
    check("版本状态由 registered 流转为 verified",
          engine.versions["v2_glue_timestamp"].status == "verified")
    check("v2 已登记核验报告引用（验证证据落版本）",
          engine.versions["v2_glue_timestamp"].verification_ref == report.report_id)
    check("总数核对：7 行 = 可转换4 + 空值2 + 不可转换1",
          report.total_rows == 7 and report.convertible_rows == 4
          and report.null_rows == 2 and report.unconvertible_rows == 1,
          report.detail)

    # 两条不同时区偏移数据：同一物理时刻归一化到同一 UTC 值
    r1 = next(r for r in report.row_results if r.row_id == "r1")
    r2 = next(r for r in report.row_results if r.row_id == "r2")
    check("时区偏移 +08:00 正确归一化",
          r1.parsed_iso == "2026-11-23T00:15:30.123456Z", r1.parsed_iso)
    check("时区偏移 +05:30 正确归一化",
          r2.parsed_iso == "2026-11-23T00:15:30.654321Z", r2.parsed_iso)
    check("两条偏移不同的文本保留各自亚秒精度（微秒）",
          r1.parsed_iso.endswith("123456Z") and r2.parsed_iso.endswith("654321Z"))
    r3 = next(r for r in report.row_results if r.row_id == "r3")
    check("毫秒精度(3位)正确解析", r3.parsed_iso == "2026-11-23T00:15:30.123Z",
          r3.parsed_iso)
    r4 = next(r for r in report.row_results if r.row_id == "r4")
    from app.migration.timestamp import parse_event_timestamp
    p4 = parse_event_timestamp(HISTORICAL_ROWS[3]["event_timestamp"])
    check("纳秒(9位)按 Spark 微秒粒度截断并留痕",
          r4.parsed_iso == "2026-11-23T00:15:30.123456Z" and p4.truncated_nanos,
          f"{r4.parsed_iso} truncated={p4.truncated_nanos}")
    r5, r6 = (next(r for r in report.row_results if r.row_id == x) for x in ("r5", "r6"))
    check("SQL NULL 与空串均保留 NULL 语义（未变成字符串）",
          r5.null_preserved and r6.null_preserved
          and r5.parsed_iso is None and r6.parsed_iso is None)
    r7 = next(r for r in report.row_results if r.row_id == "r7")
    check("非法文本判定为不可转换", not r7.convertible and r7.parse_error)
    quarantined_rows = {q["row_id"] for q in engine.quarantine.values()}
    check("不可转换行进入隔离区且只隔离 r7（v2 核验证据）",
          quarantined_rows == {"r7"}
          and any(q["report_id"] == report.report_id for q in engine.quarantine.values()),
          str([(q["report_id"], q["row_id"]) for q in engine.quarantine.values()]))

    # v1 同样核验，使两个版本都满足对账前置
    report_v1 = engine.verify_version("v1_hive_string", "event_timestamp")
    check("v1 字段版本核验通过并可作为回切证据",
          report_v1.passed and engine.versions["v1_hive_string"].status == "verified")

    # ------------------------------------------------------------------
    section("步骤3 Schema 变更对账：目录字段版本给出本次可用字段列")
    recon = engine.reconcile_schema("v1_hive_string", "v2_glue_timestamp")
    print("\n对账结果：")
    print("  本次可用字段列:", recon["eligible_columns"])
    print("  待对账清单:")
    for item in recon["backlog"]:
        print(f"    - {item['column']}: {item['diff']} | {item['reason']}")

    check("对账证据引用两个已核验版本的报告",
          all(recon["evidence_reports"]), str(recon["evidence_reports"]))
    check("event_timestamp 类型差异列已随核验版本放行（可用）",
          "event_timestamp" in recon["eligible_columns"])
    check("类型一致列直接可用",
          {"event_id", "user_id", "event_type", "dt", "event_timestamp"}
          <= set(recon["eligible_columns"]))
    check("Glue 多出的 ingest_run_id 带差异留在待对账清单",
          any(i["column"] == "ingest_run_id" and "Hive 目录缺列" in i["reason"]
              for i in recon["backlog"]))
    check("待对账清单被持久化保留（不被切换抹掉）",
          len(engine.reconcile_backlog) == 1
          and engine.reconcile_backlog[0].column == "ingest_run_id")

    # ------------------------------------------------------------------
    section("步骤4 同一表两套读取：旧报表文本输入 vs Glue 新作业类型输入")
    legacy = {r["row_id"]: r for r in engine.legacy_read()}
    glue = {r["row_id"]: r for r in engine.glue_read("v2_glue_timestamp")}
    print("\n两套读取比对（切换演练两条 + 边界行）：")
    print(f"  {'row':4} {'旧报表输入(STRING 原文)':<34} {'新作业输入(TIMESTAMP UTC)':<28}")
    for rid in ("r1", "r2", "r4", "r5", "r7"):
        g = glue.get(rid)
        g_val = "<隔离,不写入>" if g is None else str(g.get("event_timestamp"))
        print(f"  {rid:4} {str(legacy[rid]['event_timestamp']):<34} {g_val:<28}")

    check("旧报表仍拿到 Hive 原时间文本，含义不变",
          legacy["r1"]["event_timestamp"] == "2026-11-23 08:15:30.123456+08:00"
          and legacy["r2"]["event_timestamp"] == "2026-11-23T05:45:30.654321+05:30")
    check("新作业拿到 TIMESTAMP 类型值且带类型标记",
          glue["r1"]["event_timestamp"] == "2026-11-23T00:15:30.123456Z"
          and glue["r1"]["event_timestamp__glue_type"] == "TIMESTAMP")
    check("新作业视图中空值仍为 NULL", glue["r5"]["event_timestamp"] is None)
    check("不可转换行不进入 Glue 新作业输入", "r7" not in glue)
    check("两套读取行数：旧报表7行 / 新作业6行（1行隔离）",
          len(legacy) == 7 and len(glue) == 6)

    # ------------------------------------------------------------------
    section("步骤5 用两条带不同时区偏移的行为数据发起一次切换（Hive → Glue）")
    cut = engine.cutover("v2_glue_timestamp")
    check("切换后生效版本为 v2_glue_timestamp",
          engine.active_version_id == "v2_glue_timestamp", str(cut))
    check("v2 状态为 active，v1 被置为 retired 但保留可回切",
          engine.versions["v2_glue_timestamp"].status == "active"
          and engine.versions["v1_hive_string"].status == "retired"
          and engine.versions["v1_hive_string"].verification_ref is not None)
    # 切换后再次比对两套读取，确认切换只改目录版本不改数据语义
    legacy2 = {r["row_id"]: r for r in engine.legacy_read()}
    glue2 = {r["row_id"]: r for r in engine.glue_read("v2_glue_timestamp")}
    check("切换后旧报表输入与切换前完全一致",
          legacy2 == legacy)
    check("切换后新作业输入与切换前完全一致",
          glue2 == glue)
    check("待对账清单在切换后仍然保留",
          len(engine.reconcile_backlog) == 1)

    # 反例：被拒绝的版本不允许切换（构造一个 rejected 版本验证门禁）
    bad_engine = MigrationEngine(TABLE)
    bad_engine.register_version("vx", "x", HIVE_COLUMNS_V1)
    bad_engine.load_source_rows(HISTORICAL_ROWS)
    bad_engine.verify_version("vx", "event_timestamp")
    # 当前数据全部有结论，因此手工构造 rejected 场景验证门禁逻辑
    bad_engine.versions["vx"].status = "rejected"
    bad_engine.versions["vx"].verification_ref = None
    try:
        bad_engine.cutover("vx")
        check("rejected 版本禁止切换", False, "未抛出异常")
    except (ValueError, PermissionError) as exc:
        check("rejected 版本禁止切换（核验门禁生效）", True, str(exc)[:40])

    # ------------------------------------------------------------------
    section("步骤6 回切到 Hive v1，并逐项核验字段类型与实际值")
    back = engine.rollback("v1_hive_string")
    print("\n回切逐项核验：")
    for tc in back["type_checks"]:
        print(f"  类型: {tc['column']:<18} 恢复为 {tc['restored_type']:<10} "
              f"匹配v1={tc['matches_v1']}")
    mismatches = [v for v in back["value_checks"] if not v["match"]]
    print(f"  值核验: {len(back['value_checks'])} 项，不一致 {len(mismatches)} 项")

    check("回切后生效版本恢复为 v1_hive_string",
          engine.active_version_id == "v1_hive_string")
    check("v1 恢复 active，v2 置 retired（版本均未删除，可再次切换）",
          engine.versions["v1_hive_string"].status == "active"
          and engine.versions["v2_glue_timestamp"].status == "retired")
    check("字段类型逐项还原为 v1 登记类型",
          all(t["matches_v1"] for t in back["type_checks"])
          and {t["column"]: t["restored_type"] for t in back["type_checks"]}
          == {c.name: c.type_name for c in HIVE_COLUMNS_V1})
    check("实际值逐项等于 Hive 原文本（含两条时区偏移数据）",
          back["all_match"] and not mismatches)
    # 回切后旧报表两条演练数据值与最初完全一致
    legacy3 = {r["row_id"]: r for r in engine.legacy_read()}
    check("回切后 r1/r2 时间文本与迁移前逐字符一致",
          legacy3["r1"]["event_timestamp"] == HISTORICAL_ROWS[0]["event_timestamp"]
          and legacy3["r2"]["event_timestamp"] == HISTORICAL_ROWS[1]["event_timestamp"])
    check("回切后空值仍为 NULL、隔离行仍在隔离区",
          legacy3["r5"]["event_timestamp"] is None
          and {q["row_id"] for q in engine.quarantine.values()} == {"r7"})

    # ------------------------------------------------------------------
    section(f"演练汇总：{sum(1 for c in checks if c[0] == PASS)}/{len(checks)} 项核验通过")
    for status, name, _ in checks:
        print(f"  {status}  {name}")
    failed = [c for c in checks if c[0] == FAIL]
    print(f"\n结论: {'全部核验通过，切换/回切闭环成立' if not failed else f'{len(failed)} 项失败'}")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
