"""Hive→Glue 行为日志字段迁移领域服务。

把迁移引擎挂到平台服务层，供 API 与 E2E 演练共用同一个引擎实例。
与通用 CRUD 服务不同，这里的状态流转全部由引擎的核验门禁控制：
未核验字段版本不能对账，对账未完成不能切换，回切逐项核验类型与值。
"""
from __future__ import annotations

from typing import Any

from app.migration import MigrationEngine
from app.migration.catalog_defs import (
    GLUE_COLUMNS_V2,
    HIVE_COLUMNS_V1,
    HISTORICAL_ROWS,
    TABLE,
)


class BehaviorLogMigrationService:
    MODULE = "hive_glue_field_migration"
    ENTITY = "字段迁移"
    TABLE = TABLE

    def __init__(self) -> None:
        self.engine = MigrationEngine(TABLE)
        self._bootstrapped = False

    def bootstrap(self) -> dict[str, Any]:
        """登记 v1/v2 两个可回切目录字段版本并装载历史数据。"""
        if self._bootstrapped:
            return {"ok": True, "message": "场景已初始化", "table": TABLE}
        self.engine.register_version(
            "v1_hive_string", "hive_metastore_prod", HIVE_COLUMNS_V1,
            note="Hive 现状：event_timestamp 为 STRING 文本，旧报表依赖")
        self.engine.register_version(
            "v2_glue_timestamp", "aws_glue_catalog", GLUE_COLUMNS_V2,
            parent_version="v1_hive_string",
            note="Glue 目标：event_timestamp 为 TIMESTAMP，新作业依赖")
        self.engine.load_source_rows(HISTORICAL_ROWS)
        self._bootstrapped = True
        return {"ok": True, "message": "已登记 v1/v2 字段版本并装载历史行为数据",
                "table": TABLE, "rows": len(HISTORICAL_ROWS)}

    # ---- 核验：旧文本 / 新类型解析 / 不可转换行 三栏证据 ----
    def verify(self, version_id: str, field_name: str = "event_timestamp") -> dict[str, Any]:
        report = self.engine.verify_version(version_id, field_name)
        return report.to_dict()

    # ---- Schema 变更对账：只采信已核验版本 ----
    def reconcile(self) -> dict[str, Any]:
        return self.engine.reconcile_schema("v1_hive_string", "v2_glue_timestamp")

    # ---- 两套读取 ----
    def legacy_read(self) -> dict[str, Any]:
        return {"reader": "legacy_report", "semantic": "STRING 原文，含义不变",
                "rows": self.engine.legacy_read()}

    def glue_read(self) -> dict[str, Any]:
        return {"reader": "glue_new_job", "semantic": "TIMESTAMP UTC 时间类型",
                "rows": self.engine.glue_read("v2_glue_timestamp")}

    # ---- 切换 / 回切 ----
    def cutover(self) -> dict[str, Any]:
        return self.engine.cutover("v2_glue_timestamp")

    def rollback(self) -> dict[str, Any]:
        return self.engine.rollback("v1_hive_string")

    def quarantine(self) -> dict[str, Any]:
        return {"quarantine": list(self.engine.quarantine.values())}

    def snapshot(self) -> dict[str, Any]:
        return self.engine.snapshot()


service = BehaviorLogMigrationService()
