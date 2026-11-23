"""Hive Metastore → Glue 字段迁移引擎的数据模型。

只依赖标准库，后端服务与独立 E2E 脚本共用同一份模型，
保证「平台里跑的逻辑」和「对账证据里跑的逻辑」是同一套代码。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


# 目录字段版本的生命周期：登记后必须先通过核验，才允许进入变更对账/切换。
VERSION_STATUSES = (
    "registered",   # 已登记，尚未完成数据核验
    "verified",     # 核验通过（含不可转换行已隔离的结论）
    "rejected",     # 核验未通过，禁止进入对账
    "active",       # 当前生效版本（Glue 侧切换后）
    "retired",      # 已被回切/替换，保留可回切证据
)


@dataclass
class ColumnSpec:
    """目录中的一列定义（Hive 文本版或 Glue 类型版共用）。"""
    name: str
    type_name: str
    nullable: bool = True
    comment: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CatalogFieldVersion:
    """可回切的目录字段版本。

    - version_id: 版本号，v1 为 Hive 旧文本格式，v2 为 Glue 目标类型格式
    - catalog:    所属目录（hive_metastore / aws_glue）
    - columns:    该版本登记的完整字段列
    - status:     见 VERSION_STATUSES
    - verification_ref: 核验报告 id；只有 verified/active/retired 才允许有值
    """
    version_id: str
    catalog: str
    table: str
    columns: list[ColumnSpec]
    status: str = "registered"
    verification_ref: str | None = None
    parent_version: str | None = None
    created_at: str = ""
    note: str = ""

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["columns"] = [c.to_dict() for c in self.columns]
        return data


@dataclass
class RowVerification:
    """单行历史数据的核验结果：旧文本表现 → 新类型解析 → 结论。"""
    row_id: str
    raw_text: str | None                       # Hive 侧原始文本（旧表现）
    parsed_iso: str | None = None              # Glue TIMESTAMP 解析后的 ISO 表现
    parse_error: str | None = None             # 无法转换时的原因
    convertible: bool = False
    null_preserved: bool = False               # 空值是否被保留为 NULL（而非字符串"null"）

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class VerificationReport:
    """字段版本核验报告。状态为 verified 才允许被变更对账引用。"""
    report_id: str
    table: str
    field_name: str
    version_id: str
    total_rows: int = 0
    convertible_rows: int = 0
    null_rows: int = 0
    unconvertible_rows: int = 0
    row_results: list[RowVerification] = field(default_factory=list)
    passed: bool = False
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "table": self.table,
            "field_name": self.field_name,
            "version_id": self.version_id,
            "total_rows": self.total_rows,
            "convertible_rows": self.convertible_rows,
            "null_rows": self.null_rows,
            "unconvertible_rows": self.unconvertible_rows,
            "passed": self.passed,
            "detail": self.detail,
            "row_results": [r.to_dict() for r in self.row_results],
        }


@dataclass
class ReconcileItem:
    """Schema 变更对账清单中的一列。"""
    column: str
    hive_type: str | None
    glue_type: str | None
    eligible: bool                  # 本次变更能否使用该列
    reason: str
    diff: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
