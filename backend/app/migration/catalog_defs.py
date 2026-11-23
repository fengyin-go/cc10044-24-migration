"""user_behavior_log 迁移场景的固定目录定义与历史样本数据。

两条切换演练数据带不同时区偏移（+08:00 与 +05:30），
另含亚秒精度（毫秒/微秒/纳秒）与空值、不可转换行，覆盖全部核验分支。
"""
from __future__ import annotations

from app.migration.models import ColumnSpec

TABLE = "user_behavior_log"

# v1：Hive Metastore 现状，event_timestamp 为 STRING（旧文本格式）
HIVE_COLUMNS_V1 = [
    ColumnSpec("event_id", "STRING", comment="行为事件主键"),
    ColumnSpec("user_id", "BIGINT", comment="用户ID"),
    ColumnSpec("event_type", "STRING", comment="事件类型"),
    ColumnSpec("event_timestamp", "STRING", comment="事件时间-旧文本格式"),
    ColumnSpec("dt", "STRING", comment="分区日期"),
]

# v2：Glue 目标，event_timestamp 改为 TIMESTAMP；
# extra_glue_col 模拟 Glue 侧多出、Hive 没有的目录列（应进待对账清单）
GLUE_COLUMNS_V2 = [
    ColumnSpec("event_id", "STRING", comment="行为事件主键"),
    ColumnSpec("user_id", "BIGINT", comment="用户ID"),
    ColumnSpec("event_type", "STRING", comment="事件类型"),
    ColumnSpec("event_timestamp", "TIMESTAMP", nullable=False,
               comment="事件时间-Glue时间类型"),
    ColumnSpec("dt", "STRING", comment="分区日期"),
    ColumnSpec("ingest_run_id", "STRING", comment="Glue入作业运行ID"),
]

# 历史数据：raw 即 Hive STRING 里的真实文本表现
HISTORICAL_ROWS = [
    # 切换演练两条：不同时区偏移，同一物理时刻（00:15:30Z）
    {"row_id": "r1", "event_id": "evt-1001", "user_id": "88001",
     "event_type": "page_view",
     "event_timestamp": "2026-11-23 08:15:30.123456+08:00", "dt": "2026-11-23"},
    {"row_id": "r2", "event_id": "evt-1002", "user_id": "88002",
     "event_type": "add_cart",
     "event_timestamp": "2026-11-23T05:45:30.654321+05:30", "dt": "2026-11-23"},
    # 亚秒精度：毫秒 / Z 偏移
    {"row_id": "r3", "event_id": "evt-1003", "user_id": "88003",
     "event_type": "click",
     "event_timestamp": "2026-11-23T00:15:30.123Z", "dt": "2026-11-23"},
    # 亚秒精度：纳秒（9 位），超出 Spark 微秒粒度，需截断留痕
    {"row_id": "r4", "event_id": "evt-1004", "user_id": "88004",
     "event_type": "purchase",
     "event_timestamp": "2026-11-23T00:15:30.123456789Z", "dt": "2026-11-23"},
    # 空值：SQL NULL 与空串两种历史表现，都必须保留 NULL 语义
    {"row_id": "r5", "event_id": "evt-1005", "user_id": "88005",
     "event_type": "exposure", "event_timestamp": None, "dt": "2026-11-23"},
    {"row_id": "r6", "event_id": "evt-1006", "user_id": "88006",
     "event_type": "exposure", "event_timestamp": "", "dt": "2026-11-23"},
    # 无法转换的行：非法文本，必须隔离而不是猜测
    {"row_id": "r7", "event_id": "evt-1007", "user_id": "88007",
     "event_type": "click",
     "event_timestamp": "2026-13-99 25:61:61 bad-tz", "dt": "2026-11-23"},
]
