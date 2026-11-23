# Hive Metastore → Glue 行为日志字段迁移演练

把 `user_behavior_log` 行为日志表的 `event_timestamp` 字段从 Hive 的
**STRING 文本**迁移到 Glue 的 **TIMESTAMP 类型**，在不破坏旧报表的前提下
让 Glue 新作业消费时间类型，并保证全程可核验、可对账、可回切。

## 目录结构

```text
backend/app/migration/
├── models.py        # 目录字段版本、核验报告、对账清单等数据模型
├── timestamp.py     # STRING 文本 → Glue TIMESTAMP 的纯函数转换引擎
├── catalog_defs.py  # v1/v2 目录定义与历史样本（含两条不同时区偏移数据）
└── engine.py        # 迁移状态机：登记→核验→对账→双读→切换→回切
backend/app/services/behavior_migration.py  # 平台服务层封装（单例引擎）
backend/app/routers/migration.py            # HTTP 接口
scripts/migration_e2e/
├── run_e2e.py       # 端到端切换/回切演练（40 项逐项核验）
└── test_engine.py   # 单元测试（11 个，标准库 unittest）
```

## 关键设计如何对应任务要求

| 任务要求 | 实现位置 |
| --- | --- |
| 登记可回切的目录字段版本 | `engine.register_version`：v1（Hive STRING）/ v2（Glue TIMESTAMP），v2 挂 `parent_version=v1`，版本永不删除只退役 |
| event_timestamp 两侧 TIMESTAMP vs STRING | `catalog_defs` 中两版本同名字段类型不同，完整性差异在核验输出中显式打印 |
| 给依赖旧格式的读取保留原含义 | `engine.legacy_read` 原样返回 Hive 文本（`render_legacy_text`），空值仍是 NULL |
| 时区偏移 / 亚秒 / 空值逐行核对 | `timestamp.parse_event_timestamp`：偏移归一化到 UTC、毫秒保留 3 位、纳秒截断到微秒并留痕、NULL/空串保留 NULL |
| 无法转换的行 | 不猜测、不吞掉，进入 `quarantine` 隔离区并带错误原因，不写入 Glue |
| 只有已核验版本能进入对账 | `_require_verified` 门禁；catalog_sync 的「已同步」整体状态不参与判定 |
| 对账用字段版本给出可用列 | `reconcile_schema` 返回 `eligible_columns`；未核验差异列进 `backlog` 待对账清单并持久化 |
| 切换 / 回切 | `cutover`（需 verified + 已对账）、`rollback`（逐项核验类型与实际值） |

## 运行方式

```bash
# 单元测试
python3 -I scripts/migration_e2e/test_engine.py

# 端到端演练（40 项核验，含两条不同时区偏移数据的切换与回切）
python3 -I scripts/migration_e2e/run_e2e.py

# HTTP 全流程（后端启动后）
curl -X POST http://127.0.0.1:8018/api/migration/behavior-log/bootstrap
curl -X POST http://127.0.0.1:8018/api/migration/behavior-log/verify/v2_glue_timestamp
curl -X POST http://127.0.0.1:8018/api/migration/behavior-log/verify/v1_hive_string
curl -X POST http://127.0.0.1:8018/api/migration/behavior-log/reconcile
curl      http://127.0.0.1:8018/api/migration/behavior-log/reads/legacy
curl      http://127.0.0.1:8018/api/migration/behavior-log/reads/glue
curl -X POST http://127.0.0.1:8018/api/migration/behavior-log/cutover
curl -X POST http://127.0.0.1:8018/api/migration/behavior-log/rollback
```

## 演练数据（7 行）

| 行 | 旧文本 | 场景 |
| --- | --- | --- |
| r1 | `2026-11-23 08:15:30.123456+08:00` | 切换演练：+08:00 偏移、微秒 |
| r2 | `2026-11-23T05:45:30.654321+05:30` | 切换演练：+05:30 偏移、微秒 |
| r3 | `2026-11-23T00:15:30.123Z` | 毫秒精度 |
| r4 | `2026-11-23T00:15:30.123456789Z` | 纳秒，截断到微秒并留痕 |
| r5 / r6 | `NULL` / 空串 | 空值保留 NULL 语义 |
| r7 | `2026-13-99 25:61:61 bad-tz` | 不可转换，隔离不写入 |
