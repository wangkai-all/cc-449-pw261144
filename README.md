# 来源快照服务（Source Snapshot Service）

保存每次上游响应的**哈希、压缩原文和解析结果**，支持按快照重新执行解析并比较字段差异；
快照达到保留上限时按策略清理，但**绝不删除仍被审计或重放引用的版本**。内置 Web 页面展示
快照时间线、差异字段、引用关系和清理候选，并对断点续跑、坏快照、解析器升级与回滚提供了
完整验证。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 运行测试（53 个用例）
.venv/bin/python -m pytest tests/ -q

# 可选：生成演示数据
.venv/bin/python -m snapshot_service.seed demo.db

# 启动服务（SNAPSHOT_DB 指定数据库文件，默认 snapshots.db）
SNAPSHOT_DB=demo.db .venv/bin/uvicorn snapshot_service.app:app --port 8000
# 打开 http://localhost:8000
```

## 核心概念

| 概念 | 说明 |
|---|---|
| 快照 | 每次响应一条：SHA-256 哈希 + zlib 压缩原文 + 当前解析器的解析结果 + 状态 |
| 重解析 | 解压快照原文，用指定（或当前）解析器重新解析，与入库时的解析结果做字段级 diff |
| 引用 | `audit` / `replay` 两类引用，生效中的引用会钉住快照，清理永远跳过 |
| 保留策略 | 每来源 `retention_max` 上限 + `oldest_first` / `largest_first` 策略 + 可选坏快照保护 |
| 断点续跑 | 批量重解析任务逐条推进 checkpoint，中断后从断点恢复，幂等不重复 |
| 坏快照 | 解压失败或哈希不匹配即标记 `corrupt`，批量任务跳过并计数，不影响其他快照 |
| 升级/回滚 | 解析器版本化（v1→v2）；回滚只允许回到该来源历史使用过的版本 |

## 清理语义

超量 = 快照总数 − 保留上限。清理按策略排序后从未受保护的快照中选取超量条数删除；
被引用的快照豁免（因此总数可能仍高于上限），实在凑不够可删条目时以 `blocked` 上报。
`protect_corrupt` 开启后坏快照同样豁免。释放（release）引用后快照重新可被清理。

## API 一览

```
POST   /api/sources                              新建来源
POST   /api/sources/{id}/responses               写入响应（哈希+压缩+解析）
GET    /api/sources/{id}/snapshots               快照时间线
GET    /api/snapshots/{id}/payload               读取原文（完整性校验，坏快照 409）
POST   /api/snapshots/{id}/reparse               重解析并返回字段 diff
GET    /api/snapshots/{id}/runs                  重解析记录
POST   /api/snapshots/{id}/refs                  添加 audit/replay 引用
DELETE /api/refs/{id}                            释放引用
PUT    /api/sources/{id}/retention               设置保留策略
GET    /api/sources/{id}/cleanup-candidates      预览清理候选与受保护项
POST   /api/sources/{id}/cleanup                 执行清理
POST   /api/sources/{id}/parser/upgrade          升级解析器
POST   /api/sources/{id}/parser/rollback         回滚解析器（须为历史版本）
POST   /api/sources/{id}/reparse-jobs            批量重解析（支持 simulate_interrupt_after）
POST   /api/reparse-jobs/{id}/resume             从断点续跑
```

## 测试覆盖（tests/）

- `test_ingest.py` — 哈希正确性、压缩往返、解析结果入库、parse_error 落库
- `test_reparse_diff.py` — 同版本零差异、v1→v2 字段差异、重解析记录
- `test_retention.py` — 上限计算、两种策略、audit/replay 引用豁免、释放后可清理、
  级联删除、坏快照保护、幂等清理
- `test_checkpoint.py` — 断点中断/恢复、多次中断无重复、坏快照跳过计数、完成任务重跑为空操作
- `test_corrupt.py` — 解压失败与哈希篡改检测、corrupt 标记、不影响兄弟快照
- `test_parser_upgrade_rollback.py` — 升级生效与历史、旧快照版本不变、回滚校验、
  回滚后差异归零、批量迁移
- `test_api.py` — HTTP 端到端（含 409/404、页面服务）

## 结构

```
snapshot_service/
  parsers.py   # 版本化解析器注册表（v1/v2）
  diff.py      # 字段级 diff
  storage.py   # SQLite schema 与连接管理
  service.py   # 核心业务逻辑
  app.py       # FastAPI 路由
  seed.py      # 演示数据
static/index.html  # 管理页面（时间线/差异/引用/清理/任务/解析器）
tests/             # pytest 套件
```
