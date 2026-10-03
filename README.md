# 红白喜事服务运营平台

这是一个面向婚庆公司、殡葬服务机构和现场调度人员的 Python 后端服务，用于管理服务套餐、家庭订单、现场执行队列、服务人员、结果版本和运营干预。服务保留登录、角色权限、会话、审计和配额等基础能力，所有业务状态与审计事件写入本地 SQLite 数据库，适合在单个应用容器中离线运行。

## 运行环境

- Python 3.11
- SQLite 3（由 Python 标准库提供）
- FastAPI 与 Uvicorn

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/ceremony-operations.db`，可复制 `.env.example` 并设置 `TOWNSHIP_DATABASE_PATH` 指向其他本地路径。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

服务订单运营接口使用 `/api/compute` 前缀，身份、角色、审计和系统接口分别位于 `/api/auth`、`/api/roles`、`/api/audit` 与 `/api/system`。

## 测试与编译检查

```bash
python -m pytest
python -m compileall -q app tests
```

本地冒烟命令：

```bash
python -m app.cli smoke
python -m app.cli compute-demo
python -m app.cli compute-quota-check
```

`compute-quota-check` 通过维护入口重复提交同一服务单、并发领取、取消后重试并推进跨日时钟，输出每一步的额度占用、超额剩余额度与台账条目数，出现不一致时以非零状态退出。

## 目录结构

```text
app/compute/       任务模板、配额、提交、领取、回执和人工干预
app/api/            登录、角色、审计和系统管理接口
app/core/           时钟、安全、异常和分页能力
app/repositories/   SQLite 查询与事务封装
app/services/       身份、审计和后台任务服务
app/database.py     SQLite 连接、事务、表结构和权限初始化
tests/              领域、接口、调度和身份回归测试
tools/              本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL 和忙等待策略。提交、领取、回执和人工干预在即时事务中完成；租约、配额与结果版本使用可注入时钟，便于复现跨日和恢复边界。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。

### 额度台账

家庭的排队、运行与当日提交额度不再按任务当前状态估算，而是由 `compute_quota_ledger` 台账逐笔记录持有（+1）与释放（-1）：

- 提交时同事务写入 `daily`、`queued` 两笔持有；同一幂等键的重复提交（含并发）只回放唯一服务单，绝不重复扣减；数据库唯一约束为并发兜底。
- 领取把同一单的持有从 `queued` 原子转为 `running`；家庭运行额度已满时跳过该家庭、领取后续家庭的服务单，并发领取只有一个赢家。
- 完成释放 `running`；失败重试先释放 `running`、有排队空位时重新持有 `queued`，额度不足则终止为 `failed`；取消立即释放对应持有，人工重试只重新持有 `queued`（不重复计算当日提交）。
- 各桶的当前占用恒等于对应状态的服务单数；每次扣减/释放都在台账中留下原因、操作者、幂等键与业务日，服务单详情接口的 `quota_ledger` 字段可直接核对。
- 跨日结算统一使用注入时钟换算出的 UTC 业务日（`business_day_for`），提交计数、超额错误与查询接口共用同一时间边界。
- 超额请求返回 HTTP 409、错误码 `quota_exceeded`，`context` 中给出限额、已用、剩余额度与业务日；另提供 `GET /api/compute/quotas/user/{家庭}` 查询剩余额度。
