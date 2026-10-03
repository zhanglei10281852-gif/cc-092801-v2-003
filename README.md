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
```

额度维护命令：

```bash
python -m app.cli compute-recover        # 恢复过期租约、结转取消请求
python -m app.cli compute-quota-report   # 输出全部额度主体的用量与剩余额度
```

## 额度核算约定

- 每个幂等键在同一家庭（`requested_by`）下只产生一个可追踪的服务单；重复提交返回原单，不重复计额。
- 提交时核算排队额度（`max_queued`）与当日提交额度（`daily_submissions`）；领取时核算运行额度（`max_running`），运行额度已满的家庭会被跳过，不会绕过额度进入执行队列。
- 排队、运行（含取消请求中）、取消、失败重试等状态对额度的占用与释放一致；每次扣减与释放都会写入 `compute_quota_events` 审计台账。
- 跨日结算统一使用 UTC 零点边界（`app.core.clock.day_start`）。
- 超额请求返回 409 与 `quota_exceeded` 错误码，`context` 中携带维度、上限、已用与剩余额度。
- 可通过 `GET /api/compute/quotas/usage` 查看用量与剩余额度，`GET /api/compute/quotas/events` 查看额度审计台账。

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
