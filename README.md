# cc-etl-lab

CC_Codex 二期（26.11）**数据工程**产品线：**抽取转换加载台**（FastAPI + Vue 3 SPA + SQLite）。

与 `cc-data-lab`（Flask + HTMX 采集控制台）错开：本线面向 **ETL 管道编排 / 抽取落库**，前端为 Vue 单页，暖色仓运风格。

## 领域

- **所属领域**：数据工程  
- **子方向**：抽取-转换-加载 / 数据采集与落库  

## 技术栈

| 层 | 选型 |
|----|------|
| 后端 | Python 3.12、FastAPI、Uvicorn、SQLite |
| 前端 | Vue 3（CDN Composition）、双栏管道台 |
| 风格 | 暖琥珀仓运底、圆角强调、等宽源地址 |
| 测试 | pytest + httpx |

## 基线能力

登录、管道列表、管道详情、执行抽取、执行落库。

## 映射版本钉版（etl-05）

- **映射册**：映射版本只增不改（数据库触发器也禁止改写历史版本），前端可任选两版做列差异对比。
- **绑版**：创建管道必须绑定一版映射；创建后只能换到更新的版本，「不能绑到更旧版本」由服务端拒绝。
- **当日策略**：`禁切` 或 `先撤再落` 二选一，写进当日说明并锁定，当天不可更换；未锁定策略时不允许发起换版。
- **裁决在服务端**：落库进行中禁切；先撤再落会整批撤回已落行（撤回次数 +1）后绑定新版重落；请求偏离当日策略、起始版本或批次事实时返回「换版策略走歪了」并记入换版失败。
- **并发保存**：`BEGIN IMMEDIATE` 串行化，同刻保存只生成一个版号（相同内容复用，过期基线版本被拒）。
- **发布**：缺编码列的映射禁止发布。
- **总览 / 详情**：总览显示绑定版本、今日转换数、撤回次数、策略名；详情可查看版本差异、批次行数与换版失败列表。

主要接口：`/api/mappings`、`/api/mappings/{id}/versions`、`/api/mappings/{id}/versions/{no}/publish`、
`/api/switch-policy`、`/api/overview`、`/api/pipelines`、`/api/pipelines/{id}/switch`、
`/api/pipelines/{id}/withdraw`、`/confirm`。

## 本地

```powershell
cd claude-workspace\cc-etl-lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest
python run.py
```

http://127.0.0.1:6000/login · **admin / 123456**

## Docker

```powershell
docker compose up -d --build
```

端口 **6000**。冒烟：`python ..\scripts\_smoke_lab_access.py --dir .`
