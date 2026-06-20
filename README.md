# 淘宝用户行为分析数据管道 (User Behavior Data Pipeline)

[![GitHub last commit](https://img.shields.io/badge/status-active-brightgreen)](https://github.com/skis12/taobao-data-pipeline)

![项目看板展示](dashboard.png)

## 📖 项目简介

本项目基于淘宝真实用户行为数据集（**约 6,500 万行 / 6.4GB**），构建了一个端到端的数据管道，涵盖数据摄入、分布式清洗与聚合计算、自动化调度、以及业务指标归因分析的全链路。

> ⚠️ **关于项目名称**：数据集仅包含 `user_id / item_id / cat_id / behavior_type / ts` 五列，**没有交易金额字段**，因此严格意义上只计算了 **R（Recency，最近购买间隔）和 F（Frequency，购买频次）** 两个维度，暂时无法计算 M（Monetary，消费金额）。若补充商品价格表，只需在聚合阶段 JOIN 后加一行 `sum(amount)` 即可，架构无需改动。

## 🛠️ 核心技术栈

| 类别 | 技术 |
|------|------|
| **数据摄入** | Python (pandas 分块读取), SQLAlchemy |
| **分布式计算** | PySpark (DataFrame API / Spark SQL) |
| **工作流调度** | Apache Airflow (DAG / Cron 定时调度 / 容错重试) |
| **存储** | PostgreSQL（ODS 贴源层 / ADS 应用层） |
| **容器化** | Docker, Docker Compose, WSL2 |
| **安全配置** | 环境变量注入, `.env` 文件与代码分离 |
| **可视化** | Metabase 敏捷 BI 看板 |

## ⚙️ 架构与数据流

```
UserBehavior.csv (6.4GB, ~6500万行)
        │
        ▼
┌─────────────────────────────────────────┐
│  import_data.py                          │
│  职责：数据摄入（CSV → ODS 贴源层）        │
│  核心技术：chunksize 分块读取 + 生成器模式  │
│  关键决策：每批10万行, if_exists='append'  │
└─────────────────────────────────────────┘
        │
        ▼
  PostgreSQL - user_behavior 表 (ODS层)
        │
        ▼
┌─────────────────────────────────────────┐
│  airflow/dags/pyspark_rfm_etl.py         │
│  职责：清洗 + RF聚合计算（ODS → ADS层）   │
│  核心技术：Spark DataFrame API + JDBC     │
│  清洗链路：去空→去重→日期转换→购买筛选→聚合│
└─────────────────────────────────────────┘
        │
        ▼
  PostgreSQL - ads_user_rfm_pyspark 表 (ADS层)
        │
        ├──→ Metabase（BI 看板）
        │
        └──→ anomaly_analysis.py（异动归因分析）
               归因公式：GMV增量 = 流量贡献 + 转化率贡献
        │
        ▼
┌─────────────────────────────────────────┐
│  airflow/dags/taobao_rfm_dag.py          │
│  职责：每天凌晨2点自动触发 ETL 计算       │
│  容错：retries=3, retry_delay=5min       │
│  调度：Cron '0 2 * * *', catchup=False   │
└─────────────────────────────────────────┘
```

## 📂 项目目录结构

```
taobao-data-pipeline/
├── import_data.py                # 数据摄入：CSV分块读取 → PostgreSQL ODS层
├── anomaly_analysis.py           # 异动归因分析：GMV增量拆解（流量 vs 转化率）
├── airflow/
│   └── dags/
│       ├── pyspark_rfm_etl.py    # PySpark 分布式清洗 + RF聚合计算
│       └── taobao_rfm_dag.py     # Airflow DAG 定时调度（每日凌晨2点）
├── docker/
│   └── docker-compose.yml        # 基础设施容器编排（PostgreSQL + Airflow）
├── dashboard.png                 # Metabase 看板截图
├── .env.example                  # 环境变量模板（可安全上传Git）
├── .gitignore                    # 排除 .env 敏感文件
└── README.md
```

## 🔐 安全配置

数据库密码等敏感信息**不在代码中硬编码**，通过环境变量注入：

- 代码使用 `os.getenv("DB_PASSWORD")` 读取
- 实际密码存放于 `.env` 文件（已在 `.gitignore` 中排除，**不会上传到 GitHub**）
- 提供 `.env.example` 模板文件，协作者复制并填入自己的凭据即可

```bash
# 快速配置
cp .env.example .env
# 编辑 .env 填入你的数据库密码
```

## 🚀 快速启动

### 环境要求

- Docker Desktop（已开启 WSL2 后端）
- Python 3.10+
- PostgreSQL JDBC 驱动（PySpark 自动通过 Maven 下载）

### 1. 克隆项目

```bash
git clone https://github.com/skis12/taobao-data-pipeline.git
cd taobao-data-pipeline
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填入数据库密码等信息
```

### 3. 启动基础设施

```bash
cd docker
docker-compose up -d
```

### 4. 数据摄入（CSV → ODS）

```bash
python import_data.py
# 将 UserBehavior.csv 分块导入 PostgreSQL
```

### 5. 运行 RF 聚合计算

```bash
python airflow/dags/pyspark_rfm_etl.py
# Spark 从 ODS 读取 → 清洗 → 聚合 RF → 写入 ADS
```

### 6. 访问 Airflow 调度平台

浏览器访问 `http://localhost:8080`，开启 `taobao_pyspark_rfm_dag` 的每日自动调度。

### 7. 运行异动归因分析

```bash
python anomaly_analysis.py
# 两周数据对比 + GMV增长归因拆解
```

## 📊 设计决策

- **数据摄入**：6.4GB CSV 无法全量加载，用 `chunksize` 分块读 + `if_exists='append'` 逐批写入，内存峰值约 150MB
- **计算引擎**：开发用 `local[*]` 验证，切 `yarn` 即可上集群，核心代码不变
- **调度容错**：`retries=3` + 重试间隔 5min，处理瞬时故障；`catchup=False` 防止启动大量历史回填
- **敏感信息**：密码走环境变量注入，`.gitignore` 排除 `.env`，代码中不残留硬编码凭据

## 📝 已知局限与改进方向

- **M值缺失**：数据集无金额字段 → JOIN 商品价格表可补
- **local模式**：受限于无集群硬件，开发在本地伪分布式完成 → 生产环境改 `master("yarn")`
- **BashOperator**：当前用 `BashOperator` 调用 Python 脚本 → 生产建议切 `SparkSubmitOperator`
- **异常处理**：当前为全局 `try/except` → 应细化为按异常类型分类处理与重试

---

*最后更新：2026-06*
