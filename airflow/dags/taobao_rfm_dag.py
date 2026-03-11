from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. 定义默认参数 (展示你懂企业级容错机制)
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 10), # 设定一个过去的开始时间以激活调度
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,                        # 核心点：任务失败后自动重试 3 次
    'retry_delay': timedelta(minutes=5), # 核心点：每次重试间隔 5 分钟
}

# 2. 实例化 DAG (展示你懂 Cron 调度)
with DAG(
    'taobao_pyspark_rfm_dag',
    default_args=default_args,
    description='每天凌晨 2 点自动调度 PySpark RFM 计算任务',
    schedule_interval='0 2 * * *',       # 核心点：Cron 表达式，代表每天 2:00 AM 执行
    catchup=False,                       
    tags=['taobao', 'pyspark', 'rfm'],
) as dag:

    # 3. 定义具体的执行任务
    # 注意：这里的路径是 Docker 容器内部映射的默认路径，而不是你的 WSL 路径
    run_pyspark_job = BashOperator(
        task_id='run_pyspark_etl',
        bash_command='python3 /opt/airflow/dags/pyspark_rfm_etl.py',
    )

    # 这里可以定义任务流转顺序，目前只有一个任务，直接声明即可
    run_pyspark_job