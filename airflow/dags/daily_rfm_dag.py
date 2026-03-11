from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 1. 设置默认参数
default_args = {
    'owner': 'xuye',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. 定义 DAG
with DAG(
    dag_id='taobao_rfm_daily',           
    default_args=default_args,
    description='淘宝用户行为每日RFM计算',
    start_date=datetime(2023, 1, 1),    
    schedule_interval='@daily',          
    catchup=False,                       
    tags=['taobao', 'etl'],              
) as dag:

    # 3. 定义任务逻辑
    def run_etl_logic():
        import etl_rfm  # 导入刚才搬运过来的脚本
        print(">>> 开始执行 RFM 计算脚本...")
        etl_rfm.run_rfm_etl()
        print(">>> 脚本执行结束。")

    # 4. 创建任务节点
    task_calculate_rfm = PythonOperator(
        task_id='calculate_rfm_score',   
        python_callable=run_etl_logic,   
    )