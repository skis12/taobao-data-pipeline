import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, max, count, current_date, from_unixtime, to_date

def create_spark_session():
    """
    初始化 Spark 分布式计算引擎

    开发环境使用 local[*]（本机所有CPU核心），
    生产环境改为 master("yarn") 并配置 executor 资源。
    """
    master_mode = os.getenv("SPARK_MASTER", "local[*]")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")

    return SparkSession.builder \
        .appName("Taobao_BigData_RFM_Pipeline") \
        .master(master_mode) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .getOrCreate()

def run_etl_pipeline():
    # ---------- 数据库连接配置（通过环境变量注入）----------
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "taobao_data")

    if not DB_PASSWORD:
        raise ValueError("❌ 未设置 DB_PASSWORD 环境变量！请在 .env 文件或环境中设置数据库密码。")

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    db_properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # ---------- 启动 Spark ----------
    print("正在启动 PySpark 分布式引擎...")
    spark = create_spark_session()

    # ---------- 读取 ODS 层原始数据 ----------
    print("准备从数据库读取海量原始数据...")
    try:
        df_raw = spark.read.jdbc(url=jdbc_url, table="user_behavior", properties=db_properties)
        print("成功读取数据。")
    except Exception as e:
        print(f"读取数据失败，请检查连接: {e}")
        spark.stop()
        return

    # ---------- 数据清洗与 RFM 计算 ----------
    print("开始执行并行化数据清洗与用户活跃度计算...")

    # 步骤 A：去重 + 时间戳类型转换
    df_clean = df_raw.dropna().dropDuplicates()
    df_with_date = df_clean.withColumn("date", to_date(from_unixtime(col("ts"))))

    # 步骤 B：筛选购买行为（RFM 模型仅关注已转化用户）
    df_buy = df_with_date.filter(col("behavior_type") == 'buy')

    # 步骤 C：按用户聚合计算 R（Recency 最近购买间隔）和 F（Frequency 购买频次）
    # 注：本数据集无交易金额字段，因此未计算 M（Monetary）。
    #     如需补全，只需 JOIN 商品价格表，在 agg 中加 sum(amount).alias("m_score") 即可。
    df_rfm = df_buy.groupBy("user_id").agg(
        datediff(current_date(), max(col("date"))).alias("r_score"),
        count(col("item_id")).alias("f_score")
    )

    # ---------- 写入 ADS 层结果表 ----------
    print("计算完成，正在将结果分布式写入数据库...")
    df_rfm.write.jdbc(
        url=jdbc_url,
        table="ads_user_rfm_pyspark",
        mode="overwrite",
        properties=db_properties
    )

    print("数据成功落库！分布式管道运行结束。")
    spark.stop()

if __name__ == "__main__":
    run_etl_pipeline()