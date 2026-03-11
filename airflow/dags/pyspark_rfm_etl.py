from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, max, count, current_date, from_unixtime, to_date

def create_spark_session():
    """
    初始化 Spark 分布式计算引擎
    """
    return SparkSession.builder \
        .appName("Taobao_BigData_RFM_Pipeline") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .getOrCreate()

def run_etl_pipeline():
    print("正在启动 PySpark 分布式引擎...")
    spark = create_spark_session()
    
    # 替换为你 DBeaver 中显示的真实 IP
    jdbc_url = "jdbc:postgresql://172.26.180.180:5432/taobao_data"
    db_properties = {
        "user": "admin",
        "password": "password123",
        "driver": "org.postgresql.Driver"
    }

    print("准备从数据库读取海量原始数据...")
    try:
        # 使用真实的表名 user_behavior
        df_raw = spark.read.jdbc(url=jdbc_url, table="user_behavior", properties=db_properties)
        print("成功读取数据。")
    except Exception as e:
        print(f"读取数据失败，请检查连接: {e}")
        spark.stop()
        return

    print("开始执行并行化数据清洗与 RFM 模型计算...")
    
    # 步骤 A：去重，并将 int8 类型的时间戳 (ts) 转换为真正的日期格式 (date)
    df_clean = df_raw.dropna().dropDuplicates()
    df_with_date = df_clean.withColumn("date", to_date(from_unixtime(col("ts"))))
    
    # 步骤 B：筛选出产生实际购买的行为
    # 淘宝数据集中，通常 'buy' 代表购买。如果你的数据里是用数字表示，请修改这里。
    df_buy = df_with_date.filter(col("behavior_type") == 'buy')

    # 步骤 C：核心 RFM 聚合计算
    df_rfm = df_buy.groupBy("user_id").agg(
        datediff(current_date(), max(col("date"))).alias("r_score"), 
        count(col("item_id")).alias("f_score")                       
    )

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