import pandas as pd
from sqlalchemy import create_engine
import time

# ================= 你的配置区 =================
# 确保这里的 IP 和密码是正确的
DB_IP = "127.0.0.1" 
db_url = f"postgresql://admin:password123@{DB_IP}:5432/taobao_data"
engine = create_engine(db_url)

# CSV 文件路径
file_path = './data/UserBehavior.csv'

# 设置每一批次处理的数据量
# 建议：10万 (100000) 到 50万 (500000) 之间
# 如果你的电脑内存较小 (8G以下)，可以改小一点，比如 50000
CHUNK_SIZE = 100000 

try:
    print(f"正在连接数据库 {DB_IP} ...")
    
    # 初始化计数器
    count = 0
    start_time = time.time()
    
    print(f"🚀 开始分块读取并写入数据，每批次 {CHUNK_SIZE} 行...")

    # ================= 核心修改部分 =================
    # 1. 使用 chunksize 参数，pd.read_csv 变成了一个“生成器”
    # 它不会一次性读完，而是你循环一次，它吐出一块数据
    with pd.read_csv(
        file_path, 
        header=None, 
        names=['user_id', 'item_id', 'cat_id', 'behavior_type', 'ts'],
        chunksize=CHUNK_SIZE
    ) as reader:
        
        # 2. 循环遍历每一块数据 (chunk)
        for i, chunk in enumerate(reader):
            # i 是批次序号（从0开始），chunk 是包含10万行数据的 DataFrame
            
            # 3. 写入数据库
            # 注意：必须用 'append' (追加模式)，否则后面会覆盖前面的
            chunk.to_sql('user_behavior', engine, if_exists='append', index=False)
            
            # 更新计数器并打印进度
            count += len(chunk)
            elapsed = time.time() - start_time
            print(f" -> 第 {i+1} 批写入完成 | 当前总行数: {count} | 耗时: {elapsed:.2f}秒")

    print(f"✅ 所有数据写入完成！总计写入: {count} 行。")

except Exception as e:
    print(f"❌ 发生错误: {e}")