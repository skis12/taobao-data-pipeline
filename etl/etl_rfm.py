import pandas as pd
from sqlalchemy import create_engine
import datetime

# ================= 你的真实数据库配置 =================
DB_USER = 'admin'
DB_PASS = 'password123' 
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'taobao_data' 
# ===================================================

# 连接串
db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

def run_rfm_etl():
    print(f"[{datetime.datetime.now()}] >>> Step 1: 读取 DWD 层购买数据...")
    
    sql = """
    SELECT 
        user_id,
        MAX(ts) as last_buy_time,
        COUNT(*) as frequency
    FROM dwd.dwd_user_behavior_detail
    WHERE behavior_type = 'buy'
    GROUP BY user_id
    """
    
    try:
        df = pd.read_sql(sql, engine)
        print(f"    读取成功！付费用户数: {len(df)}")
    except Exception as e:
        print(f"!!! 连接或读取失败: {e}")
        return

    if df.empty:
        print("    数据为空，请检查数据库。")
        return

    print(f"[{datetime.datetime.now()}] >>> Step 2: 计算 RFM 分值...")
    
    PROCESS_DATE = pd.to_datetime('2017-12-03')
    
    # R (Recency)
    df['last_buy_time'] = pd.to_datetime(df['last_buy_time'])
    df['r_days'] = (PROCESS_DATE - df['last_buy_time']).dt.days
    
    # ================= 核心修正点 =================
    # 使用 rank(method='first') 强制排名，解决数据重复导致的 qcut 报错
    # R值越小越好 (rank越靠前，分越高: 5分)
    df['r_score'] = pd.qcut(df['r_days'].rank(method='first'), 5, labels=[5, 4, 3, 2, 1]).astype(int)
    
    # F值越大越好 (rank越靠后，分越高: 5分)
    df['f_score'] = pd.qcut(df['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5]).astype(int)
    # =============================================
    
    # 人群标签
    def get_label(row):
        if row['r_score'] >= 4 and row['f_score'] >= 4: return '重要价值用户'
        if row['r_score'] >= 4 and row['f_score'] < 4: return '重要发展用户'
        if row['r_score'] < 4 and row['f_score'] >= 4: return '重要保持用户'
        return '一般挽留用户'

    df['rfm_group'] = df.apply(get_label, axis=1)
    df['calc_date'] = datetime.date.today()
    
    # 整理字段
    df.rename(columns={'frequency': 'f_count'}, inplace=True)
    final_df = df[['user_id', 'r_days', 'f_count', 'r_score', 'f_score', 'rfm_group', 'calc_date']]

    print(f"[{datetime.datetime.now()}] >>> Step 3: 回写 ADS 层...")
    
    try:
        # 生产环境通常配合 DELETE 语句使用，这里开发环境直接追加
        final_df.to_sql('ads_user_rfm_score', engine, schema='ads', if_exists='append', index=False)
        print(f"[{datetime.datetime.now()}] >>> 成功！写入 {len(final_df)} 条数据。")
    except Exception as e:
        print(f"!!! 写入失败: {e}")

if __name__ == "__main__":
    run_rfm_etl()