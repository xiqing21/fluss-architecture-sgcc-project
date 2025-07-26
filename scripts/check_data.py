#!/usr/bin/env python3
import psycopg2

try:
    # 连接数据库
    conn = psycopg2.connect(
        host='localhost',
        port=5443,
        user='sgcc_user',
        password='sgcc_pass_2024',
        database='sgcc_dw_db'
    )
    cur = conn.cursor()
    
    # 检查各个ADS表的数据量
    tables = [
        'ads_power_quality',
        'ads_equipment_health', 
        'ads_alert_statistics',
        'ads_energy_efficiency'
    ]
    
    print("=== ADS表数据统计 ===")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"{table}: {count} 条记录")
    
    # 检查最新数据的时间戳
    print("\n=== 最新数据时间戳 ===")
    for table in tables:
        if table == 'ads_power_quality':
            time_col = 'analysis_time'
        elif table == 'ads_equipment_health':
            time_col = 'analysis_time'
        elif table == 'ads_alert_statistics':
            time_col = 'stat_time'
        elif table == 'ads_energy_efficiency':
            time_col = 'analysis_time'
        
        cur.execute(f"SELECT MAX({time_col}) FROM {table}")
        max_time = cur.fetchone()[0]
        print(f"{table}.{time_col}: {max_time}")
    
    conn.close()
    print("\n✅ 数据库连接成功，数据检查完成")
    
except Exception as e:
    print(f"❌ 数据库连接或查询失败: {e}")