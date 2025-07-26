#!/usr/bin/env python3
import psycopg2
from datetime import datetime

def check_data():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5443,
            database='sgcc_dw_db',
            user='sgcc_user',
            password='sgcc_pass_2024'
        )
        cur = conn.cursor()
        
        print("=== 检查ADS表数据 ===")
        
        # 检查ads_power_quality
        cur.execute("SELECT analysis_time, overall_quality FROM ads_power_quality ORDER BY analysis_time DESC LIMIT 3")
        print("\nads_power_quality最新数据:")
        for row in cur.fetchall():
            print(f"  {row[0]} - 质量评分: {row[1]}")
        
        # 检查ads_alert_statistics
        cur.execute("SELECT stat_time, critical_alerts, error_alerts FROM ads_alert_statistics ORDER BY stat_time DESC LIMIT 3")
        print("\nads_alert_statistics最新数据:")
        for row in cur.fetchall():
            print(f"  {row[0]} - 严重告警: {row[1]}, 错误告警: {row[2]}")
        
        # 检查ads_energy_efficiency
        cur.execute("SELECT analysis_time, efficiency_ratio, potential_savings FROM ads_energy_efficiency ORDER BY analysis_time DESC LIMIT 3")
        print("\nads_energy_efficiency最新数据:")
        for row in cur.fetchall():
            print(f"  {row[0]} - 能效比: {row[1]}, 节能潜力: {row[2]}")
        
        # 检查ads_risk_assessment表结构
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ads_risk_assessment' ORDER BY ordinal_position")
        print("\nads_risk_assessment表结构:")
        for row in cur.fetchall():
            print(f"  {row[0]} - {row[1]}")
        
        # 检查ads_risk_assessment数据
        cur.execute("SELECT * FROM ads_risk_assessment LIMIT 3")
        print("\nads_risk_assessment最新数据:")
        for row in cur.fetchall():
            print(f"  {row}")
        
        # 检查时间范围
        print("\n=== 时间范围检查 ===")
        now = datetime.now()
        print(f"当前时间: {now}")
        
        cur.execute("SELECT MIN(analysis_time), MAX(analysis_time) FROM ads_power_quality")
        time_range = cur.fetchone()
        print(f"ads_power_quality时间范围: {time_range[0]} ~ {time_range[1]}")
        
        cur.execute("SELECT MIN(stat_time), MAX(stat_time) FROM ads_alert_statistics")
        time_range = cur.fetchone()
        print(f"ads_alert_statistics时间范围: {time_range[0]} ~ {time_range[1]}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")

if __name__ == "__main__":
    check_data()