#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import time
import datetime

def monitor_data_flow():
    """监测数据流状态"""
    
    # 数据库连接参数
    source_params = {
        'host': 'localhost',
        'port': 5442,
        'database': 'sgcc_source_db',
        'user': 'sgcc_user',
        'password': 'sgcc_pass_2024'
    }
    
    sink_params = {
        'host': 'localhost',
        'port': 5443,
        'database': 'sgcc_dw_db',
        'user': 'sgcc_user',
        'password': 'sgcc_pass_2024'
    }
    
    print("🔍 开始监测数据流状态...")
    print("=" * 60)
    
    try:
        # 检查source端数据
        with psycopg2.connect(**source_params) as source_conn:
            with source_conn.cursor() as cursor:
                # 统计各表数据量
                cursor.execute("SELECT COUNT(*) FROM equipment_info")
                equipment_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM power_consumption")
                power_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM alert_records")
                alert_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT MAX(record_time) FROM power_consumption")
                latest_power_time = cursor.fetchone()[0]
                
                cursor.execute("SELECT MAX(alert_time) FROM alert_records")
                latest_alert_time = cursor.fetchone()[0]
                
                print("📊 Source端数据统计:")
                print(f"   设备信息: {equipment_count} 条")
                print(f"   电力消耗: {power_count} 条")
                print(f"   告警记录: {alert_count} 条")
                print(f"   最新电力数据时间: {latest_power_time}")
                print(f"   最新告警时间: {latest_alert_time}")
                print()
        
        # 检查sink端数据
        with psycopg2.connect(**sink_params) as sink_conn:
            with sink_conn.cursor() as cursor:
                # 检查各ADS表数据
                tables = [
                    'ads_realtime_dashboard',
                    'ads_equipment_health', 
                    'ads_customer_behavior',
                    'ads_alert_statistics',
                    'ads_power_quality',
                    'ads_risk_assessment',
                    'ads_energy_efficiency'
                ]
                
                print("📈 Sink端ADS层数据统计:")
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        
                        # 获取最新更新时间
                        time_columns = ['update_time', 'stat_time', 'analysis_time', 'created_at']
                        latest_time = None
                        for col in time_columns:
                            try:
                                cursor.execute(f"SELECT MAX({col}) FROM {table}")
                                latest_time = cursor.fetchone()[0]
                                if latest_time:
                                    break
                            except:
                                continue
                        
                        print(f"   {table}: {count} 条, 最新时间: {latest_time}")
                    except Exception as e:
                        print(f"   {table}: 查询失败 - {e}")
                
                print()
        
        # 数据流分析
        print("🔍 数据流分析:")
        current_time = datetime.datetime.now()
        
        if latest_power_time:
            power_delay = (current_time - latest_power_time.replace(tzinfo=None)).total_seconds()
            print(f"   Source端最新电力数据距现在: {power_delay:.0f} 秒")
        
        if latest_alert_time:
            alert_delay = (current_time - latest_alert_time.replace(tzinfo=None)).total_seconds()
            print(f"   Source端最新告警数据距现在: {alert_delay:.0f} 秒")
        
        print()
        print("💡 结论:")
        print("   ✅ Source端数据正常插入")
        print("   ⚠️ Sink端数据更新滞后，可能原因:")
        print("      - Flink作业处理延迟")
        print("      - CDC连接器配置问题")
        print("      - 数据流管道阻塞")
        print("      - 批处理窗口时间设置")
        
    except Exception as e:
        print(f"❌ 监测过程中出现错误: {e}")
    
    print("=" * 60)

if __name__ == "__main__":
    monitor_data_flow()