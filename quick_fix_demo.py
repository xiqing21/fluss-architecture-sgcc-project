#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速修复演示脚本
用于快速生成测试数据，解决sink端三张表无数据的问题
"""

import psycopg2
import time
import datetime
import random

def quick_fix_empty_tables():
    """快速修复空表问题"""
    
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
    
    print("🔧 快速修复sink端空表问题")
    print("="*50)
    
    try:
        # 检查当前sink端状态
        print("📊 检查当前sink端状态...")
        with psycopg2.connect(**sink_params) as conn:
            with conn.cursor() as cursor:
                empty_tables = []
                tables = [
                    'ads_realtime_dashboard',
                    'ads_equipment_health', 
                    'ads_customer_behavior',
                    'ads_alert_statistics',
                    'ads_power_quality',
                    'ads_risk_assessment',
                    'ads_energy_efficiency'
                ]
                
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    status = "✅" if count > 0 else "❌"
                    print(f"   {status} {table}: {count} 条")
                    if count == 0:
                        empty_tables.append(table)
        
        if not empty_tables:
            print("\n🎉 所有表都有数据，无需修复！")
            return
        
        print(f"\n⚠️ 发现 {len(empty_tables)} 个空表: {', '.join(empty_tables)}")
        print("\n🤖 开始生成修复数据...")
        
        # 生成修复数据
        with psycopg2.connect(**source_params) as conn:
            with conn.cursor() as cursor:
                # 确保有测试客户
                print("👥 创建测试客户...")
                for i in range(5):
                    customer_id = f"FIX_CUST_{i+1:03d}"
                    
                    cursor.execute("""
                        INSERT INTO customer_info 
                        (customer_id, customer_name, customer_type, contact_person, contact_phone, address, contract_capacity, voltage_level, tariff_type, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
                        ON CONFLICT (customer_id) DO NOTHING
                    """, (
                        customer_id,
                        f"修复测试客户_{i+1}",
                        random.choice(['工业', '商业', '居民']),
                        f"联系人_{i+1}",
                        f"1380000{i+1:04d}",
                        f"修复测试地址_{i+1}",
                        random.uniform(1000.0, 10000.0),  # 合同容量
                        random.choice([220, 380, 10000, 35000]),  # 电压等级
                        random.choice(['峰谷电价', '一般工商业', '大工业'])
                    ))
                
                # 确保有测试设备
                print("📦 创建测试设备...")
                for i in range(10):
                    equipment_id = f"FIX_EQ_{i+1:03d}"
                    
                    cursor.execute("""
                        INSERT INTO equipment_info 
                        (equipment_id, equipment_name, equipment_type, location, voltage_level, capacity, manufacturer, status, install_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'NORMAL', CURRENT_DATE)
                        ON CONFLICT (equipment_id) DO NOTHING
                    """, (
                        equipment_id,
                        f"修复测试设备_{i+1}",
                        random.choice(['变压器', '发电机', '电机']),
                        f"修复测试位置_{i+1}",
                        random.choice([220, 380, 10000, 35000]),  # 电压等级
                        random.uniform(100.0, 5000.0),  # 容量
                        random.choice(['西门子', '施耐德', 'ABB', '华为', '中兴'])
                    ))
                
                conn.commit()
                
                # 生成电力消耗数据（针对ads_power_quality）
                print("⚡ 生成电力消耗数据...")
                for i in range(100):
                    equipment_id = f"FIX_EQ_{random.randint(1, 10):03d}"
                    customer_id = f"FIX_CUST_{random.randint(1, 5):03d}"
                    
                    # 生成一些异常数据来触发电力质量分析
                    if i % 10 == 0:  # 10%异常数据
                        voltage_a = random.uniform(180, 200)  # 低电压
                        voltage_b = random.uniform(180, 200)
                        voltage_c = random.uniform(180, 200)
                        current_a = random.uniform(60, 80)    # 高电流
                        current_b = random.uniform(60, 80)
                        current_c = random.uniform(60, 80)
                        power_factor = random.uniform(0.6, 0.8)  # 低功率因数
                    else:
                        voltage_a = random.uniform(220, 240)
                        voltage_b = random.uniform(220, 240)
                        voltage_c = random.uniform(220, 240)
                        current_a = random.uniform(1, 50)
                        current_b = random.uniform(1, 50)
                        current_c = random.uniform(1, 50)
                        power_factor = random.uniform(0.85, 0.95)
                    
                    active_power = (voltage_a * current_a + voltage_b * current_b + voltage_c * current_c) * power_factor / 1000
                    reactive_power = active_power * (1 - power_factor) / power_factor
                    frequency = random.uniform(49.0, 51.0)
                    energy_consumption = active_power * random.uniform(0.8, 1.2)
                    
                    # 分布在过去2小时
                    minutes_ago = random.randint(0, 120)
                    
                    cursor.execute("""
                        INSERT INTO power_consumption 
                        (customer_id, equipment_id, record_time, active_power, reactive_power, 
                         voltage_a, voltage_b, voltage_c, current_a, current_b, current_c, 
                         power_factor, frequency, energy_consumption)
                        VALUES (%s, %s, CURRENT_TIMESTAMP - INTERVAL '%s minutes', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (customer_id, equipment_id, minutes_ago, active_power, reactive_power,
                          voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                          power_factor, frequency, energy_consumption))
                
                # 生成告警数据（针对ads_risk_assessment）
                print("🚨 生成告警数据...")
                for i in range(50):
                    equipment_id = f"FIX_EQ_{random.randint(1, 10):03d}"
                    customer_id = f"FIX_CUST_{random.randint(1, 5):03d}"
                    alert_type = random.choice(['电压异常', '电流异常', '温度异常', '频率异常', '功率因数异常'])
                    alert_level = random.choice(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'])
                    
                    # 确保有高级别告警
                    if i < 10:
                        alert_level = random.choice(['HIGH', 'CRITICAL'])
                    
                    alert_title = f"设备{equipment_id}{alert_type}"
                    alert_description = f"修复测试-设备{equipment_id}发生{alert_type}，需要及时处理"
                    alert_value = random.uniform(50.0, 500.0)
                    threshold_value = random.uniform(100.0, 300.0)
                    minutes_ago = random.randint(0, 120)
                    
                    cursor.execute("""
                        INSERT INTO alert_records 
                        (equipment_id, customer_id, alert_type, alert_level, alert_title, alert_description, 
                         alert_time, alert_value, threshold_value, status)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP - INTERVAL '%s minutes', %s, %s, 'OPEN')
                    """, (equipment_id, customer_id, alert_type, alert_level, alert_title, alert_description, 
                          minutes_ago, alert_value, threshold_value))
                
                conn.commit()
                print("✅ 修复数据生成完成！")
        
        # 等待数据处理
        print("\n⏳ 等待Flink处理数据...")
        for i in range(30, 0, -1):
            print(f"\r   倒计时: {i} 秒", end="")
            time.sleep(1)
        print("\n")
        
        # 再次检查sink端状态
        print("🔍 检查修复结果...")
        with psycopg2.connect(**sink_params) as conn:
            with conn.cursor() as cursor:
                fixed_count = 0
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    
                    if table in empty_tables and count > 0:
                        print(f"   ✅ {table}: {count} 条 (已修复)")
                        fixed_count += 1
                    elif table in empty_tables:
                        print(f"   ❌ {table}: {count} 条 (仍为空)")
                    else:
                        print(f"   ✅ {table}: {count} 条")
        
        print(f"\n📈 修复结果: {fixed_count}/{len(empty_tables)} 个表已有数据")
        
        if fixed_count < len(empty_tables):
            print("\n💡 建议：")
            print("   1. 等待更长时间（DWS层可能需要小时级聚合）")
            print("   2. 使用完整的交互式工具生成更多数据")
            print("   3. 检查Flink作业状态")
            print("\n🔧 运行完整工具: ./start_data_manager.sh")
        else:
            print("\n🎉 所有表都已修复成功！")
    
    except Exception as e:
        print(f"❌ 修复过程中出现错误: {e}")
        print("\n💡 请检查：")
        print("   1. 数据库连接是否正常")
        print("   2. Docker容器是否运行")
        print("   3. 使用完整工具进行诊断")

if __name__ == "__main__":
    quick_fix_empty_tables()