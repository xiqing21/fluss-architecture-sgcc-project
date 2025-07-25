#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交互式数据管理脚本 - 修正版
功能：
1. Source端CRUD操作（基于实际表结构）
2. 自动生成测试数据（可配置时长和数据量）
3. Sink端数据监控
4. 数据流延迟分析
"""

import psycopg2
import time
import datetime
import random
import json
import threading
from typing import Dict, List, Optional
import sys

class DataManager:
    def __init__(self):
        # 数据库连接参数
        self.source_params = {
            'host': 'localhost',
            'port': 5442,
            'database': 'sgcc_source_db',
            'user': 'sgcc_user',
            'password': 'sgcc_pass_2024'
        }
        
        self.sink_params = {
            'host': 'localhost',
            'port': 5443,
            'database': 'sgcc_dw_db',
            'user': 'sgcc_user',
            'password': 'sgcc_pass_2024'
        }
        
        self.monitoring = False
        self.monitor_thread = None
        
    def get_source_connection(self):
        """获取source数据库连接"""
        return psycopg2.connect(**self.source_params)
    
    def get_sink_connection(self):
        """获取sink数据库连接"""
        return psycopg2.connect(**self.sink_params)
    
    def show_menu(self):
        """显示主菜单"""
        print("\n" + "="*60)
        print("🔧 实时数据流管理工具 - 修正版")
        print("="*60)
        print("1. Source端数据操作")
        print("2. 自动生成测试数据")
        print("3. Sink端数据监控")
        print("4. 数据流延迟分析")
        print("5. 查看表结构")
        print("6. 清理测试数据")
        print("0. 退出")
        print("="*60)
    
    def source_data_operations(self):
        """Source端数据操作菜单"""
        while True:
            print("\n📊 Source端数据操作:")
            print("1. 查看数据统计")
            print("2. 插入客户信息")
            print("3. 插入设备信息")
            print("4. 插入电力消耗数据")
            print("5. 插入告警记录")
            print("6. 更新设备状态")
            print("7. 删除过期数据")
            print("0. 返回主菜单")
            
            choice = input("请选择操作: ").strip()
            
            if choice == '1':
                self.show_source_statistics()
            elif choice == '2':
                self.insert_customer_info()
            elif choice == '3':
                self.insert_equipment_info()
            elif choice == '4':
                self.insert_power_consumption()
            elif choice == '5':
                self.insert_alert_record()
            elif choice == '6':
                self.update_equipment_status()
            elif choice == '7':
                self.delete_expired_data()
            elif choice == '0':
                break
            else:
                print("❌ 无效选择，请重试")
    
    def show_source_statistics(self):
        """显示source端数据统计"""
        try:
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    print("\n📈 Source端数据统计:")
                    
                    # 客户信息统计
                    cursor.execute("SELECT COUNT(*) FROM customer_info")
                    customer_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM customer_info WHERE status = 'ACTIVE'")
                    active_customer_count = cursor.fetchone()[0]
                    
                    # 设备信息统计
                    cursor.execute("SELECT COUNT(*) FROM equipment_info")
                    equipment_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM equipment_info WHERE status = 'NORMAL'")
                    normal_equipment_count = cursor.fetchone()[0]
                    
                    # 电力消耗统计
                    cursor.execute("SELECT COUNT(*) FROM power_consumption")
                    power_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT MAX(record_time), MIN(record_time) FROM power_consumption")
                    power_time_range = cursor.fetchone()
                    
                    # 告警记录统计
                    cursor.execute("SELECT COUNT(*) FROM alert_records")
                    alert_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM alert_records WHERE alert_level = 'CRITICAL'")
                    critical_alert_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM alert_records WHERE status = 'OPEN'")
                    open_alert_count = cursor.fetchone()[0]
                    
                    print(f"   客户总数: {customer_count} (活跃: {active_customer_count})")
                    print(f"   设备总数: {equipment_count} (正常: {normal_equipment_count})")
                    print(f"   电力记录: {power_count} 条")
                    if power_time_range[0] and power_time_range[1]:
                        print(f"   时间范围: {power_time_range[1]} ~ {power_time_range[0]}")
                    print(f"   告警记录: {alert_count} 条 (严重: {critical_alert_count}, 未处理: {open_alert_count})")
                    
        except Exception as e:
            print(f"❌ 查询失败: {e}")
    
    def insert_customer_info(self):
        """插入客户信息"""
        try:
            customer_id = input("客户ID: ").strip()
            customer_name = input("客户名称: ").strip()
            customer_type = input("客户类型 (RESIDENTIAL/COMMERCIAL/INDUSTRIAL): ").strip()
            contact_person = input("联系人: ").strip()
            contact_phone = input("联系电话: ").strip()
            address = input("地址: ").strip()
            contract_capacity = float(input("合同容量(kW): ") or "100")
            voltage_level = int(input("电压等级(V): ") or "380")
            tariff_type = input("电价类型: ").strip() or "STANDARD"
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO customer_info 
                        (customer_id, customer_name, customer_type, contact_person, contact_phone,
                         address, contract_capacity, voltage_level, tariff_type, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
                    """, (customer_id, customer_name, customer_type, contact_person, contact_phone,
                          address, contract_capacity, voltage_level, tariff_type))
                    conn.commit()
                    print("✅ 客户信息插入成功")
                    
        except Exception as e:
            print(f"❌ 插入失败: {e}")
    
    def insert_equipment_info(self):
        """插入设备信息"""
        try:
            equipment_id = input("设备ID: ").strip()
            equipment_name = input("设备名称: ").strip()
            equipment_type = input("设备类型: ").strip()
            location = input("位置: ").strip()
            voltage_level = int(input("电压等级(V): ") or "380")
            capacity = float(input("容量(kW): ") or "100")
            manufacturer = input("制造商: ").strip() or "Unknown"
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO equipment_info 
                        (equipment_id, equipment_name, equipment_type, location, voltage_level,
                         capacity, manufacturer, install_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, 'NORMAL')
                    """, (equipment_id, equipment_name, equipment_type, location, voltage_level,
                          capacity, manufacturer))
                    conn.commit()
                    print("✅ 设备信息插入成功")
                    
        except Exception as e:
            print(f"❌ 插入失败: {e}")
    
    def insert_power_consumption(self):
        """插入电力消耗数据"""
        try:
            customer_id = input("客户ID: ").strip()
            equipment_id = input("设备ID: ").strip()
            
            # 三相电压
            voltage_a = float(input("A相电压(V): ") or str(random.uniform(220, 240)))
            voltage_b = float(input("B相电压(V): ") or str(random.uniform(220, 240)))
            voltage_c = float(input("C相电压(V): ") or str(random.uniform(220, 240)))
            
            # 三相电流
            current_a = float(input("A相电流(A): ") or str(random.uniform(1, 50)))
            current_b = float(input("B相电流(A): ") or str(random.uniform(1, 50)))
            current_c = float(input("C相电流(A): ") or str(random.uniform(1, 50)))
            
            # 功率和其他参数
            active_power = float(input("有功功率(kW): ") or str(random.uniform(10, 100)))
            reactive_power = float(input("无功功率(kVar): ") or str(random.uniform(5, 50)))
            power_factor = float(input("功率因数: ") or str(random.uniform(0.8, 0.95)))
            frequency = float(input("频率(Hz): ") or "50.0")
            energy_consumption = float(input("电能消耗(kWh): ") or str(random.uniform(50, 500)))
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO power_consumption 
                        (customer_id, equipment_id, record_time, active_power, reactive_power,
                         voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                         power_factor, frequency, energy_consumption)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (customer_id, equipment_id, active_power, reactive_power,
                          voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                          power_factor, frequency, energy_consumption))
                    conn.commit()
                    print(f"✅ 电力数据插入成功")
                    
        except Exception as e:
            print(f"❌ 插入失败: {e}")
    
    def insert_alert_record(self):
        """插入告警记录"""
        try:
            equipment_id = input("设备ID: ").strip()
            customer_id = input("客户ID: ").strip()
            alert_type = input("告警类型 (VOLTAGE_SAG/VOLTAGE_SWELL/POWER_INTERRUPTION/OVERLOAD): ").strip()
            alert_level = input("告警级别 (INFO/WARNING/ERROR/CRITICAL): ").strip()
            alert_title = input("告警标题: ").strip()
            alert_description = input("告警描述: ").strip()
            alert_value = float(input("告警值: ") or "0")
            threshold_value = float(input("阈值: ") or "0")
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO alert_records 
                        (equipment_id, customer_id, alert_type, alert_level, alert_title,
                         alert_description, alert_time, alert_value, threshold_value, status)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, 'OPEN')
                    """, (equipment_id, customer_id, alert_type, alert_level, alert_title,
                          alert_description, alert_value, threshold_value))
                    conn.commit()
                    print("✅ 告警记录插入成功")
                    
        except Exception as e:
            print(f"❌ 插入失败: {e}")
    
    def update_equipment_status(self):
        """更新设备状态"""
        try:
            equipment_id = input("设备ID: ").strip()
            status = input("新状态 (NORMAL/FAULT/MAINTENANCE/OFFLINE): ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE equipment_info SET status = %s WHERE equipment_id = %s
                    """, (status, equipment_id))
                    
                    if cursor.rowcount > 0:
                        conn.commit()
                        print(f"✅ 设备 {equipment_id} 状态更新为 {status}")
                    else:
                        print(f"❌ 未找到设备 {equipment_id}")
                        
        except Exception as e:
            print(f"❌ 更新失败: {e}")
    
    def delete_expired_data(self):
        """删除过期数据"""
        try:
            days = int(input("删除多少天前的数据: "))
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    # 删除过期的电力消耗数据
                    cursor.execute("""
                        DELETE FROM power_consumption 
                        WHERE record_time < CURRENT_TIMESTAMP - INTERVAL '%s days'
                    """, (days,))
                    power_deleted = cursor.rowcount
                    
                    # 删除过期的告警记录
                    cursor.execute("""
                        DELETE FROM alert_records 
                        WHERE alert_time < CURRENT_TIMESTAMP - INTERVAL '%s days'
                    """, (days,))
                    alert_deleted = cursor.rowcount
                    
                    conn.commit()
                    print(f"✅ 删除完成: 电力数据 {power_deleted} 条, 告警记录 {alert_deleted} 条")
                    
        except Exception as e:
            print(f"❌ 删除失败: {e}")
    
    def auto_generate_data(self):
        """自动生成测试数据"""
        print("\n🤖 自动数据生成配置:")
        
        try:
            duration_minutes = int(input("生成数据时长(分钟): "))
            records_per_minute = int(input("每分钟记录数: "))
            customer_count = int(input("客户数量: ") or "5")
            equipment_count = int(input("设备数量: ") or "10")
            
            print(f"\n开始生成数据...")
            print(f"时长: {duration_minutes} 分钟")
            print(f"频率: {records_per_minute} 条/分钟")
            print(f"客户数: {customer_count} 个")
            print(f"设备数: {equipment_count} 个")
            print(f"预计总记录数: {duration_minutes * records_per_minute}")
            
            confirm = input("确认开始生成? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            self._generate_test_data(duration_minutes, records_per_minute, customer_count, equipment_count)
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 生成失败: {e}")
    
    def _generate_test_data(self, duration_minutes: int, records_per_minute: int, customer_count: int, equipment_count: int):
        """生成测试数据的核心逻辑"""
        try:
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    # 创建测试客户
                    print(f"创建 {customer_count} 个测试客户...")
                    customer_ids = []
                    for i in range(customer_count):
                        customer_id = f"CUST_{i+1:03d}"
                        customer_ids.append(customer_id)
                        cursor.execute("""
                            INSERT INTO customer_info 
                            (customer_id, customer_name, customer_type, contact_person, contact_phone,
                             address, contract_capacity, voltage_level, tariff_type, status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
                            ON CONFLICT (customer_id) DO NOTHING
                        """, (
                            customer_id,
                            f"测试客户_{i+1}",
                            random.choice(['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL']),
                            f"联系人_{i+1}",
                            f"138{random.randint(10000000, 99999999)}",
                            f"测试地址_{i+1}",
                            random.uniform(50, 500),
                            random.choice([220, 380, 10000]),
                            "STANDARD"
                        ))
                    
                    # 创建测试设备
                    print(f"创建 {equipment_count} 个测试设备...")
                    equipment_ids = []
                    for i in range(equipment_count):
                        equipment_id = f"EQ_{i+1:03d}"
                        equipment_ids.append(equipment_id)
                        cursor.execute("""
                            INSERT INTO equipment_info 
                            (equipment_id, equipment_name, equipment_type, location, voltage_level,
                             capacity, manufacturer, install_date, status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, 'NORMAL')
                            ON CONFLICT (equipment_id) DO NOTHING
                        """, (
                            equipment_id,
                            f"测试设备_{i+1}",
                            random.choice(['变压器', '发电机', '电动机', '开关柜']),
                            f"测试位置_{i+1}",
                            random.choice([220, 380, 10000]),
                            random.uniform(50, 1000),
                            random.choice(['ABB', 'Siemens', '施耐德', '华为'])
                        ))
                    
                    conn.commit()
                    
                    total_records = 0
                    start_time = time.time()
                    
                    for minute in range(duration_minutes):
                        minute_start = time.time()
                        
                        for _ in range(records_per_minute):
                            customer_id = random.choice(customer_ids)
                            equipment_id = random.choice(equipment_ids)
                            
                            # 生成电力消耗数据
                            cursor.execute("""
                                INSERT INTO power_consumption 
                                (customer_id, equipment_id, record_time, active_power, reactive_power,
                                 voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                                 power_factor, frequency, energy_consumption)
                                VALUES (%s, %s, CURRENT_TIMESTAMP - INTERVAL '%s minutes', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                customer_id, equipment_id, duration_minutes - minute - 1,
                                random.uniform(10, 100),  # active_power
                                random.uniform(5, 50),    # reactive_power
                                random.uniform(220, 240), # voltage_a
                                random.uniform(220, 240), # voltage_b
                                random.uniform(220, 240), # voltage_c
                                random.uniform(1, 50),    # current_a
                                random.uniform(1, 50),    # current_b
                                random.uniform(1, 50),    # current_c
                                random.uniform(0.8, 0.95), # power_factor
                                random.uniform(49.5, 50.5), # frequency
                                random.uniform(50, 500)   # energy_consumption
                            ))
                            
                            # 随机生成告警
                            if random.random() < 0.05:  # 5%概率生成告警
                                alert_type = random.choice(['VOLTAGE_SAG', 'VOLTAGE_SWELL', 'POWER_INTERRUPTION', 'OVERLOAD'])
                                alert_level = random.choice(['INFO', 'WARNING', 'ERROR', 'CRITICAL'])
                                
                                cursor.execute("""
                                    INSERT INTO alert_records 
                                    (equipment_id, customer_id, alert_type, alert_level, alert_title,
                                     alert_description, alert_time, alert_value, threshold_value, status)
                                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP - INTERVAL '%s minutes', %s, %s, 'OPEN')
                                """, (
                                    equipment_id, customer_id, alert_type, alert_level,
                                    f"{alert_type}告警",
                                    f"设备{equipment_id}发生{alert_type}异常",
                                    duration_minutes - minute - 1,
                                    random.uniform(100, 300),
                                    random.uniform(200, 250)
                                ))
                            
                            total_records += 1
                        
                        conn.commit()
                        
                        # 控制生成速度
                        elapsed = time.time() - minute_start
                        if elapsed < 1:  # 每分钟的数据在1秒内生成完
                            time.sleep(1 - elapsed)
                        
                        print(f"\r进度: {minute+1}/{duration_minutes} 分钟, 已生成 {total_records} 条记录", end="")
                    
                    print(f"\n✅ 数据生成完成! 总计 {total_records} 条记录")
                    print(f"用时: {time.time() - start_time:.2f} 秒")
                    
        except Exception as e:
            print(f"\n❌ 数据生成失败: {e}")
    
    def monitor_sink_data(self):
        """监控sink端数据"""
        print("\n📊 Sink端数据监控:")
        print("1. 实时监控 (每5秒刷新)")
        print("2. 单次查询")
        print("0. 返回")
        
        choice = input("请选择: ").strip()
        
        if choice == '1':
            self.start_real_time_monitoring()
        elif choice == '2':
            self.show_sink_statistics()
    
    def start_real_time_monitoring(self):
        """开始实时监控"""
        print("\n🔄 开始实时监控 (按 Ctrl+C 停止)...")
        
        try:
            while True:
                print("\033[2J\033[H")  # 清屏
                print(f"📊 实时监控 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*80)
                
                self.show_sink_statistics()
                self.analyze_data_flow()
                
                print("\n按 Ctrl+C 停止监控...")
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n✅ 监控已停止")
    
    def show_sink_statistics(self):
        """显示sink端数据统计"""
        try:
            with self.get_sink_connection() as conn:
                with conn.cursor() as cursor:
                    tables = [
                        'ads_realtime_dashboard',
                        'ads_equipment_health', 
                        'ads_customer_behavior',
                        'ads_alert_statistics',
                        'ads_power_quality',
                        'ads_risk_assessment',
                        'ads_energy_efficiency'
                    ]
                    
                    print("\n📈 Sink端ADS层数据统计:")
                    
                    for table in tables:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cursor.fetchone()[0]
                            
                            # 尝试获取最新更新时间
                            latest_time = None
                            time_columns = ['update_time', 'stat_time', 'analysis_time', 'created_at']
                            
                            for col in time_columns:
                                try:
                                    cursor.execute(f"SELECT MAX({col}) FROM {table} WHERE {col} IS NOT NULL")
                                    result = cursor.fetchone()
                                    if result and result[0]:
                                        latest_time = result[0]
                                        break
                                except:
                                    continue
                            
                            status = "✅" if count > 0 else "❌"
                            time_str = latest_time.strftime('%H:%M:%S') if latest_time else "无数据"
                            print(f"   {status} {table:<25}: {count:>6} 条  最新: {time_str}")
                            
                        except Exception as e:
                            print(f"   ❌ {table:<25}: 查询失败 - {str(e)[:30]}")
                    
        except Exception as e:
            print(f"❌ Sink端查询失败: {e}")
    
    def analyze_data_flow(self):
        """分析数据流延迟"""
        try:
            print("\n🔍 数据流延迟分析:")
            
            # 获取source端最新数据时间
            with self.get_source_connection() as source_conn:
                with source_conn.cursor() as cursor:
                    cursor.execute("SELECT MAX(record_time) FROM power_consumption")
                    latest_source_time = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT MAX(alert_time) FROM alert_records")
                    latest_alert_time = cursor.fetchone()[0]
            
            # 获取sink端最新数据时间
            with self.get_sink_connection() as sink_conn:
                with sink_conn.cursor() as cursor:
                    try:
                        cursor.execute("SELECT MAX(analysis_time) FROM ads_power_quality")
                        latest_sink_time = cursor.fetchone()[0]
                    except:
                        latest_sink_time = None
            
            current_time = datetime.datetime.now()
            
            if latest_source_time:
                source_delay = (current_time - latest_source_time.replace(tzinfo=None)).total_seconds()
                print(f"   Source端数据延迟: {source_delay:.1f} 秒")
            
            if latest_sink_time:
                sink_delay = (current_time - latest_sink_time.replace(tzinfo=None)).total_seconds()
                print(f"   Sink端数据延迟: {sink_delay:.1f} 秒")
                
                if latest_source_time and latest_sink_time:
                    flow_delay = (latest_sink_time.replace(tzinfo=None) - latest_source_time.replace(tzinfo=None)).total_seconds()
                    print(f"   数据流处理延迟: {abs(flow_delay):.1f} 秒")
            else:
                print("   Sink端暂无数据")
                
        except Exception as e:
            print(f"❌ 延迟分析失败: {e}")
    
    def show_table_structures(self):
        """显示表结构"""
        print("\n📋 表结构查看:")
        print("1. Source端表结构")
        print("2. Sink端表结构")
        print("0. 返回")
        
        choice = input("请选择: ").strip()
        
        if choice == '1':
            self._show_source_tables()
        elif choice == '2':
            self._show_sink_tables()
    
    def _show_source_tables(self):
        """显示source端表结构"""
        try:
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    tables = ['customer_info', 'equipment_info', 'power_consumption', 'alert_records', 'equipment_status']
                    
                    for table in tables:
                        print(f"\n📊 {table} 表结构:")
                        cursor.execute(f"""
                            SELECT column_name, data_type, is_nullable, column_default
                            FROM information_schema.columns 
                            WHERE table_name = '{table}'
                            ORDER BY ordinal_position
                        """)
                        columns = cursor.fetchall()
                        
                        for col in columns:
                            nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                            default = f" DEFAULT {col[3]}" if col[3] else ""
                            print(f"   {col[0]:<20} {col[1]:<25} {nullable}{default}")
                        
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"   当前记录数: {count}")
                        
        except Exception as e:
            print(f"❌ 查询失败: {e}")
    
    def _show_sink_tables(self):
        """显示sink端表结构"""
        try:
            with self.get_sink_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    """)
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table in tables:
                        print(f"\n📊 {table} 表结构:")
                        cursor.execute(f"""
                            SELECT column_name, data_type, is_nullable, column_default
                            FROM information_schema.columns 
                            WHERE table_name = '{table}'
                            ORDER BY ordinal_position
                        """)
                        columns = cursor.fetchall()
                        
                        for col in columns:
                            nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                            default = f" DEFAULT {col[3]}" if col[3] else ""
                            print(f"   {col[0]:<20} {col[1]:<25} {nullable}{default}")
                        
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"   当前记录数: {count}")
                        
        except Exception as e:
            print(f"❌ 查询失败: {e}")
    
    def cleanup_test_data(self):
        """清理测试数据"""
        print("\n🧹 清理测试数据:")
        print("1. 清理Source端测试数据")
        print("2. 清理Sink端所有数据")
        print("3. 清理所有测试数据")
        print("0. 返回")
        
        choice = input("请选择: ").strip()
        
        if choice == '1':
            self._cleanup_source_data()
        elif choice == '2':
            self._cleanup_sink_data()
        elif choice == '3':
            self._cleanup_all_data()
    
    def _cleanup_source_data(self):
        """清理source端测试数据"""
        try:
            confirm = input("确认清理Source端测试数据? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    # 删除测试数据（按依赖关系顺序）
                    cursor.execute("DELETE FROM power_consumption WHERE customer_id LIKE 'CUST_%' OR equipment_id LIKE 'EQ_%'")
                    power_deleted = cursor.rowcount
                    
                    cursor.execute("DELETE FROM alert_records WHERE customer_id LIKE 'CUST_%' OR equipment_id LIKE 'EQ_%'")
                    alert_deleted = cursor.rowcount
                    
                    cursor.execute("DELETE FROM equipment_info WHERE equipment_id LIKE 'EQ_%' OR equipment_id LIKE 'TEST_%'")
                    equipment_deleted = cursor.rowcount
                    
                    cursor.execute("DELETE FROM customer_info WHERE customer_id LIKE 'CUST_%'")
                    customer_deleted = cursor.rowcount
                    
                    conn.commit()
                    print(f"✅ Source端清理完成:")
                    print(f"   客户: {customer_deleted} 条")
                    print(f"   设备: {equipment_deleted} 条")
                    print(f"   电力数据: {power_deleted} 条")
                    print(f"   告警数据: {alert_deleted} 条")
                    
        except Exception as e:
            print(f"❌ 清理失败: {e}")
    
    def _cleanup_sink_data(self):
        """清理sink端所有数据"""
        try:
            confirm = input("确认清理Sink端所有数据? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            with self.get_sink_connection() as conn:
                with conn.cursor() as cursor:
                    tables = [
                        'ads_realtime_dashboard',
                        'ads_equipment_health', 
                        'ads_customer_behavior',
                        'ads_alert_statistics',
                        'ads_power_quality',
                        'ads_risk_assessment',
                        'ads_energy_efficiency'
                    ]
                    
                    total_deleted = 0
                    for table in tables:
                        try:
                            cursor.execute(f"DELETE FROM {table}")
                            deleted = cursor.rowcount
                            total_deleted += deleted
                            print(f"   {table}: {deleted} 条")
                        except Exception as e:
                            print(f"   {table}: 清理失败 - {e}")
                    
                    conn.commit()
                    print(f"✅ Sink端清理完成，总计删除 {total_deleted} 条记录")
                    
        except Exception as e:
            print(f"❌ 清理失败: {e}")
    
    def _cleanup_all_data(self):
        """清理所有测试数据"""
        try:
            confirm = input("确认清理所有测试数据? 这将删除Source和Sink端的所有数据! (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            print("清理Source端数据...")
            self._cleanup_source_data()
            
            print("\n清理Sink端数据...")
            self._cleanup_sink_data()
            
            print("\n🎉 所有数据清理完成!")
            
        except Exception as e:
            print(f"❌ 清理失败: {e}")
    
    def run(self):
        """运行主程序"""
        try:
            # 测试数据库连接
            with self.get_source_connection() as conn:
                pass
            with self.get_sink_connection() as conn:
                pass
            
            print("✅ 数据库连接正常")
            
            while True:
                self.show_menu()
                choice = input("请选择操作: ").strip()
                
                if choice == '1':
                    self.source_data_operations()
                elif choice == '2':
                    self.auto_generate_data()
                elif choice == '3':
                    self.monitor_sink_data()
                elif choice == '4':
                    self.analyze_data_flow()
                elif choice == '5':
                    self.show_table_structures()
                elif choice == '6':
                    self.cleanup_test_data()
                elif choice == '0':
                    print("👋 再见!")
                    break
                else:
                    print("❌ 无效选择，请重试")
                    
        except psycopg2.Error as e:
            print(f"❌ 数据库连接失败: {e}")
            print("请检查数据库服务是否正常运行")
        except KeyboardInterrupt:
            print("\n👋 程序已退出")
        except Exception as e:
            print(f"❌ 程序运行错误: {e}")

def main():
    manager = DataManager()
    manager.run()

if __name__ == "__main__":
    main()