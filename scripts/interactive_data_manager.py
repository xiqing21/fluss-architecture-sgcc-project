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
        """Source端数据操作菜单 - 增强版CRUD"""
        while True:
            print("\n📊 Source端数据操作 (增强CRUD):")
            print("=== 基础操作 ===")
            print("1. 查看数据统计")
            print("2. 插入客户信息")
            print("3. 插入设备信息")
            print("4. 插入电力消耗数据")
            print("5. 插入告警记录")
            print("=== 查询操作 ===")
            print("6. 批量查询客户")
            print("7. 批量查询设备")
            print("8. 条件查询电力数据")
            print("9. 条件查询告警记录")
            print("=== 更新操作 ===")
            print("10. 更新设备状态")
            print("11. 批量更新客户信息")
            print("12. 批量更新设备信息")
            print("13. 更新告警状态")
            print("=== 删除操作 ===")
            print("14. 删除过期数据")
            print("15. 批量删除客户")
            print("16. 批量删除设备")
            print("17. 条件删除数据")
            print("=== 数据管理 ===")
            print("18. 数据导出")
            print("19. 数据导入")
            print("20. 数据备份")
            print("21. 数据恢复")
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
                self.batch_query_customers()
            elif choice == '7':
                self.batch_query_equipment()
            elif choice == '8':
                self.conditional_query_power_data()
            elif choice == '9':
                self.conditional_query_alerts()
            elif choice == '10':
                self.update_equipment_status()
            elif choice == '11':
                self.batch_update_customers()
            elif choice == '12':
                self.batch_update_equipment()
            elif choice == '13':
                self.update_alert_status()
            elif choice == '14':
                self.delete_expired_data()
            elif choice == '15':
                self.batch_delete_customers()
            elif choice == '16':
                self.batch_delete_equipment()
            elif choice == '17':
                self.conditional_delete_data()
            elif choice == '18':
                self.export_data()
            elif choice == '19':
                self.import_data()
            elif choice == '20':
                self.backup_data()
            elif choice == '21':
                self.restore_data()
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
        """自动生成测试数据 - 增强版CRUD模式"""
        print("\n🤖 智能数据生成器 (增强CRUD模式):")
        print("=== 基础生成模式 ===")
        print("1. 批量生成历史数据")
        print("2. 实时持续生成数据")
        print("3. 定时任务生成数据")
        print("=== 增强CRUD模式 ===")
        print("4. 混合CRUD操作生成")
        print("5. 智能数据演化模拟")
        print("6. 业务场景模拟")
        print("0. 返回")
        
        choice = input("请选择生成模式: ").strip()
        
        if choice == '1':
            self._batch_generate_data()
        elif choice == '2':
            self._realtime_generate_data()
        elif choice == '3':
            self._scheduled_generate_data()
        elif choice == '4':
            self._mixed_crud_generation()
        elif choice == '5':
            self._intelligent_data_evolution()
        elif choice == '6':
            self._business_scenario_simulation()
    
    def _batch_generate_data(self):
        """批量生成历史数据"""
        try:
            print("\n📊 批量数据生成配置:")
            print("\n💡 说明: 客户数量和设备数量用于创建测试数据的基础实体")
            print("   - 客户数量: 创建多少个不同的客户账户 (如CUST_001, CUST_002...)")
            print("   - 设备数量: 创建多少个不同的监控设备 (如EQ_001, EQ_002...)")
            print("   - 生成的电力数据会随机分配给这些客户和设备")
            print("   - 数量越多，数据分布越分散，更接近真实场景\n")
            
            duration_minutes = int(input("生成数据时长(分钟): "))
            records_per_minute = int(input("每分钟记录数: "))
            customer_count = int(input("客户数量 [默认5]: ") or "5")
            equipment_count = int(input("设备数量 [默认10]: ") or "10")
            
            # 新增配置选项
            alert_probability = float(input("告警概率(0-1) [默认0.05]: ") or "0.05")
            data_quality = input("数据质量(high/medium/low) [默认medium]: ").strip() or "medium"
            
            print(f"\n📈 生成配置确认:")
            print(f"   时长: {duration_minutes} 分钟")
            print(f"   频率: {records_per_minute} 条/分钟")
            print(f"   客户数: {customer_count} 个 (将创建 CUST_001 到 CUST_{customer_count:03d})")
            print(f"   设备数: {equipment_count} 个 (将创建 EQ_001 到 EQ_{equipment_count:03d})")
            print(f"   告警概率: {alert_probability*100:.1f}%")
            print(f"   数据质量: {data_quality}")
            print(f"   预计总记录数: {duration_minutes * records_per_minute}")
            print(f"   数据分布: 每条记录随机分配给 {customer_count}个客户 和 {equipment_count}个设备")
            
            confirm = input("\n确认开始生成? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            self._generate_test_data(duration_minutes, records_per_minute, customer_count, equipment_count, alert_probability, data_quality)
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 生成失败: {e}")
    
    def _realtime_generate_data(self):
        """实时持续生成数据"""
        try:
            print("\n🔄 实时数据生成配置:")
            print("\n💡 说明: 实时生成模式会持续向Source端插入新数据")
            print("   - 客户数量和设备数量: 定义数据的分布范围")
            print("   - 生成间隔: 控制数据生成的频率")
            print("   - 每批记录数: 每次生成多少条电力消耗记录")
            print("   - 适用于测试数据流的实时处理能力\n")
            
            interval_seconds = int(input("生成间隔(秒) [默认5]: ") or "5")
            records_per_batch = int(input("每批记录数 [默认3]: ") or "3")
            customer_count = int(input("客户数量 [默认5]: ") or "5")
            equipment_count = int(input("设备数量 [默认10]: ") or "10")
            
            print(f"\n🚀 实时生成配置确认:")
            print(f"   间隔: {interval_seconds} 秒")
            print(f"   每批: {records_per_batch} 条")
            print(f"   客户数: {customer_count} 个 (CUST_001 到 CUST_{customer_count:03d})")
            print(f"   设备数: {equipment_count} 个 (EQ_001 到 EQ_{equipment_count:03d})")
            print(f"   预计速率: {records_per_batch * 60 / interval_seconds:.1f} 条/分钟")
            print("   按 Ctrl+C 停止生成")
            
            confirm = input("\n确认开始? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            self._start_realtime_generation(interval_seconds, records_per_batch, customer_count, equipment_count)
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 生成失败: {e}")
    
    def _scheduled_generate_data(self):
        """定时任务生成数据"""
        try:
            print("\n⏰ 定时任务配置:")
            total_duration = int(input("总运行时长(分钟): "))
            generation_interval = int(input("生成间隔(分钟): "))
            records_per_generation = int(input("每次生成记录数: "))
            
            print(f"\n📅 定时任务配置:")
            print(f"   总时长: {total_duration} 分钟")
            print(f"   间隔: {generation_interval} 分钟")
            print(f"   每次: {records_per_generation} 条")
            print(f"   总次数: {total_duration // generation_interval} 次")
            
            confirm = input("确认启动定时任务? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            self._start_scheduled_generation(total_duration, generation_interval, records_per_generation)
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 生成失败: {e}")
    
    def _generate_test_data(self, duration_minutes: int, records_per_minute: int, customer_count: int, equipment_count: int, alert_probability: float = 0.05, data_quality: str = "medium"):
        """生成测试数据的核心逻辑 - 增强版"""
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
                            
                            # 根据数据质量生成电力消耗数据
                            power_data = self._generate_quality_power_data(data_quality)
                            
                            cursor.execute("""
                                INSERT INTO power_consumption 
                                (customer_id, equipment_id, record_time, active_power, reactive_power,
                                 voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                                 power_factor, frequency, energy_consumption)
                                VALUES (%s, %s, CURRENT_TIMESTAMP - INTERVAL '%s minutes', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                customer_id, equipment_id, duration_minutes - minute - 1,
                                power_data['active_power'],
                                power_data['reactive_power'],
                                power_data['voltage_a'],
                                power_data['voltage_b'],
                                power_data['voltage_c'],
                                power_data['current_a'],
                                power_data['current_b'],
                                power_data['current_c'],
                                power_data['power_factor'],
                                power_data['frequency'],
                                power_data['energy_consumption']
                            ))
                            
                            # 根据配置的概率生成告警
                            if random.random() < alert_probability:
                                alert_type = random.choice(['VOLTAGE_SAG', 'VOLTAGE_SWELL', 'POWER_INTERRUPTION', 'OVERLOAD'])
                                alert_level = random.choice(['INFO', 'WARNING', 'ERROR', 'CRITICAL'])
                                
                                # 根据数据质量调整告警参数
                                alert_value, threshold_value = self._get_quality_adjusted_values(data_quality)
                                
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
                                    alert_value,
                                    threshold_value
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
    
    def _generate_quality_power_data(self, data_quality: str) -> dict:
        """根据数据质量生成电力数据"""
        if data_quality == "high":
            # 高质量数据：更稳定的范围
            return {
                'active_power': random.uniform(80, 120),
                'reactive_power': random.uniform(20, 40),
                'voltage_a': random.uniform(218, 222),
                'voltage_b': random.uniform(218, 222),
                'voltage_c': random.uniform(218, 222),
                'current_a': random.uniform(20, 30),
                'current_b': random.uniform(20, 30),
                'current_c': random.uniform(20, 30),
                'power_factor': random.uniform(0.90, 0.95),
                'frequency': random.uniform(49.8, 50.2),
                'energy_consumption': random.uniform(200, 300)
            }
        elif data_quality == "low":
            # 低质量数据：更大的波动范围
            return {
                'active_power': random.uniform(5, 200),
                'reactive_power': random.uniform(1, 80),
                'voltage_a': random.uniform(200, 250),
                'voltage_b': random.uniform(200, 250),
                'voltage_c': random.uniform(200, 250),
                'current_a': random.uniform(0.5, 80),
                'current_b': random.uniform(0.5, 80),
                'current_c': random.uniform(0.5, 80),
                'power_factor': random.uniform(0.6, 0.98),
                'frequency': random.uniform(48.5, 51.5),
                'energy_consumption': random.uniform(10, 800)
            }
        else:  # medium
            # 中等质量数据：标准范围
            return {
                'active_power': random.uniform(10, 100),
                'reactive_power': random.uniform(5, 50),
                'voltage_a': random.uniform(220, 240),
                'voltage_b': random.uniform(220, 240),
                'voltage_c': random.uniform(220, 240),
                'current_a': random.uniform(1, 50),
                'current_b': random.uniform(1, 50),
                'current_c': random.uniform(1, 50),
                'power_factor': random.uniform(0.8, 0.95),
                'frequency': random.uniform(49.5, 50.5),
                'energy_consumption': random.uniform(50, 500)
            }
    
    def _get_quality_adjusted_values(self, data_quality: str) -> tuple:
        """根据数据质量调整告警值和阈值"""
        if data_quality == "high":
            return random.uniform(150, 200), random.uniform(180, 220)
        elif data_quality == "low":
            return random.uniform(50, 400), random.uniform(100, 350)
        else:  # medium
            return random.uniform(100, 300), random.uniform(200, 250)
    
    def _start_realtime_generation(self, interval_seconds: int, records_per_batch: int, customer_count: int, equipment_count: int):
        """开始实时数据生成"""
        try:
            # 创建基础客户和设备数据
            self._create_base_entities(customer_count, equipment_count)
            
            total_generated = 0
            start_time = time.time()
            
            print(f"\n🚀 实时数据生成已启动...")
            
            while True:
                batch_start = time.time()
                
                # 生成一批数据
                batch_count = self._generate_realtime_batch(records_per_batch, customer_count, equipment_count)
                total_generated += batch_count
                
                elapsed_total = time.time() - start_time
                rate = total_generated / elapsed_total if elapsed_total > 0 else 0
                
                print(f"\r⚡ 已生成: {total_generated} 条 | 速率: {rate:.1f} 条/秒 | 运行时间: {elapsed_total:.0f}s", end="")
                
                # 控制生成间隔
                batch_elapsed = time.time() - batch_start
                if batch_elapsed < interval_seconds:
                    time.sleep(interval_seconds - batch_elapsed)
                    
        except KeyboardInterrupt:
            print(f"\n✅ 实时生成已停止，总计生成 {total_generated} 条记录")
        except Exception as e:
            print(f"\n❌ 实时生成失败: {e}")
    
    def _start_scheduled_generation(self, total_duration: int, generation_interval: int, records_per_generation: int):
        """开始定时任务生成"""
        try:
            total_cycles = total_duration // generation_interval
            total_generated = 0
            
            print(f"\n⏰ 定时任务已启动，共 {total_cycles} 个周期...")
            
            for cycle in range(total_cycles):
                cycle_start = time.time()
                
                # 生成数据
                generated = self._generate_scheduled_batch(records_per_generation)
                total_generated += generated
                
                print(f"\n📊 周期 {cycle+1}/{total_cycles}: 生成 {generated} 条记录")
                print(f"   累计: {total_generated} 条")
                print(f"   下次生成: {generation_interval} 分钟后")
                
                # 等待下一个周期
                if cycle < total_cycles - 1:  # 不是最后一个周期
                    time.sleep(generation_interval * 60)  # 转换为秒
            
            print(f"\n🎉 定时任务完成！总计生成 {total_generated} 条记录")
            
        except KeyboardInterrupt:
            print(f"\n✅ 定时任务已停止，总计生成 {total_generated} 条记录")
        except Exception as e:
            print(f"\n❌ 定时任务失败: {e}")
    
    def _create_base_entities(self, customer_count: int, equipment_count: int):
        """创建基础的客户和设备实体"""
        with self.get_source_connection() as conn:
            with conn.cursor() as cursor:
                # 创建客户
                for i in range(customer_count):
                    customer_id = f"CUST_{i+1:03d}"
                    cursor.execute("""
                        INSERT INTO customer_info 
                        (customer_id, customer_name, customer_type, contact_person, contact_phone,
                         address, contract_capacity, voltage_level, tariff_type, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
                        ON CONFLICT (customer_id) DO NOTHING
                    """, (
                        customer_id, f"客户_{i+1}", random.choice(['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL']),
                        f"联系人_{i+1}", f"138{random.randint(10000000, 99999999)}",
                        f"地址_{i+1}", random.uniform(50, 500), random.choice([220, 380, 10000]), "STANDARD"
                    ))
                
                # 创建设备
                for i in range(equipment_count):
                    equipment_id = f"EQ_{i+1:03d}"
                    cursor.execute("""
                        INSERT INTO equipment_info 
                        (equipment_id, equipment_name, equipment_type, location, voltage_level,
                         capacity, manufacturer, install_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, 'NORMAL')
                        ON CONFLICT (equipment_id) DO NOTHING
                    """, (
                        equipment_id, f"设备_{i+1}", random.choice(['变压器', '发电机', '电动机', '开关柜']),
                        f"位置_{i+1}", random.choice([220, 380, 10000]), random.uniform(50, 1000),
                        random.choice(['ABB', 'Siemens', '施耐德', '华为'])
                    ))
                
                conn.commit()
    
    def _generate_realtime_batch(self, records_count: int, customer_count: int, equipment_count: int) -> int:
        """生成一批实时数据"""
        try:
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    generated = 0
                    
                    for _ in range(records_count):
                        customer_id = f"CUST_{random.randint(1, customer_count):03d}"
                        equipment_id = f"EQ_{random.randint(1, equipment_count):03d}"
                        
                        # 生成电力数据
                        power_data = self._generate_quality_power_data("medium")
                        
                        cursor.execute("""
                            INSERT INTO power_consumption 
                            (customer_id, equipment_id, record_time, active_power, reactive_power,
                             voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                             power_factor, frequency, energy_consumption)
                            VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            customer_id, equipment_id,
                            power_data['active_power'], power_data['reactive_power'],
                            power_data['voltage_a'], power_data['voltage_b'], power_data['voltage_c'],
                            power_data['current_a'], power_data['current_b'], power_data['current_c'],
                            power_data['power_factor'], power_data['frequency'], power_data['energy_consumption']
                        ))
                        
                        generated += 1
                    
                    conn.commit()
                    return generated
                    
        except Exception as e:
            print(f"\n❌ 批量生成失败: {e}")
            return 0
    
    def _generate_scheduled_batch(self, records_count: int) -> int:
        """生成定时任务批次数据"""
        return self._generate_realtime_batch(records_count, 5, 10)  # 使用默认的客户和设备数量
    
    def monitor_sink_data(self):
        """监控sink端数据 - 数据变化监控"""
        print("\n📊 Sink端数据监控:")
        print("1. 实时数据变化监控 (每5秒刷新)")
        print("2. 单次查询")
        print("3. 数据延迟监控")
        print("4. 数据流问题诊断 🔍")
        print("0. 返回")
        
        choice = input("请选择: ").strip()
        
        if choice == '1':
            self._realtime_data_change_monitor()
        elif choice == '2':
            self.show_sink_statistics()
        elif choice == '3':
            self._realtime_delay_monitor()
        elif choice == '4':
            self._diagnose_data_flow_issues()
        elif choice == '0':
            return
    
    def _realtime_data_change_monitor(self):
        """实时数据变化监控"""
        print("\n🔄 开始实时数据变化监控 (按 Ctrl+C 停止)...")
        
        # 存储上一次的数据统计
        previous_stats = {}
        
        try:
            while True:
                print("\033[2J\033[H")  # 清屏
                print(f"📊 实时数据变化监控 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*80)
                
                current_stats = self._get_table_stats()
                
                # 显示数据变化
                for table, stats in current_stats.items():
                    if table in previous_stats:
                        count_change = stats['count'] - previous_stats[table]['count']
                        change_indicator = "📈" if count_change > 0 else "📊" if count_change == 0 else "📉"
                        change_text = f"(+{count_change})" if count_change > 0 else f"({count_change})" if count_change < 0 else "(无变化)"
                    else:
                        change_indicator = "🆕"
                        change_text = "(新表)"
                    
                    time_str = stats['latest_time'].strftime('%H:%M:%S') if stats['latest_time'] else "无数据"
                    print(f"   {change_indicator} {table:<25}: {stats['count']:>6} 条 {change_text:<10} 最新: {time_str} - {stats['description']}")
                
                # 显示延迟信息
                self._show_delay_summary()
                
                previous_stats = current_stats
                print("\n按 Ctrl+C 停止监控...")
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n✅ 数据变化监控已停止")
    
    def _realtime_delay_monitor(self):
        """实时延迟监控"""
        print("\n⏱️ 开始实时延迟监控 (按 Ctrl+C 停止)...")
        
        try:
            while True:
                print("\033[2J\033[H")  # 清屏
                print(f"⏱️ 实时延迟监控 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*80)
                
                self._detailed_delay_analysis()
                
                print("\n按 Ctrl+C 停止监控...")
                time.sleep(3)  # 延迟监控更频繁
                
        except KeyboardInterrupt:
            print("\n✅ 延迟监控已停止")
    
    def _get_table_stats(self) -> dict:
        """获取表统计信息"""
        tables_info = {
            'ads_realtime_dashboard': '实时监控大屏数据',
            'ads_equipment_health': '设备健康状态分析', 
            'ads_customer_behavior': '客户行为分析',
            'ads_alert_statistics': '告警统计分析',
            'ads_power_quality': '电力质量分析',
            'ads_risk_assessment': '风险评估分析',
            'ads_energy_efficiency': '能效分析'
        }
        
        stats = {}
        
        for table, description in tables_info.items():
            try:
                with self.get_sink_connection() as conn:
                    conn.autocommit = True
                    with conn.cursor() as cursor:
                        # 检查表是否存在
                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = %s
                            )
                        """, (table,))
                        
                        if not cursor.fetchone()[0]:
                            continue
                        
                        # 获取记录数
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        
                        # 获取最新时间
                        latest_time = None
                        time_columns = ['update_time', 'stat_time', 'analysis_time', 'created_at', 'alert_time']
                        
                        for col in time_columns:
                            try:
                                cursor.execute(f"""
                                    SELECT column_name FROM information_schema.columns 
                                    WHERE table_name = %s AND column_name = %s
                                """, (table, col))
                                
                                if cursor.fetchone():
                                    cursor.execute(f"SELECT MAX({col}) FROM {table} WHERE {col} IS NOT NULL")
                                    result = cursor.fetchone()
                                    if result and result[0]:
                                        latest_time = result[0]
                                        break
                            except Exception:
                                continue
                        
                        stats[table] = {
                            'count': count,
                            'latest_time': latest_time,
                            'description': description
                        }
                        
            except Exception as e:
                stats[table] = {
                    'count': 0,
                    'latest_time': None,
                    'description': f"{description} (错误: {str(e)[:20]})"
                }
        
        return stats
    
    def _show_delay_summary(self):
        """显示延迟摘要 - 修复版：计算同一批数据的真实流转延迟"""
        try:
            print("\n⏱️ 数据流转延迟摘要:")
            
            current_time = datetime.datetime.now()
            
            # 获取最近的Source端数据时间戳
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT MAX(record_time), COUNT(*) 
                        FROM power_consumption 
                        WHERE record_time >= CURRENT_TIMESTAMP - INTERVAL '10 minutes'
                    """)
                    source_result = cursor.fetchone()
                    latest_source_time = source_result[0] if source_result[0] else None
                    recent_source_count = source_result[1] if source_result[1] else 0
            
            if not latest_source_time:
                print("   🔴 Source端无最近数据 (10分钟内)")
                return
            
            # 计算Source端数据新鲜度
            source_freshness = (current_time - latest_source_time.replace(tzinfo=None)).total_seconds()
            status = "🟢" if source_freshness < 30 else "🟡" if source_freshness < 120 else "🔴"
            print(f"   {status} Source端数据新鲜度: {source_freshness:.1f}秒 (最近10分钟: {recent_source_count}条)")
            
            # 检查各个Sink表的数据流转延迟
            sink_tables = {
                'ads_realtime_dashboard': ['update_time', 'created_at'],
                'ads_power_quality': ['analysis_time', 'stat_time'], 
                'ads_equipment_health': ['analysis_time', 'update_time'],
                'ads_alert_statistics': ['stat_time', 'update_time'],
                'ads_customer_behavior': ['analysis_time', 'update_time'],
                'ads_risk_assessment': ['analysis_time', 'update_time'],
                'ads_energy_efficiency': ['analysis_time', 'update_time']
            }
            
            print("\n   📊 各表数据流转延迟:")
            active_tables = 0
            total_delay = 0
            
            with self.get_sink_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    for table, time_cols in sink_tables.items():
                        try:
                            # 检查表是否存在
                            cursor.execute("""
                                SELECT EXISTS (
                                    SELECT FROM information_schema.tables 
                                    WHERE table_schema = 'public' AND table_name = %s
                                )
                            """, (table,))
                            
                            if not cursor.fetchone()[0]:
                                print(f"     ⚠️  {table:<25}: 表不存在")
                                continue
                            
                            # 查找最新的时间戳
                            latest_sink_time = None
                            sink_count = 0
                            
                            for col in time_cols:
                                try:
                                    cursor.execute(f"""
                                        SELECT column_name FROM information_schema.columns 
                                        WHERE table_name = %s AND column_name = %s
                                    """, (table, col))
                                    
                                    if cursor.fetchone():
                                        cursor.execute(f"""
                                            SELECT MAX({col}), COUNT(*) 
                                            FROM {table} 
                                            WHERE {col} >= %s
                                        """, (latest_source_time - datetime.timedelta(minutes=15),))
                                        result = cursor.fetchone()
                                        if result and result[0]:
                                            latest_sink_time = result[0]
                                            sink_count = result[1]
                                            break
                                except Exception:
                                    continue
                            
                            if latest_sink_time:
                                # 计算数据流转延迟（同一时间窗口内的数据）
                                flow_delay = (latest_sink_time.replace(tzinfo=None) - latest_source_time.replace(tzinfo=None)).total_seconds()
                                sink_freshness = (current_time - latest_sink_time.replace(tzinfo=None)).total_seconds()
                                
                                # 状态判断
                                if abs(flow_delay) < 60 and sink_freshness < 120:
                                    status = "🟢"
                                elif abs(flow_delay) < 300 and sink_freshness < 600:
                                    status = "🟡"
                                else:
                                    status = "🔴"
                                
                                print(f"     {status} {table:<25}: 流转延迟 {flow_delay:>6.1f}秒, 数据新鲜度 {sink_freshness:>6.1f}秒 ({sink_count}条)")
                                active_tables += 1
                                total_delay += abs(flow_delay)
                            else:
                                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                                total_count = cursor.fetchone()[0]
                                print(f"     🔴 {table:<25}: 无匹配时间窗口数据 (总计{total_count}条)")
                                
                        except Exception as e:
                            print(f"     ❌ {table:<25}: 查询失败 - {str(e)[:30]}")
            
            # 显示整体流转性能
            if active_tables > 0:
                avg_delay = total_delay / active_tables
                table_coverage = (active_tables / len(sink_tables)) * 100
                
                print(f"\n   📈 整体流转性能:")
                print(f"     活跃表数: {active_tables}/{len(sink_tables)} ({table_coverage:.1f}%)")
                print(f"     平均延迟: {avg_delay:.1f}秒")
                
                if avg_delay < 60 and table_coverage >= 80:
                    print(f"     🟢 数据流转状态: 优秀")
                elif avg_delay < 180 and table_coverage >= 60:
                    print(f"     🟡 数据流转状态: 良好")
                else:
                    print(f"     🔴 数据流转状态: 需要优化")
            else:
                print(f"\n   🔴 无活跃的Sink表，数据流可能存在问题")
                
        except Exception as e:
            print(f"   ❌ 延迟分析失败: {e}")
    
    def _detailed_delay_analysis(self):
        """详细延迟分析"""
        try:
            print("\n📊 详细延迟分析:")
            
            current_time = datetime.datetime.now()
            
            # Source端各表延迟
            print("\n🔵 Source端数据延迟:")
            source_tables = {
                'power_consumption': 'record_time',
                'alert_records': 'alert_time',
                'equipment_status': 'status_time'
            }
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    for table, time_col in source_tables.items():
                        try:
                            cursor.execute(f"SELECT MAX({time_col}), COUNT(*) FROM {table}")
                            result = cursor.fetchone()
                            if result and result[0]:
                                delay = (current_time - result[0].replace(tzinfo=None)).total_seconds()
                                status = "🟢" if delay < 30 else "🟡" if delay < 120 else "🔴"
                                print(f"   {status} {table:<20}: {delay:>6.1f}秒 ({result[1]} 条记录)")
                            else:
                                print(f"   🔴 {table:<20}: 无数据")
                        except Exception as e:
                            print(f"   ❌ {table:<20}: 查询失败 - {str(e)[:30]}")
            
            # Sink端各表延迟
            print("\n🟠 Sink端数据延迟:")
            sink_tables = {
                'ads_realtime_dashboard': ['update_time', 'created_at'],
                'ads_power_quality': ['analysis_time', 'stat_time'],
                'ads_equipment_health': ['analysis_time', 'update_time'],
                'ads_alert_statistics': ['stat_time', 'update_time']
            }
            
            with self.get_sink_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    for table, time_cols in sink_tables.items():
                        try:
                            # 检查表是否存在
                            cursor.execute("""
                                SELECT EXISTS (
                                    SELECT FROM information_schema.tables 
                                    WHERE table_schema = 'public' AND table_name = %s
                                )
                            """, (table,))
                            
                            if not cursor.fetchone()[0]:
                                print(f"   ⚠️  {table:<25}: 表不存在")
                                continue
                            
                            latest_time = None
                            for col in time_cols:
                                try:
                                    cursor.execute(f"""
                                        SELECT column_name FROM information_schema.columns 
                                        WHERE table_name = %s AND column_name = %s
                                    """, (table, col))
                                    
                                    if cursor.fetchone():
                                        cursor.execute(f"SELECT MAX({col}), COUNT(*) FROM {table} WHERE {col} IS NOT NULL")
                                        result = cursor.fetchone()
                                        if result and result[0]:
                                            latest_time = result[0]
                                            count = result[1]
                                            break
                                except Exception:
                                    continue
                            
                            if latest_time:
                                delay = (current_time - latest_time.replace(tzinfo=None)).total_seconds()
                                status = "🟢" if delay < 60 else "🟡" if delay < 300 else "🔴"
                                print(f"   {status} {table:<25}: {delay:>6.1f}秒 ({count} 条记录)")
                            else:
                                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                                count = cursor.fetchone()[0]
                                print(f"   🔴 {table:<25}: 无时间数据 ({count} 条记录)")
                                
                        except Exception as e:
                            print(f"   ❌ {table:<25}: 查询失败 - {str(e)[:30]}")
            
            # 数据流健康度评估
            print("\n🏥 数据流健康度评估:")
            self._assess_data_flow_health()
                
        except Exception as e:
            print(f"❌ 详细延迟分析失败: {e}")
    
    def _assess_data_flow_health(self):
        """评估数据流健康度"""
        try:
            health_score = 100
            issues = []
            
            # 检查Source端数据生成
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT MAX(record_time) FROM power_consumption")
                    latest_source = cursor.fetchone()[0]
                    
                    if latest_source:
                        source_delay = (datetime.datetime.now() - latest_source.replace(tzinfo=None)).total_seconds()
                        if source_delay > 300:  # 5分钟
                            health_score -= 30
                            issues.append(f"Source端数据过旧 ({source_delay:.0f}秒)")
                        elif source_delay > 120:  # 2分钟
                            health_score -= 15
                            issues.append(f"Source端数据延迟 ({source_delay:.0f}秒)")
                    else:
                        health_score -= 50
                        issues.append("Source端无数据")
            
            # 检查Sink端数据处理
            with self.get_sink_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    active_tables = 0
                    total_tables = 7
                    
                    for table in ['ads_realtime_dashboard', 'ads_power_quality', 'ads_equipment_health', 
                                'ads_alert_statistics', 'ads_customer_behavior', 'ads_risk_assessment', 'ads_energy_efficiency']:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            if cursor.fetchone()[0] > 0:
                                active_tables += 1
                        except:
                            pass
                    
                    table_health = (active_tables / total_tables) * 100
                    if table_health < 50:
                        health_score -= 30
                        issues.append(f"Sink端表活跃度低 ({active_tables}/{total_tables})")
                    elif table_health < 80:
                        health_score -= 15
                        issues.append(f"部分Sink端表无数据 ({active_tables}/{total_tables})")
            
            # 显示健康度
            if health_score >= 90:
                status = "🟢 优秀"
            elif health_score >= 70:
                status = "🟡 良好"
            elif health_score >= 50:
                status = "🟠 一般"
            else:
                status = "🔴 差"
            
            print(f"   整体健康度: {status} ({health_score}/100)")
            
            if issues:
                print("   发现问题:")
                for issue in issues:
                    print(f"     • {issue}")
            else:
                print("   ✅ 数据流运行正常")
                
        except Exception as e:
            print(f"   ❌ 健康度评估失败: {e}")
    
    def _diagnose_data_flow_issues(self):
        """诊断数据流问题 - 分析为什么只有一个表变化"""
        print("\n🔍 数据流问题诊断:")
        print("="*60)
        
        try:
            # 1. 检查Source端数据状态
            print("\n📥 Source端数据检查:")
            source_stats = {}
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    # 检查各个Source表的数据
                    source_tables = {
                        'power_consumption': 'record_time',
                        'alert_records': 'alert_time', 
                        'equipment_status': 'status_time',
                        'customer_info': 'created_at',
                        'equipment_info': 'install_date'
                    }
                    
                    for table, time_col in source_tables.items():
                        try:
                            cursor.execute(f"SELECT COUNT(*), MAX({time_col}) FROM {table}")
                            result = cursor.fetchone()
                            count = result[0] if result else 0
                            latest_time = result[1] if result and result[1] else None
                            
                            # 检查最近数据
                            cursor.execute(f"""
                                SELECT COUNT(*) FROM {table} 
                                WHERE {time_col} >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                            """)
                            recent_count = cursor.fetchone()[0]
                            
                            source_stats[table] = {
                                'total_count': count,
                                'latest_time': latest_time,
                                'recent_count': recent_count
                            }
                            
                            if recent_count > 0:
                                status = "🟢"
                            elif count > 0:
                                status = "🟡"
                            else:
                                status = "🔴"
                            
                            time_str = latest_time.strftime('%H:%M:%S') if latest_time else "无数据"
                            print(f"   {status} {table:<20}: 总计{count:>6}条, 近1小时{recent_count:>4}条, 最新: {time_str}")
                            
                        except Exception as e:
                            print(f"   ❌ {table:<20}: 查询失败 - {str(e)[:30]}")
            
            # 2. 检查Sink端表状态
            print("\n📤 Sink端表状态检查:")
            sink_stats = {}
            
            with self.get_sink_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    sink_tables = {
                        'ads_realtime_dashboard': ['update_time', 'created_at'],
                        'ads_power_quality': ['analysis_time', 'stat_time'],
                        'ads_equipment_health': ['analysis_time', 'update_time'],
                        'ads_alert_statistics': ['stat_time', 'update_time'],
                        'ads_customer_behavior': ['analysis_time', 'update_time'],
                        'ads_risk_assessment': ['analysis_time', 'update_time'],
                        'ads_energy_efficiency': ['analysis_time', 'update_time']
                    }
                    
                    for table, time_cols in sink_tables.items():
                        try:
                            # 检查表是否存在
                            cursor.execute("""
                                SELECT EXISTS (
                                    SELECT FROM information_schema.tables 
                                    WHERE table_schema = 'public' AND table_name = %s
                                )
                            """, (table,))
                            
                            if not cursor.fetchone()[0]:
                                print(f"   🔴 {table:<25}: 表不存在")
                                continue
                            
                            # 获取总记录数
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            total_count = cursor.fetchone()[0]
                            
                            # 查找最新时间戳
                            latest_time = None
                            recent_count = 0
                            
                            for col in time_cols:
                                try:
                                    cursor.execute(f"""
                                        SELECT column_name FROM information_schema.columns 
                                        WHERE table_name = %s AND column_name = %s
                                    """, (table, col))
                                    
                                    if cursor.fetchone():
                                        cursor.execute(f"SELECT MAX({col}) FROM {table} WHERE {col} IS NOT NULL")
                                        result = cursor.fetchone()
                                        if result and result[0]:
                                            latest_time = result[0]
                                            
                                            # 检查最近数据
                                            cursor.execute(f"""
                                                SELECT COUNT(*) FROM {table} 
                                                WHERE {col} >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                                            """)
                                            recent_count = cursor.fetchone()[0]
                                            break
                                except Exception:
                                    continue
                            
                            sink_stats[table] = {
                                'total_count': total_count,
                                'latest_time': latest_time,
                                'recent_count': recent_count
                            }
                            
                            if recent_count > 0:
                                status = "🟢"
                            elif total_count > 0:
                                status = "🟡"
                            else:
                                status = "🔴"
                            
                            time_str = latest_time.strftime('%H:%M:%S') if latest_time else "无数据"
                            print(f"   {status} {table:<25}: 总计{total_count:>6}条, 近1小时{recent_count:>4}条, 最新: {time_str}")
                            
                        except Exception as e:
                            print(f"   ❌ {table:<25}: 查询失败 - {str(e)[:30]}")
            
            # 3. 问题分析和建议
            print("\n🔧 问题分析和建议:")
            
            # 分析活跃的Sink表数量
            active_sink_tables = sum(1 for stats in sink_stats.values() if stats.get('recent_count', 0) > 0)
            total_sink_tables = len(sink_stats)
            
            if active_sink_tables <= 1:
                print(f"   🔴 发现问题: 只有 {active_sink_tables} 个Sink表有最近数据")
                print("   💡 可能原因:")
                print("     1. Fluss数据流配置问题 - 检查Fluss作业是否正常运行")
                print("     2. Source到Sink的数据映射配置不完整")
                print("     3. 某些Sink表的触发条件未满足")
                print("     4. 数据处理逻辑存在错误")
                
                print("   🛠️  建议解决方案:")
                print("     1. 检查Fluss Web UI中的作业状态")
                print("     2. 查看Fluss日志文件中的错误信息")
                print("     3. 验证Source端数据是否满足Sink表的处理条件")
                print("     4. 重启Fluss服务或重新提交作业")
            
            elif active_sink_tables < total_sink_tables * 0.7:
                print(f"   🟡 部分问题: {active_sink_tables}/{total_sink_tables} 个Sink表活跃")
                print("   💡 可能原因: 部分数据流配置或处理逻辑需要优化")
            
            else:
                print(f"   🟢 数据流状态良好: {active_sink_tables}/{total_sink_tables} 个Sink表活跃")
            
            # 检查Source端数据生成
            active_source_tables = sum(1 for stats in source_stats.values() if stats.get('recent_count', 0) > 0)
            if active_source_tables == 0:
                print("   🔴 Source端无最近数据生成，建议先生成测试数据")
            elif active_source_tables == 1:
                print("   🟡 Source端只有一个表有最近数据，可能影响Sink端处理")
            
            print("\n📋 下一步操作建议:")
            print("   1. 如果Source端数据不足，请使用'生成数据'功能")
            print("   2. 如果Sink端表不活跃，请检查Fluss服务状态")
            print("   3. 使用'数据延迟监控'功能持续观察数据流转情况")
            
        except Exception as e:
            print(f"❌ 诊断失败: {e}")
    
    def start_real_time_monitoring(self):
        """开始实时监控 (保留兼容性)"""
        self._realtime_data_change_monitor()
    
    def show_sink_statistics(self):
        """显示sink端数据统计 - 修复版"""
        # 表名及其中文注释
        tables_info = {
            'ads_realtime_dashboard': '实时监控大屏数据',
            'ads_equipment_health': '设备健康状态分析', 
            'ads_customer_behavior': '客户行为分析',
            'ads_alert_statistics': '告警统计分析',
            'ads_power_quality': '电力质量分析',
            'ads_risk_assessment': '风险评估分析',
            'ads_energy_efficiency': '能效分析'
        }
        
        print("\n📈 Sink端ADS层数据统计:")
        
        for table, description in tables_info.items():
            # 为每个表使用独立连接，避免事务中止影响
            try:
                with self.get_sink_connection() as conn:
                    conn.autocommit = True  # 设置自动提交，避免事务问题
                    with conn.cursor() as cursor:
                        # 检查表是否存在
                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = %s
                            )
                        """, (table,))
                        
                        if not cursor.fetchone()[0]:
                            print(f"   ⚠️  {table:<25}: 表不存在 - {description}")
                            continue
                        
                        # 获取记录数
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        
                        # 尝试获取最新更新时间
                        latest_time = None
                        time_columns = ['update_time', 'stat_time', 'analysis_time', 'created_at', 'alert_time']
                        
                        for col in time_columns:
                            try:
                                cursor.execute(f"""
                                    SELECT column_name FROM information_schema.columns 
                                    WHERE table_name = %s AND column_name = %s
                                """, (table, col))
                                
                                if cursor.fetchone():  # 列存在
                                    cursor.execute(f"SELECT MAX({col}) FROM {table} WHERE {col} IS NOT NULL")
                                    result = cursor.fetchone()
                                    if result and result[0]:
                                        latest_time = result[0]
                                        break
                            except Exception:
                                continue
                        
                        status = "✅" if count > 0 else "❌"
                        time_str = latest_time.strftime('%H:%M:%S') if latest_time else "无数据"
                        print(f"   {status} {table:<25}: {count:>6} 条  最新: {time_str} - {description}")
                        
            except Exception as e:
                print(f"   ❌ {table:<25}: 连接失败 - {str(e)[:30]} - {description}")
    
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
        """显示source端表结构 - 带中文注释"""
        try:
            # Source端表及字段的中文注释
            tables_info = {
                'customer_info': {
                    'name': '客户信息表',
                    'fields': {
                        'customer_id': '客户编号',
                        'customer_name': '客户名称',
                        'customer_type': '客户类型',
                        'contact_person': '联系人',
                        'contact_phone': '联系电话',
                        'address': '地址',
                        'contract_capacity': '合同容量(kW)',
                        'voltage_level': '电压等级(V)',
                        'tariff_type': '电价类型',
                        'status': '状态',
                        'created_at': '创建时间',
                        'updated_at': '更新时间'
                    }
                },
                'equipment_info': {
                    'name': '设备信息表',
                    'fields': {
                        'equipment_id': '设备编号',
                        'equipment_name': '设备名称',
                        'equipment_type': '设备类型',
                        'location': '安装位置',
                        'voltage_level': '额定电压(V)',
                        'capacity': '额定容量(kW)',
                        'manufacturer': '制造商',
                        'install_date': '安装日期',
                        'status': '运行状态',
                        'created_at': '创建时间',
                        'updated_at': '更新时间'
                    }
                },
                'power_consumption': {
                    'name': '电力消耗数据表',
                    'fields': {
                        'id': '记录ID',
                        'customer_id': '客户编号',
                        'equipment_id': '设备编号',
                        'record_time': '记录时间',
                        'active_power': '有功功率(kW)',
                        'reactive_power': '无功功率(kVar)',
                        'voltage_a': 'A相电压(V)',
                        'voltage_b': 'B相电压(V)',
                        'voltage_c': 'C相电压(V)',
                        'current_a': 'A相电流(A)',
                        'current_b': 'B相电流(A)',
                        'current_c': 'C相电流(A)',
                        'power_factor': '功率因数',
                        'frequency': '频率(Hz)',
                        'energy_consumption': '电能消耗(kWh)'
                    }
                },
                'alert_records': {
                    'name': '告警记录表',
                    'fields': {
                        'id': '告警ID',
                        'customer_id': '客户编号',
                        'equipment_id': '设备编号',
                        'alert_type': '告警类型',
                        'alert_level': '告警级别',
                        'alert_message': '告警信息',
                        'alert_time': '告警时间',
                        'alert_value': '告警数值',
                        'threshold_value': '阈值',
                        'status': '处理状态',
                        'resolved_time': '解决时间',
                        'resolved_by': '处理人员'
                    }
                },
                'equipment_status': {
                    'name': '设备状态表',
                    'fields': {
                        'id': '状态ID',
                        'equipment_id': '设备编号',
                        'status_time': '状态时间',
                        'operational_status': '运行状态',
                        'health_score': '健康评分',
                        'temperature': '温度(°C)',
                        'vibration': '振动值',
                        'load_rate': '负载率(%)',
                        'efficiency': '效率(%)',
                        'maintenance_due': '维护到期日',
                        'last_maintenance': '上次维护时间'
                    }
                }
            }
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    for table, info in tables_info.items():
                        print(f"\n📊 {table} - {info['name']}:")
                        
                        # 检查表是否存在
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = '{table}'
                            )
                        """)
                        
                        if not cursor.fetchone()[0]:
                            print(f"   ⚠️  表不存在")
                            continue
                        
                        cursor.execute(f"""
                            SELECT column_name, data_type, is_nullable, column_default
                            FROM information_schema.columns 
                            WHERE table_name = '{table}'
                            ORDER BY ordinal_position
                        """)
                        columns = cursor.fetchall()
                        
                        for col in columns:
                            field_name = col[0]
                            field_comment = info['fields'].get(field_name, '未知字段')
                            nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                            default = f" DEFAULT {col[3]}" if col[3] else ""
                            print(f"   {field_name:<20} {col[1]:<15} {nullable:<8} - {field_comment}{default}")
                        
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"   📈 当前记录数: {count}")
                        
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
                    cursor.execute("DELETE FROM power_consumption WHERE customer_id LIKE 'CUST_%' OR equipment_id LIKE 'EQ_%' OR equipment_id LIKE 'TEST_%'")
                    power_deleted = cursor.rowcount
                    
                    cursor.execute("DELETE FROM alert_records WHERE customer_id LIKE 'CUST_%' OR equipment_id LIKE 'EQ_%' OR equipment_id LIKE 'TEST_%'")
                    alert_deleted = cursor.rowcount
                    
                    # 先删除equipment_status表的数据（外键约束）
                    cursor.execute("DELETE FROM equipment_status WHERE equipment_id LIKE 'EQ_%' OR equipment_id LIKE 'TEST_%'")
                    status_deleted = cursor.rowcount
                    
                    cursor.execute("DELETE FROM equipment_info WHERE equipment_id LIKE 'EQ_%' OR equipment_id LIKE 'TEST_%'")
                    equipment_deleted = cursor.rowcount
                    
                    cursor.execute("DELETE FROM customer_info WHERE customer_id LIKE 'CUST_%'")
                    customer_deleted = cursor.rowcount
                    
                    conn.commit()
                    print(f"✅ Source端清理完成:")
                    print(f"   客户: {customer_deleted} 条")
                    print(f"   设备: {equipment_deleted} 条")
                    print(f"   设备状态: {status_deleted} 条")
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
    
    # ==================== 新增的CRUD方法 ====================
    
    def batch_query_customers(self):
        """批量查询客户"""
        try:
            print("\n🔍 批量查询客户:")
            print("1. 查询所有客户")
            print("2. 按客户类型查询")
            print("3. 按状态查询")
            print("4. 按合同容量范围查询")
            
            choice = input("请选择查询方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        cursor.execute("SELECT * FROM customer_info ORDER BY customer_id")
                    elif choice == '2':
                        customer_type = input("客户类型 (RESIDENTIAL/COMMERCIAL/INDUSTRIAL): ").strip()
                        cursor.execute("SELECT * FROM customer_info WHERE customer_type = %s ORDER BY customer_id", (customer_type,))
                    elif choice == '3':
                        status = input("状态 (ACTIVE/INACTIVE): ").strip()
                        cursor.execute("SELECT * FROM customer_info WHERE status = %s ORDER BY customer_id", (status,))
                    elif choice == '4':
                        min_capacity = float(input("最小容量(kW): ") or "0")
                        max_capacity = float(input("最大容量(kW): ") or "999999")
                        cursor.execute("SELECT * FROM customer_info WHERE contract_capacity BETWEEN %s AND %s ORDER BY customer_id", (min_capacity, max_capacity))
                    else:
                        print("❌ 无效选择")
                        return
                    
                    results = cursor.fetchall()
                    if results:
                        print(f"\n📊 查询结果 ({len(results)} 条):")
                        for row in results:
                            print(f"   ID: {row[0]}, 名称: {row[1]}, 类型: {row[2]}, 状态: {row[9]}")
                    else:
                        print("❌ 未找到匹配的客户")
                        
        except Exception as e:
            print(f"❌ 查询失败: {e}")
    
    def batch_query_equipment(self):
        """批量查询设备"""
        try:
            print("\n🔍 批量查询设备:")
            print("1. 查询所有设备")
            print("2. 按设备类型查询")
            print("3. 按状态查询")
            print("4. 按容量范围查询")
            
            choice = input("请选择查询方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        cursor.execute("SELECT * FROM equipment_info ORDER BY equipment_id")
                    elif choice == '2':
                        equipment_type = input("设备类型: ").strip()
                        cursor.execute("SELECT * FROM equipment_info WHERE equipment_type = %s ORDER BY equipment_id", (equipment_type,))
                    elif choice == '3':
                        status = input("状态 (NORMAL/FAULT/MAINTENANCE/OFFLINE): ").strip()
                        cursor.execute("SELECT * FROM equipment_info WHERE status = %s ORDER BY equipment_id", (status,))
                    elif choice == '4':
                        min_capacity = float(input("最小容量(kW): ") or "0")
                        max_capacity = float(input("最大容量(kW): ") or "999999")
                        cursor.execute("SELECT * FROM equipment_info WHERE capacity BETWEEN %s AND %s ORDER BY equipment_id", (min_capacity, max_capacity))
                    else:
                        print("❌ 无效选择")
                        return
                    
                    results = cursor.fetchall()
                    if results:
                        print(f"\n📊 查询结果 ({len(results)} 条):")
                        for row in results:
                            print(f"   ID: {row[0]}, 名称: {row[1]}, 类型: {row[2]}, 状态: {row[8]}")
                    else:
                        print("❌ 未找到匹配的设备")
                        
        except Exception as e:
            print(f"❌ 查询失败: {e}")
    
    def conditional_query_power_data(self):
        """条件查询电力数据"""
        try:
            print("\n🔍 条件查询电力数据:")
            print("1. 按时间范围查询")
            print("2. 按客户ID查询")
            print("3. 按设备ID查询")
            print("4. 按功率范围查询")
            
            choice = input("请选择查询方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        hours = int(input("查询最近多少小时的数据: ") or "24")
                        cursor.execute("""
                            SELECT customer_id, equipment_id, record_time, active_power, reactive_power 
                            FROM power_consumption 
                            WHERE record_time >= CURRENT_TIMESTAMP - INTERVAL '%s hours'
                            ORDER BY record_time DESC LIMIT 100
                        """, (hours,))
                    elif choice == '2':
                        customer_id = input("客户ID: ").strip()
                        cursor.execute("""
                            SELECT customer_id, equipment_id, record_time, active_power, reactive_power 
                            FROM power_consumption 
                            WHERE customer_id = %s 
                            ORDER BY record_time DESC LIMIT 100
                        """, (customer_id,))
                    elif choice == '3':
                        equipment_id = input("设备ID: ").strip()
                        cursor.execute("""
                            SELECT customer_id, equipment_id, record_time, active_power, reactive_power 
                            FROM power_consumption 
                            WHERE equipment_id = %s 
                            ORDER BY record_time DESC LIMIT 100
                        """, (equipment_id,))
                    elif choice == '4':
                        min_power = float(input("最小功率(kW): ") or "0")
                        max_power = float(input("最大功率(kW): ") or "999999")
                        cursor.execute("""
                            SELECT customer_id, equipment_id, record_time, active_power, reactive_power 
                            FROM power_consumption 
                            WHERE active_power BETWEEN %s AND %s 
                            ORDER BY record_time DESC LIMIT 100
                        """, (min_power, max_power))
                    else:
                        print("❌ 无效选择")
                        return
                    
                    results = cursor.fetchall()
                    if results:
                        print(f"\n📊 查询结果 ({len(results)} 条):")
                        for row in results:
                            print(f"   客户: {row[0]}, 设备: {row[1]}, 时间: {row[2]}, 有功功率: {row[3]}kW, 无功功率: {row[4]}kVar")
                    else:
                        print("❌ 未找到匹配的数据")
                        
        except Exception as e:
            print(f"❌ 查询失败: {e}")
    
    def conditional_query_alerts(self):
        """条件查询告警记录"""
        try:
            print("\n🔍 条件查询告警记录:")
            print("1. 按告警级别查询")
            print("2. 按告警类型查询")
            print("3. 按状态查询")
            print("4. 按时间范围查询")
            
            choice = input("请选择查询方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        alert_level = input("告警级别 (INFO/WARNING/ERROR/CRITICAL): ").strip()
                        cursor.execute("""
                            SELECT equipment_id, customer_id, alert_type, alert_level, alert_title, alert_time, status 
                            FROM alert_records 
                            WHERE alert_level = %s 
                            ORDER BY alert_time DESC LIMIT 100
                        """, (alert_level,))
                    elif choice == '2':
                        alert_type = input("告警类型: ").strip()
                        cursor.execute("""
                            SELECT equipment_id, customer_id, alert_type, alert_level, alert_title, alert_time, status 
                            FROM alert_records 
                            WHERE alert_type = %s 
                            ORDER BY alert_time DESC LIMIT 100
                        """, (alert_type,))
                    elif choice == '3':
                        status = input("状态 (OPEN/CLOSED/ACKNOWLEDGED): ").strip()
                        cursor.execute("""
                            SELECT equipment_id, customer_id, alert_type, alert_level, alert_title, alert_time, status 
                            FROM alert_records 
                            WHERE status = %s 
                            ORDER BY alert_time DESC LIMIT 100
                        """, (status,))
                    elif choice == '4':
                        hours = int(input("查询最近多少小时的告警: ") or "24")
                        cursor.execute("""
                            SELECT equipment_id, customer_id, alert_type, alert_level, alert_title, alert_time, status 
                            FROM alert_records 
                            WHERE alert_time >= CURRENT_TIMESTAMP - INTERVAL '%s hours'
                            ORDER BY alert_time DESC LIMIT 100
                        """, (hours,))
                    else:
                        print("❌ 无效选择")
                        return
                    
                    results = cursor.fetchall()
                    if results:
                        print(f"\n📊 查询结果 ({len(results)} 条):")
                        for row in results:
                            print(f"   设备: {row[0]}, 客户: {row[1]}, 类型: {row[2]}, 级别: {row[3]}, 标题: {row[4]}, 状态: {row[6]}")
                    else:
                        print("❌ 未找到匹配的告警")
                        
        except Exception as e:
            print(f"❌ 查询失败: {e}")
    
    def batch_update_customers(self):
        """批量更新客户信息"""
        try:
            print("\n✏️ 批量更新客户信息:")
            print("1. 批量更新客户状态")
            print("2. 批量更新电价类型")
            print("3. 批量更新合同容量")
            
            choice = input("请选择更新方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        old_status = input("原状态: ").strip()
                        new_status = input("新状态: ").strip()
                        cursor.execute("UPDATE customer_info SET status = %s WHERE status = %s", (new_status, old_status))
                        print(f"✅ 更新了 {cursor.rowcount} 个客户的状态")
                    elif choice == '2':
                        customer_type = input("客户类型: ").strip()
                        new_tariff = input("新电价类型: ").strip()
                        cursor.execute("UPDATE customer_info SET tariff_type = %s WHERE customer_type = %s", (new_tariff, customer_type))
                        print(f"✅ 更新了 {cursor.rowcount} 个客户的电价类型")
                    elif choice == '3':
                        multiplier = float(input("容量调整倍数: ") or "1.0")
                        cursor.execute("UPDATE customer_info SET contract_capacity = contract_capacity * %s", (multiplier,))
                        print(f"✅ 更新了 {cursor.rowcount} 个客户的合同容量")
                    else:
                        print("❌ 无效选择")
                        return
                    
                    conn.commit()
                        
        except Exception as e:
            print(f"❌ 更新失败: {e}")
    
    def batch_update_equipment(self):
        """批量更新设备信息"""
        try:
            print("\n✏️ 批量更新设备信息:")
            print("1. 批量更新设备状态")
            print("2. 批量更新设备容量")
            print("3. 按类型更新设备")
            
            choice = input("请选择更新方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        old_status = input("原状态: ").strip()
                        new_status = input("新状态: ").strip()
                        cursor.execute("UPDATE equipment_info SET status = %s WHERE status = %s", (new_status, old_status))
                        print(f"✅ 更新了 {cursor.rowcount} 个设备的状态")
                    elif choice == '2':
                        multiplier = float(input("容量调整倍数: ") or "1.0")
                        cursor.execute("UPDATE equipment_info SET capacity = capacity * %s", (multiplier,))
                        print(f"✅ 更新了 {cursor.rowcount} 个设备的容量")
                    elif choice == '3':
                        equipment_type = input("设备类型: ").strip()
                        new_status = input("新状态: ").strip()
                        cursor.execute("UPDATE equipment_info SET status = %s WHERE equipment_type = %s", (new_status, equipment_type))
                        print(f"✅ 更新了 {cursor.rowcount} 个{equipment_type}设备的状态")
                    else:
                        print("❌ 无效选择")
                        return
                    
                    conn.commit()
                        
        except Exception as e:
            print(f"❌ 更新失败: {e}")
    
    def update_alert_status(self):
        """更新告警状态"""
        try:
            print("\n✏️ 更新告警状态:")
            print("1. 单个告警状态更新")
            print("2. 批量关闭告警")
            print("3. 批量确认告警")
            
            choice = input("请选择更新方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        alert_id = input("告警ID: ").strip()
                        new_status = input("新状态 (OPEN/CLOSED/ACKNOWLEDGED): ").strip()
                        cursor.execute("UPDATE alert_records SET status = %s WHERE alert_id = %s", (new_status, alert_id))
                        if cursor.rowcount > 0:
                            print(f"✅ 告警 {alert_id} 状态更新为 {new_status}")
                        else:
                            print(f"❌ 未找到告警 {alert_id}")
                    elif choice == '2':
                        alert_level = input("告警级别 (INFO/WARNING/ERROR/CRITICAL): ").strip()
                        cursor.execute("UPDATE alert_records SET status = 'CLOSED' WHERE alert_level = %s AND status = 'OPEN'", (alert_level,))
                        print(f"✅ 批量关闭了 {cursor.rowcount} 个{alert_level}级别的告警")
                    elif choice == '3':
                        hours = int(input("确认多少小时前的告警: ") or "24")
                        cursor.execute("""
                            UPDATE alert_records SET status = 'ACKNOWLEDGED' 
                            WHERE status = 'OPEN' AND alert_time < CURRENT_TIMESTAMP - INTERVAL '%s hours'
                        """, (hours,))
                        print(f"✅ 批量确认了 {cursor.rowcount} 个告警")
                    else:
                        print("❌ 无效选择")
                        return
                    
                    conn.commit()
                        
        except Exception as e:
            print(f"❌ 更新失败: {e}")
    
    def batch_delete_customers(self):
        """批量删除客户"""
        try:
            print("\n🗑️ 批量删除客户:")
            print("⚠️ 警告: 删除客户将同时删除相关的电力数据和告警记录")
            
            print("1. 按状态删除客户")
            print("2. 按客户类型删除")
            print("3. 删除测试客户")
            
            choice = input("请选择删除方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        status = input("要删除的客户状态: ").strip()
                        confirm = input(f"确认删除所有状态为'{status}'的客户? (y/N): ").strip().lower()
                        if confirm == 'y':
                            # 先删除相关数据
                            cursor.execute("DELETE FROM power_consumption WHERE customer_id IN (SELECT customer_id FROM customer_info WHERE status = %s)", (status,))
                            power_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM alert_records WHERE customer_id IN (SELECT customer_id FROM customer_info WHERE status = %s)", (status,))
                            alert_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM customer_info WHERE status = %s", (status,))
                            customer_deleted = cursor.rowcount
                            print(f"✅ 删除完成: 客户 {customer_deleted} 个, 电力数据 {power_deleted} 条, 告警 {alert_deleted} 条")
                    elif choice == '2':
                        customer_type = input("要删除的客户类型: ").strip()
                        confirm = input(f"确认删除所有'{customer_type}'类型的客户? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("DELETE FROM power_consumption WHERE customer_id IN (SELECT customer_id FROM customer_info WHERE customer_type = %s)", (customer_type,))
                            power_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM alert_records WHERE customer_id IN (SELECT customer_id FROM customer_info WHERE customer_type = %s)", (customer_type,))
                            alert_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM customer_info WHERE customer_type = %s", (customer_type,))
                            customer_deleted = cursor.rowcount
                            print(f"✅ 删除完成: 客户 {customer_deleted} 个, 电力数据 {power_deleted} 条, 告警 {alert_deleted} 条")
                    elif choice == '3':
                        confirm = input("确认删除所有测试客户 (CUST_开头)? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("DELETE FROM power_consumption WHERE customer_id LIKE 'CUST_%'")
                            power_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM alert_records WHERE customer_id LIKE 'CUST_%'")
                            alert_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM customer_info WHERE customer_id LIKE 'CUST_%'")
                            customer_deleted = cursor.rowcount
                            print(f"✅ 删除完成: 测试客户 {customer_deleted} 个, 电力数据 {power_deleted} 条, 告警 {alert_deleted} 条")
                    else:
                        print("❌ 无效选择")
                        return
                    
                    conn.commit()
                        
        except Exception as e:
            print(f"❌ 删除失败: {e}")
    
    def batch_delete_equipment(self):
        """批量删除设备"""
        try:
            print("\n🗑️ 批量删除设备:")
            print("⚠️ 警告: 删除设备将同时删除相关的电力数据和告警记录")
            
            print("1. 按状态删除设备")
            print("2. 按设备类型删除")
            print("3. 删除测试设备")
            
            choice = input("请选择删除方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        status = input("要删除的设备状态: ").strip()
                        confirm = input(f"确认删除所有状态为'{status}'的设备? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("DELETE FROM power_consumption WHERE equipment_id IN (SELECT equipment_id FROM equipment_info WHERE status = %s)", (status,))
                            power_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM alert_records WHERE equipment_id IN (SELECT equipment_id FROM equipment_info WHERE status = %s)", (status,))
                            alert_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM equipment_status WHERE equipment_id IN (SELECT equipment_id FROM equipment_info WHERE status = %s)", (status,))
                            status_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM equipment_info WHERE status = %s", (status,))
                            equipment_deleted = cursor.rowcount
                            print(f"✅ 删除完成: 设备 {equipment_deleted} 个, 设备状态 {status_deleted} 条, 电力数据 {power_deleted} 条, 告警 {alert_deleted} 条")
                    elif choice == '2':
                        equipment_type = input("要删除的设备类型: ").strip()
                        confirm = input(f"确认删除所有'{equipment_type}'类型的设备? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("DELETE FROM power_consumption WHERE equipment_id IN (SELECT equipment_id FROM equipment_info WHERE equipment_type = %s)", (equipment_type,))
                            power_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM alert_records WHERE equipment_id IN (SELECT equipment_id FROM equipment_info WHERE equipment_type = %s)", (equipment_type,))
                            alert_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM equipment_status WHERE equipment_id IN (SELECT equipment_id FROM equipment_info WHERE equipment_type = %s)", (equipment_type,))
                            status_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM equipment_info WHERE equipment_type = %s", (equipment_type,))
                            equipment_deleted = cursor.rowcount
                            print(f"✅ 删除完成: 设备 {equipment_deleted} 个, 设备状态 {status_deleted} 条, 电力数据 {power_deleted} 条, 告警 {alert_deleted} 条")
                    elif choice == '3':
                        confirm = input("确认删除所有测试设备 (EQ_开头)? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("DELETE FROM power_consumption WHERE equipment_id LIKE 'EQ_%'")
                            power_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM alert_records WHERE equipment_id LIKE 'EQ_%'")
                            alert_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM equipment_status WHERE equipment_id LIKE 'EQ_%'")
                            status_deleted = cursor.rowcount
                            cursor.execute("DELETE FROM equipment_info WHERE equipment_id LIKE 'EQ_%'")
                            equipment_deleted = cursor.rowcount
                            print(f"✅ 删除完成: 测试设备 {equipment_deleted} 个, 设备状态 {status_deleted} 条, 电力数据 {power_deleted} 条, 告警 {alert_deleted} 条")
                    else:
                        print("❌ 无效选择")
                        return
                    
                    conn.commit()
                        
        except Exception as e:
            print(f"❌ 删除失败: {e}")
    
    def conditional_delete_data(self):
        """条件删除数据"""
        try:
            print("\n🗑️ 条件删除数据:")
            print("1. 删除指定时间范围的电力数据")
            print("2. 删除指定级别的告警")
            print("3. 删除异常功率数据")
            
            choice = input("请选择删除方式: ").strip()
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        hours = int(input("删除多少小时前的电力数据: ") or "24")
                        confirm = input(f"确认删除{hours}小时前的电力数据? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("""
                                DELETE FROM power_consumption 
                                WHERE record_time < CURRENT_TIMESTAMP - INTERVAL '%s hours'
                            """, (hours,))
                            print(f"✅ 删除了 {cursor.rowcount} 条电力数据")
                    elif choice == '2':
                        alert_level = input("要删除的告警级别: ").strip()
                        confirm = input(f"确认删除所有'{alert_level}'级别的告警? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("DELETE FROM alert_records WHERE alert_level = %s", (alert_level,))
                            print(f"✅ 删除了 {cursor.rowcount} 条告警记录")
                    elif choice == '3':
                        max_power = float(input("删除功率大于多少kW的数据: ") or "1000")
                        confirm = input(f"确认删除功率大于{max_power}kW的异常数据? (y/N): ").strip().lower()
                        if confirm == 'y':
                            cursor.execute("DELETE FROM power_consumption WHERE active_power > %s", (max_power,))
                            print(f"✅ 删除了 {cursor.rowcount} 条异常电力数据")
                    else:
                        print("❌ 无效选择")
                        return
                    
                    conn.commit()
                        
        except Exception as e:
            print(f"❌ 删除失败: {e}")
    
    def export_data(self):
        """数据导出"""
        try:
            print("\n📤 数据导出:")
            print("1. 导出客户信息")
            print("2. 导出设备信息")
            print("3. 导出电力数据")
            print("4. 导出告警记录")
            
            choice = input("请选择导出内容: ").strip()
            filename = input("导出文件名 (不含扩展名): ").strip() or "export_data"
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    if choice == '1':
                        cursor.execute("SELECT * FROM customer_info ORDER BY customer_id")
                        data = cursor.fetchall()
                        self._save_to_json(f"{filename}_customers.json", data, "客户信息")
                    elif choice == '2':
                        cursor.execute("SELECT * FROM equipment_info ORDER BY equipment_id")
                        data = cursor.fetchall()
                        self._save_to_json(f"{filename}_equipment.json", data, "设备信息")
                    elif choice == '3':
                        limit = int(input("导出最近多少条电力数据: ") or "1000")
                        cursor.execute("SELECT * FROM power_consumption ORDER BY record_time DESC LIMIT %s", (limit,))
                        data = cursor.fetchall()
                        self._save_to_json(f"{filename}_power.json", data, "电力数据")
                    elif choice == '4':
                        limit = int(input("导出最近多少条告警记录: ") or "1000")
                        cursor.execute("SELECT * FROM alert_records ORDER BY alert_time DESC LIMIT %s", (limit,))
                        data = cursor.fetchall()
                        self._save_to_json(f"{filename}_alerts.json", data, "告警记录")
                    else:
                        print("❌ 无效选择")
                        
        except Exception as e:
            print(f"❌ 导出失败: {e}")
    
    def _save_to_json(self, filename: str, data: List, data_type: str):
        """保存数据到JSON文件"""
        try:
            # 转换数据为可序列化格式
            json_data = []
            for row in data:
                json_row = []
                for item in row:
                    if isinstance(item, datetime.datetime):
                        json_row.append(item.isoformat())
                    else:
                        json_row.append(str(item) if item is not None else None)
                json_data.append(json_row)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'export_time': datetime.datetime.now().isoformat(),
                    'data_type': data_type,
                    'count': len(json_data),
                    'data': json_data
                }, f, ensure_ascii=False, indent=2)
            
            print(f"✅ {data_type}导出成功: {filename} ({len(json_data)} 条记录)")
            
        except Exception as e:
            print(f"❌ 保存文件失败: {e}")
    
    def import_data(self):
        """数据导入"""
        try:
            print("\n📥 数据导入:")
            filename = input("导入文件名: ").strip()
            
            if not filename:
                print("❌ 请输入文件名")
                return
            
            with open(filename, 'r', encoding='utf-8') as f:
                import_data = json.load(f)
            
            data_type = import_data.get('data_type', '未知')
            data = import_data.get('data', [])
            
            print(f"📊 准备导入 {data_type}: {len(data)} 条记录")
            confirm = input("确认导入? (y/N): ").strip().lower()
            
            if confirm != 'y':
                return
            
            # 这里可以根据数据类型进行相应的导入操作
            print(f"✅ 数据导入功能已准备就绪，需要根据具体数据格式实现导入逻辑")
            
        except FileNotFoundError:
            print(f"❌ 文件不存在: {filename}")
        except json.JSONDecodeError:
            print("❌ 文件格式错误，不是有效的JSON文件")
        except Exception as e:
            print(f"❌ 导入失败: {e}")
    
    def backup_data(self):
        """数据备份"""
        try:
            print("\n💾 数据备份:")
            backup_name = input("备份名称: ").strip() or f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    # 备份所有表的数据
                    tables = ['customer_info', 'equipment_info', 'power_consumption', 'alert_records']
                    backup_data = {
                        'backup_time': datetime.datetime.now().isoformat(),
                        'backup_name': backup_name,
                        'tables': {}
                    }
                    
                    for table in tables:
                        cursor.execute(f"SELECT * FROM {table}")
                        data = cursor.fetchall()
                        
                        # 转换为可序列化格式
                        json_data = []
                        for row in data:
                            json_row = []
                            for item in row:
                                if isinstance(item, datetime.datetime):
                                    json_row.append(item.isoformat())
                                else:
                                    json_row.append(str(item) if item is not None else None)
                            json_data.append(json_row)
                        
                        backup_data['tables'][table] = {
                            'count': len(json_data),
                            'data': json_data
                        }
                        print(f"   {table}: {len(json_data)} 条记录")
                    
                    filename = f"{backup_name}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(backup_data, f, ensure_ascii=False, indent=2)
                    
                    print(f"✅ 数据备份完成: {filename}")
                    
        except Exception as e:
            print(f"❌ 备份失败: {e}")
    
    def restore_data(self):
        """数据恢复"""
        try:
            print("\n🔄 数据恢复:")
            filename = input("备份文件名: ").strip()
            
            if not filename:
                print("❌ 请输入备份文件名")
                return
            
            with open(filename, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)
            
            backup_name = backup_data.get('backup_name', '未知')
            backup_time = backup_data.get('backup_time', '未知')
            tables = backup_data.get('tables', {})
            
            print(f"📊 备份信息:")
            print(f"   名称: {backup_name}")
            print(f"   时间: {backup_time}")
            print(f"   表数量: {len(tables)}")
            
            for table, info in tables.items():
                print(f"   {table}: {info['count']} 条记录")
            
            print("\n⚠️ 警告: 恢复操作将清空现有数据!")
            confirm = input("确认恢复? (y/N): ").strip().lower()
            
            if confirm != 'y':
                return
            
            print(f"✅ 数据恢复功能已准备就绪，需要根据具体需求实现恢复逻辑")
            
        except FileNotFoundError:
            print(f"❌ 备份文件不存在: {filename}")
        except json.JSONDecodeError:
            print("❌ 备份文件格式错误")
        except Exception as e:
             print(f"❌ 恢复失败: {e}")
    
    # ==================== 增强CRUD生成方法 ====================
    
    def _mixed_crud_generation(self):
        """混合CRUD操作生成"""
        try:
            print("\n🔄 混合CRUD操作生成:")
            print("💡 说明: 此模式会随机执行增删改查操作，模拟真实业务场景")
            
            duration_minutes = int(input("运行时长(分钟): ") or "10")
            operations_per_minute = int(input("每分钟操作数: ") or "5")
            
            # 操作权重配置
            print("\n⚖️ 操作权重配置 (总和应为100):")
            insert_weight = int(input("新增操作权重% [默认40]: ") or "40")
            update_weight = int(input("更新操作权重% [默认30]: ") or "30")
            delete_weight = int(input("删除操作权重% [默认20]: ") or "20")
            query_weight = int(input("查询操作权重% [默认10]: ") or "10")
            
            print(f"\n🎯 混合CRUD配置确认:")
            print(f"   运行时长: {duration_minutes} 分钟")
            print(f"   操作频率: {operations_per_minute} 次/分钟")
            print(f"   操作权重: 增{insert_weight}% 改{update_weight}% 删{delete_weight}% 查{query_weight}%")
            print(f"   预计总操作数: {duration_minutes * operations_per_minute}")
            
            confirm = input("\n确认开始混合CRUD生成? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            self._execute_mixed_crud(duration_minutes, operations_per_minute, 
                                   insert_weight, update_weight, delete_weight, query_weight)
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 生成失败: {e}")
    
    def _execute_mixed_crud(self, duration_minutes: int, operations_per_minute: int,
                           insert_weight: int, update_weight: int, delete_weight: int, query_weight: int):
        """执行混合CRUD操作"""
        try:
            # 创建操作权重列表
            operations = (['INSERT'] * insert_weight + 
                         ['UPDATE'] * update_weight + 
                         ['DELETE'] * delete_weight + 
                         ['QUERY'] * query_weight)
            
            total_operations = 0
            operation_stats = {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0, 'QUERY': 0}
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    start_time = time.time()
                    
                    for minute in range(duration_minutes):
                        minute_start = time.time()
                        
                        for _ in range(operations_per_minute):
                            operation = random.choice(operations)
                            
                            try:
                                if operation == 'INSERT':
                                    self._random_insert_operation(cursor)
                                elif operation == 'UPDATE':
                                    self._random_update_operation(cursor)
                                elif operation == 'DELETE':
                                    self._random_delete_operation(cursor)
                                elif operation == 'QUERY':
                                    self._random_query_operation(cursor)
                                
                                operation_stats[operation] += 1
                                total_operations += 1
                                
                            except Exception as e:
                                print(f"\n⚠️ 操作失败 ({operation}): {e}")
                        
                        conn.commit()
                        
                        # 控制执行速度
                        elapsed = time.time() - minute_start
                        if elapsed < 60:  # 每分钟的操作在60秒内完成
                            time.sleep(min(1, 60 - elapsed))
                        
                        print(f"\r进度: {minute+1}/{duration_minutes} 分钟, 总操作: {total_operations} 次", end="")
                    
                    print(f"\n\n✅ 混合CRUD操作完成!")
                    print(f"用时: {time.time() - start_time:.2f} 秒")
                    print(f"操作统计:")
                    for op, count in operation_stats.items():
                        percentage = (count / total_operations * 100) if total_operations > 0 else 0
                        print(f"   {op}: {count} 次 ({percentage:.1f}%)")
                    
        except Exception as e:
            print(f"\n❌ 混合CRUD执行失败: {e}")
    
    def _random_insert_operation(self, cursor):
        """随机插入操作"""
        operation_type = random.choice(['customer', 'equipment', 'power', 'alert'])
        
        if operation_type == 'customer':
            customer_id = f"CUST_{random.randint(1000, 9999)}"
            cursor.execute("""
                INSERT INTO customer_info 
                (customer_id, customer_name, customer_type, contact_person, contact_phone,
                 address, contract_capacity, voltage_level, tariff_type, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
                ON CONFLICT (customer_id) DO NOTHING
            """, (
                customer_id,
                f"随机客户_{random.randint(1, 1000)}",
                random.choice(['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL']),
                f"联系人_{random.randint(1, 100)}",
                f"138{random.randint(10000000, 99999999)}",
                f"随机地址_{random.randint(1, 100)}",
                random.uniform(50, 500),
                random.choice([220, 380, 10000]),
                "STANDARD"
            ))
        
        elif operation_type == 'equipment':
            equipment_id = f"EQ_{random.randint(1000, 9999)}"
            cursor.execute("""
                INSERT INTO equipment_info 
                (equipment_id, equipment_name, equipment_type, location, voltage_level,
                 capacity, manufacturer, install_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, 'NORMAL')
                ON CONFLICT (equipment_id) DO NOTHING
            """, (
                equipment_id,
                f"随机设备_{random.randint(1, 1000)}",
                random.choice(['变压器', '发电机', '电动机', '开关柜']),
                f"随机位置_{random.randint(1, 100)}",
                random.choice([220, 380, 10000]),
                random.uniform(50, 1000),
                random.choice(['ABB', 'Siemens', '施耐德', '华为'])
            ))
        
        elif operation_type == 'power':
            # 获取随机的客户和设备ID
            cursor.execute("SELECT customer_id FROM customer_info ORDER BY RANDOM() LIMIT 1")
            customer_result = cursor.fetchone()
            cursor.execute("SELECT equipment_id FROM equipment_info ORDER BY RANDOM() LIMIT 1")
            equipment_result = cursor.fetchone()
            
            if customer_result and equipment_result:
                cursor.execute("""
                    INSERT INTO power_consumption 
                    (customer_id, equipment_id, record_time, active_power, reactive_power,
                     voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                     power_factor, frequency, energy_consumption)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    customer_result[0], equipment_result[0],
                    random.uniform(10, 100), random.uniform(5, 50),
                    random.uniform(220, 240), random.uniform(220, 240), random.uniform(220, 240),
                    random.uniform(1, 50), random.uniform(1, 50), random.uniform(1, 50),
                    random.uniform(0.8, 0.95), 50.0, random.uniform(50, 500)
                ))
        
        elif operation_type == 'alert':
            # 获取随机的客户和设备ID
            cursor.execute("SELECT customer_id FROM customer_info ORDER BY RANDOM() LIMIT 1")
            customer_result = cursor.fetchone()
            cursor.execute("SELECT equipment_id FROM equipment_info ORDER BY RANDOM() LIMIT 1")
            equipment_result = cursor.fetchone()
            
            if customer_result and equipment_result:
                alert_type = random.choice(['VOLTAGE_SAG', 'VOLTAGE_SWELL', 'POWER_INTERRUPTION', 'OVERLOAD'])
                cursor.execute("""
                    INSERT INTO alert_records 
                    (equipment_id, customer_id, alert_type, alert_level, alert_title,
                     alert_description, alert_time, alert_value, threshold_value, status)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, 'OPEN')
                """, (
                    equipment_result[0], customer_result[0], alert_type,
                    random.choice(['INFO', 'WARNING', 'ERROR', 'CRITICAL']),
                    f"{alert_type}告警",
                    f"设备{equipment_result[0]}发生{alert_type}异常",
                    random.uniform(0, 1000), random.uniform(100, 500)
                ))
    
    def _random_update_operation(self, cursor):
        """随机更新操作"""
        operation_type = random.choice(['customer_status', 'equipment_status', 'alert_status'])
        
        if operation_type == 'customer_status':
            new_status = random.choice(['ACTIVE', 'INACTIVE'])
            cursor.execute("""
                UPDATE customer_info SET status = %s 
                WHERE customer_id IN (
                    SELECT customer_id FROM customer_info ORDER BY RANDOM() LIMIT 1
                )
            """, (new_status,))
        
        elif operation_type == 'equipment_status':
            new_status = random.choice(['NORMAL', 'FAULT', 'MAINTENANCE', 'OFFLINE'])
            cursor.execute("""
                UPDATE equipment_info SET status = %s 
                WHERE equipment_id IN (
                    SELECT equipment_id FROM equipment_info ORDER BY RANDOM() LIMIT 1
                )
            """, (new_status,))
        
        elif operation_type == 'alert_status':
            new_status = random.choice(['CLOSED', 'ACKNOWLEDGED'])
            cursor.execute("""
                UPDATE alert_records SET status = %s 
                WHERE alert_id IN (
                    SELECT alert_id FROM alert_records WHERE status = 'OPEN' ORDER BY RANDOM() LIMIT 1
                )
            """, (new_status,))
    
    def _random_delete_operation(self, cursor):
        """随机删除操作"""
        operation_type = random.choice(['old_power_data', 'closed_alerts', 'inactive_customers'])
        
        if operation_type == 'old_power_data':
            # 删除1小时前的部分电力数据 (PostgreSQL不支持DELETE LIMIT，使用子查询)
            cursor.execute("""
                DELETE FROM power_consumption 
                WHERE record_time IN (
                    SELECT record_time FROM power_consumption 
                    WHERE record_time < CURRENT_TIMESTAMP - INTERVAL '1 hour'
                    AND random() < 0.1
                    LIMIT 10
                )
            """)
        
        elif operation_type == 'closed_alerts':
            # 删除已关闭的告警
            cursor.execute("""
                DELETE FROM alert_records 
                WHERE alert_id IN (
                    SELECT alert_id FROM alert_records 
                    WHERE status = 'CLOSED' AND alert_time < CURRENT_TIMESTAMP - INTERVAL '1 hour'
                    LIMIT 5
                )
            """)
        
        elif operation_type == 'inactive_customers':
            # 删除非活跃客户（谨慎操作）
            cursor.execute("""
                DELETE FROM customer_info 
                WHERE customer_id IN (
                    SELECT customer_id FROM customer_info 
                    WHERE status = 'INACTIVE' AND customer_id LIKE 'CUST_%'
                    LIMIT 1
                )
            """)
    
    def _random_query_operation(self, cursor):
        """随机查询操作"""
        query_type = random.choice(['customer_count', 'equipment_status', 'power_stats', 'alert_summary'])
        
        if query_type == 'customer_count':
            cursor.execute("SELECT COUNT(*) FROM customer_info WHERE status = 'ACTIVE'")
        elif query_type == 'equipment_status':
            cursor.execute("SELECT status, COUNT(*) FROM equipment_info GROUP BY status")
        elif query_type == 'power_stats':
            cursor.execute("SELECT AVG(active_power), MAX(active_power) FROM power_consumption WHERE record_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour'")
        elif query_type == 'alert_summary':
            cursor.execute("SELECT alert_level, COUNT(*) FROM alert_records WHERE status = 'OPEN' GROUP BY alert_level")
        
        # 获取查询结果（但不显示，只是执行查询）
        cursor.fetchall()
    
    def _intelligent_data_evolution(self):
        """智能数据演化模拟"""
        try:
            print("\n🧠 智能数据演化模拟:")
            print("💡 说明: 模拟数据的生命周期，包括创建、成长、老化、清理等阶段")
            
            evolution_cycles = int(input("演化周期数: ") or "5")
            cycle_duration_minutes = int(input("每周期时长(分钟): ") or "2")
            
            print(f"\n🔄 演化配置确认:")
            print(f"   演化周期: {evolution_cycles} 个")
            print(f"   周期时长: {cycle_duration_minutes} 分钟")
            print(f"   总时长: {evolution_cycles * cycle_duration_minutes} 分钟")
            
            confirm = input("\n确认开始智能演化? (y/N): ").strip().lower()
            if confirm != 'y':
                return
            
            for cycle in range(evolution_cycles):
                print(f"\n🌱 第 {cycle + 1} 个演化周期开始...")
                
                # 每个周期的不同阶段
                if cycle == 0:
                    print("   阶段: 初始化数据")
                    self._evolution_phase_initialize(cycle_duration_minutes)
                elif cycle < evolution_cycles // 2:
                    print("   阶段: 数据成长期")
                    self._evolution_phase_growth(cycle_duration_minutes)
                else:
                    print("   阶段: 数据成熟期")
                    self._evolution_phase_mature(cycle_duration_minutes)
                
                print(f"   第 {cycle + 1} 个周期完成")
            
            print("\n🎉 智能数据演化模拟完成!")
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 演化模拟失败: {e}")
    
    def _evolution_phase_initialize(self, duration_minutes: int):
        """演化阶段：初始化"""
        # 主要进行数据创建
        self._execute_mixed_crud(duration_minutes, 8, 70, 20, 5, 5)
    
    def _evolution_phase_growth(self, duration_minutes: int):
        """演化阶段：成长期"""
        # 平衡的CRUD操作
        self._execute_mixed_crud(duration_minutes, 6, 40, 35, 15, 10)
    
    def _evolution_phase_mature(self, duration_minutes: int):
        """演化阶段：成熟期"""
        # 更多的更新和查询，较少的新增
        self._execute_mixed_crud(duration_minutes, 5, 20, 45, 25, 10)
    
    def _business_scenario_simulation(self):
        """业务场景模拟"""
        try:
            print("\n🏢 业务场景模拟:")
            print("1. 正常工作日场景")
            print("2. 高峰用电场景")
            print("3. 设备故障场景")
            print("4. 系统维护场景")
            print("5. 紧急响应场景")
            
            scenario = input("请选择业务场景: ").strip()
            duration_minutes = int(input("场景持续时间(分钟): ") or "10")
            
            if scenario == '1':
                self._simulate_normal_workday(duration_minutes)
            elif scenario == '2':
                self._simulate_peak_usage(duration_minutes)
            elif scenario == '3':
                self._simulate_equipment_failure(duration_minutes)
            elif scenario == '4':
                self._simulate_system_maintenance(duration_minutes)
            elif scenario == '5':
                self._simulate_emergency_response(duration_minutes)
            else:
                print("❌ 无效的场景选择")
                
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 场景模拟失败: {e}")
    
    def _simulate_normal_workday(self, duration_minutes: int):
        """模拟正常工作日"""
        print(f"\n📅 模拟正常工作日场景 ({duration_minutes}分钟)")
        print("   特点: 稳定的数据流，少量告警，定期状态更新")
        # 正常工作日：主要是数据插入和少量更新
        self._execute_mixed_crud(duration_minutes, 10, 60, 25, 5, 10)
    
    def _simulate_peak_usage(self, duration_minutes: int):
        """模拟高峰用电场景"""
        print(f"\n⚡ 模拟高峰用电场景 ({duration_minutes}分钟)")
        print("   特点: 大量电力数据，频繁状态更新，增加告警")
        # 高峰期：更多的数据插入和更新
        self._execute_mixed_crud(duration_minutes, 15, 70, 20, 5, 5)
    
    def _simulate_equipment_failure(self, duration_minutes: int):
        """模拟设备故障场景"""
        print(f"\n🔧 模拟设备故障场景 ({duration_minutes}分钟)")
        print("   特点: 大量告警生成，设备状态频繁更新，故障数据清理")
        # 故障场景：更多告警和状态更新
        self._execute_mixed_crud(duration_minutes, 12, 45, 35, 10, 10)
    
    def _simulate_system_maintenance(self, duration_minutes: int):
        """模拟系统维护场景"""
        print(f"\n🛠️ 模拟系统维护场景 ({duration_minutes}分钟)")
        print("   特点: 数据清理，批量更新，系统优化")
        # 维护场景：更多删除和更新操作
        self._execute_mixed_crud(duration_minutes, 8, 20, 40, 30, 10)
    
    def _simulate_emergency_response(self, duration_minutes: int):
        """模拟紧急响应场景"""
        print(f"\n🚨 模拟紧急响应场景 ({duration_minutes}分钟)")
        print("   特点: 频繁查询，快速状态更新，紧急数据处理")
        # 紧急响应：更多查询和更新
        self._execute_mixed_crud(duration_minutes, 20, 30, 40, 10, 20)

def main():
    manager = DataManager()
    manager.run()

if __name__ == "__main__":
    main()