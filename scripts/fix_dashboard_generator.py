#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大屏数据生成器修复脚本

修复问题:
1. 修复ads_realtime_dashboard表的主键冲突问题
2. 确保数据生成遵循Fluss架构（Source端生成，传播到Sink端）
3. 修复ON CONFLICT语句的错误
4. 优化数据生成逻辑
"""

import sys
import os
import re

def fix_dashboard_generator():
    """修复interactive_data_manager.py中的大屏数据生成功能"""
    
    script_path = '/Users/felix/cloud_enviroment/realtime/fluss/trae-fluss-base/scripts/interactive_data_manager.py'
    
    print("🔧 开始修复大屏数据生成功能...")
    
    try:
        # 读取原文件
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print("📖 已读取原文件")
        
        # 修复1: 修复ads_realtime_dashboard表的INSERT语句
        # 移除metric_id字段，让数据库自动生成
        old_insert_pattern = r'INSERT INTO ads_realtime_dashboard\s*\(metric_name, metric_value, metric_unit, metric_desc, metric_category, update_time\)\s*VALUES \(%s, %s, %s, %s, %s, %s\)\s*ON CONFLICT \(metric_id\) DO UPDATE SET'
        
        new_insert_statement = '''INSERT INTO ads_realtime_dashboard 
                            (metric_name, metric_value, metric_unit, metric_desc, metric_category, update_time)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (metric_name) DO UPDATE SET'''
        
        # 查找并替换错误的INSERT语句
        content = re.sub(
            r'INSERT INTO ads_realtime_dashboard\s*\([^)]+\)\s*VALUES \([^)]+\)\s*ON CONFLICT \(metric_id\) DO UPDATE SET',
            new_insert_statement,
            content,
            flags=re.MULTILINE | re.DOTALL
        )
        
        print("✅ 已修复ads_realtime_dashboard表的INSERT语句")
        
        # 修复2: 添加新的Source端数据生成方法
        new_methods = '''
    def _generate_basic_source_data(self):
        """在Source端生成基础数据，遵循Fluss架构"""
        print("\n🚀 在Source端生成基础数据...")
        print("此操作将在Source端生成数据，通过Fluss架构传播到Sink端ADS表")
        
        try:
            # 生成基础客户和设备数据
            customer_count = 10
            equipment_count = 15
            
            with self.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    print(f"\n👥 创建 {customer_count} 个测试客户...")
                    
                    # 清理并创建客户数据
                    cursor.execute("DELETE FROM customer_info WHERE customer_id LIKE 'DASH_%'")
                    
                    customer_types = ['INDUSTRIAL', 'COMMERCIAL', 'RESIDENTIAL']
                    for i in range(customer_count):
                        customer_id = f"DASH_{i+1:03d}"
                        customer_name = f"大屏测试客户{i+1}"
                        customer_type = customer_types[i % len(customer_types)]
                        
                        cursor.execute("""
                            INSERT INTO customer_info (customer_id, customer_name, customer_type, 
                                                     contact_person, phone, address, registration_date)
                            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE)
                            ON CONFLICT (customer_id) DO UPDATE SET
                            customer_name = EXCLUDED.customer_name,
                            customer_type = EXCLUDED.customer_type
                        """, (customer_id, customer_name, customer_type, 
                              f"联系人{i+1}", f"138{i+1:04d}0000", f"测试地址{i+1}号"))
                    
                    print(f"\n🔧 创建 {equipment_count} 个测试设备...")
                    
                    # 清理并创建设备数据
                    cursor.execute("DELETE FROM equipment_info WHERE equipment_id LIKE 'DASH_%'")
                    
                    equipment_types = ['TRANSFORMER', 'SWITCHGEAR', 'BREAKER', 'CABLE', 'SWITCH']
                    for i in range(equipment_count):
                        equipment_id = f"DASH_{i+1:03d}"
                        equipment_name = f"大屏测试设备{i+1}"
                        equipment_type = equipment_types[i % len(equipment_types)]
                        
                        cursor.execute("""
                            INSERT INTO equipment_info (equipment_id, equipment_name, equipment_type,
                                                       location, installation_date, status)
                            VALUES (%s, %s, %s, %s, CURRENT_DATE, 'ACTIVE')
                            ON CONFLICT (equipment_id) DO UPDATE SET
                            equipment_name = EXCLUDED.equipment_name,
                            equipment_type = EXCLUDED.equipment_type
                        """, (equipment_id, equipment_name, equipment_type, f"测试位置{i+1}"))
                    
                    print("\n⚡ 生成电力消耗数据...")
                    
                    # 生成电力消耗数据
                    import random
                    import datetime
                    
                    current_time = datetime.datetime.now()
                    
                    for i in range(100):  # 生成100条电力数据
                        customer_id = f"DASH_{random.randint(1, customer_count):03d}"
                        equipment_id = f"DASH_{random.randint(1, equipment_count):03d}"
                        
                        # 随机生成过去24小时内的时间
                        record_time = current_time - datetime.timedelta(
                            hours=random.randint(0, 24),
                            minutes=random.randint(0, 59)
                        )
                        
                        cursor.execute("""
                            INSERT INTO power_consumption 
                            (customer_id, equipment_id, record_time, active_power, reactive_power,
                             voltage, current_value, power_factor, frequency)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            customer_id, equipment_id, record_time,
                            random.uniform(50, 200),    # active_power
                            random.uniform(10, 50),     # reactive_power
                            random.uniform(220, 240),   # voltage
                            random.uniform(10, 100),    # current_value
                            random.uniform(0.8, 0.95),  # power_factor
                            random.uniform(49.8, 50.2)  # frequency
                        ))
                    
                    print("\n🚨 生成告警数据...")
                    
                    # 生成告警数据
                    alert_types = ['POWER_OUTAGE', 'VOLTAGE_ABNORMAL', 'EQUIPMENT_FAULT', 'OVERLOAD']
                    alert_levels = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
                    
                    for i in range(20):  # 生成20条告警数据
                        equipment_id = f"DASH_{random.randint(1, equipment_count):03d}"
                        
                        alert_time = current_time - datetime.timedelta(
                            hours=random.randint(0, 48),
                            minutes=random.randint(0, 59)
                        )
                        
                        cursor.execute("""
                            INSERT INTO alert_records
                            (equipment_id, alert_type, alert_level, alert_message, alert_time, status)
                            VALUES (%s, %s, %s, %s, %s, 'ACTIVE')
                        """, (
                            equipment_id,
                            random.choice(alert_types),
                            random.choice(alert_levels),
                            f"大屏测试告警{i+1}",
                            alert_time
                        ))
                    
                    conn.commit()
                    
                    print("\n✅ Source端数据生成完成!")
                    print(f"   - 客户数据: {customer_count} 条")
                    print(f"   - 设备数据: {equipment_count} 条")
                    print(f"   - 电力数据: 100 条")
                    print(f"   - 告警数据: 20 条")
                    
                    print("\n⏳ 等待数据传播到Sink端ADS表...")
                    print("💡 提示: 数据传播需要一些时间，请稍后查看Grafana大屏")
                    
        except Exception as e:
            print(f"❌ Source端数据生成失败: {e}")
    
    def _generate_batch_historical_data(self):
        """生成批量历史数据"""
        print("\n📊 生成批量历史数据...")
        
        try:
            days = int(input("请输入要生成的历史天数 (1-7，推荐3): ").strip() or "3")
            if days < 1 or days > 7:
                print("❌ 天数必须在1-7之间")
                return
            
            records_per_day = int(input("每天生成记录数 (50-500，推荐200): ").strip() or "200")
            if records_per_day < 50 or records_per_day > 500:
                print("❌ 每天记录数必须在50-500之间")
                return
            
            print(f"\n🚀 开始生成过去 {days} 天的历史数据，每天 {records_per_day} 条记录...")
            
            # 调用现有的批量生成方法
            duration_minutes = days * 24 * 60  # 转换为分钟
            records_per_minute = records_per_day / (24 * 60)  # 每分钟记录数
            
            self._generate_test_data(
                duration_minutes=duration_minutes,
                records_per_minute=int(records_per_minute) or 1,
                customer_count=10,
                equipment_count=15,
                alert_probability=0.05
            )
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except Exception as e:
            print(f"❌ 批量历史数据生成失败: {e}")
    
    def _generate_realtime_source_data(self):
        """生成实时Source端数据"""
        print("\n🔄 启动实时数据生成...")
        print("此功能将持续在Source端生成数据，按Ctrl+C停止")
        
        try:
            interval = int(input("生成间隔(秒，推荐30): ").strip() or "30")
            if interval < 10:
                print("❌ 间隔不能小于10秒")
                return
            
            records_per_batch = int(input("每批记录数 (1-10，推荐3): ").strip() or "3")
            if records_per_batch < 1 or records_per_batch > 10:
                print("❌ 每批记录数必须在1-10之间")
                return
            
            print(f"\n⏰ 开始每 {interval} 秒生成 {records_per_batch} 条记录...")
            
            # 调用现有的实时生成方法
            self._start_realtime_generation(
                interval_seconds=interval,
                records_per_batch=records_per_batch,
                customer_count=10,
                equipment_count=15
            )
            
        except ValueError:
            print("❌ 请输入有效的数字")
        except KeyboardInterrupt:
            print("\n\n⏹️ 实时数据生成已停止")
        except Exception as e:
            print(f"❌ 实时数据生成失败: {e}")
    
    def _generate_scenario_data(self):
        """生成场景化数据"""
        print("\n🎭 场景化数据生成")
        print("选择要模拟的业务场景:")
        print("1. 正常运行场景 - 系统稳定运行")
        print("2. 高负载场景 - 用电高峰期")
        print("3. 故障场景 - 设备故障和告警")
        print("4. 维护场景 - 设备维护期间")
        print("0. 返回")
        
        choice = input("\n请选择场景: ").strip()
        
        if choice == '1':
            self._generate_normal_scenario()
        elif choice == '2':
            self._generate_high_load_scenario()
        elif choice == '3':
            self._generate_fault_scenario()
        elif choice == '4':
            self._generate_maintenance_scenario()
        elif choice == '0':
            return
        else:
            print("❌ 无效选择")
    
    def _check_data_propagation(self):
        """检查数据传播状态"""
        print("\n🔍 检查数据传播状态...")
        
        try:
            # 检查Source端数据
            source_stats = self._get_detailed_source_stats()
            print("\nSource端数据统计:")
            for table, count in source_stats.items():
                print(f"   📊 {table}: {count} 条记录")
            
            # 检查Sink端数据
            sink_stats = self._get_detailed_sink_stats()
            print("\nSink端ADS表统计:")
            for table, count in sink_stats.items():
                print(f"   📊 {table}: {count} 条记录")
            
            # 分析传播状态
            source_total = sum(source_stats.values())
            sink_total = sum(sink_stats.values())
            
            print(f"\n📈 数据传播分析:")
            print(f"   Source端总计: {source_total} 条")
            print(f"   Sink端总计: {sink_total} 条")
            
            if source_total == 0:
                print("   ⚠️  Source端无数据，请先生成基础数据")
            elif sink_total == 0:
                print("   ⚠️  Sink端无数据，可能CDC未正常工作或数据还在传播中")
                print("   💡 建议: 等待几分钟后再次检查，或检查Flink作业状态")
            else:
                ratio = sink_total / source_total * 100
                print(f"   📊 传播比例: {ratio:.1f}%")
                
                if ratio > 80:
                    print("   ✅ 数据传播正常")
                elif ratio > 50:
                    print("   🟡 数据传播部分完成")
                else:
                    print("   🔴 数据传播可能存在问题")
            
            # 检查数据新鲜度
            freshness = self._check_data_freshness()
            if freshness:
                print("\n🕒 数据新鲜度:")
                for table, info in freshness.items():
                    status = "🟢 新鲜" if info['fresh'] else "🔴 过期"
                    print(f"   {table}: {info['latest']} ({status})")
            
        except Exception as e:
            print(f"❌ 检查数据传播失败: {e}")
'''
        
        # 查找dashboard_data_generator方法的结束位置
        dashboard_method_end = content.find('def _dashboard_data_generator_old(self):')
        
        if dashboard_method_end != -1:
            # 在_dashboard_data_generator_old方法之前插入新方法
            content = content[:dashboard_method_end] + new_methods + '\n    ' + content[dashboard_method_end:]
            print("✅ 已添加新的Source端数据生成方法")
        else:
            print("⚠️  未找到_dashboard_data_generator_old方法，将在文件末尾添加新方法")
            content += new_methods
        
        # 创建备份
        backup_path = script_path + '.backup'
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"💾 已创建备份文件: {backup_path}")
        
        # 写入修复后的文件
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("✅ 大屏数据生成功能修复完成!")
        print("\n🎯 修复内容:")
        print("   1. ✅ 修复了ads_realtime_dashboard表的主键冲突问题")
        print("   2. ✅ 添加了遵循Fluss架构的Source端数据生成方法")
        print("   3. ✅ 修复了ON CONFLICT语句的错误")
        print("   4. ✅ 优化了数据生成逻辑")
        print("\n💡 使用建议:")
        print("   1. 重新运行interactive_data_manager.py")
        print("   2. 选择'8. 📊 大屏数据生成'")
        print("   3. 选择'1. 基础数据生成'开始生成数据")
        print("   4. 等待数据传播到ADS表后查看Grafana大屏")
        
    except Exception as e:
        print(f"❌ 修复过程出错: {e}")
        return False
    
    return True

if __name__ == '__main__':
    print("🔧 大屏数据生成器修复工具")
    print("=" * 50)
    
    if fix_dashboard_generator():
        print("\n🎉 修复成功完成!")
    else:
        print("\n❌ 修复失败，请检查错误信息")
        sys.exit(1)