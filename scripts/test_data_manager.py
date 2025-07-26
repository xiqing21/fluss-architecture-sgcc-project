#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据管理脚本自动化测试
测试 interactive_data_manager.py 的各项功能
"""

import sys
import os
import psycopg2
import time
from datetime import datetime

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入数据管理器
from interactive_data_manager import DataManager

class DataManagerTester:
    def __init__(self):
        self.data_manager = DataManager()
        self.test_results = []
        
    def log_test(self, test_name: str, success: bool, message: str = ""):
        """记录测试结果"""
        status = "✅ PASS" if success else "❌ FAIL"
        result = f"{status} {test_name}"
        if message:
            result += f": {message}"
        print(result)
        self.test_results.append({
            'test': test_name,
            'success': success,
            'message': message,
            'timestamp': datetime.now().isoformat()
        })
    
    def test_database_connections(self):
        """测试数据库连接"""
        print("\n🔍 测试数据库连接...")
        
        # 测试 Source 数据库连接
        try:
            with self.data_manager.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    self.log_test("Source数据库连接", result[0] == 1)
        except Exception as e:
            self.log_test("Source数据库连接", False, str(e))
        
        # 测试 Sink 数据库连接
        try:
            with self.data_manager.get_sink_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    self.log_test("Sink数据库连接", result[0] == 1)
        except Exception as e:
            self.log_test("Sink数据库连接", False, str(e))
    
    def test_source_table_structure(self):
        """测试 Source 端表结构"""
        print("\n🏗️ 测试 Source 端表结构...")
        
        required_tables = [
            'customer_info',
            'equipment_info', 
            'power_consumption',
            'alert_records'
        ]
        
        try:
            with self.data_manager.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    for table in required_tables:
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = '{table}'
                            )
                        """)
                        exists = cursor.fetchone()[0]
                        self.log_test(f"Source表 {table}", exists, "存在" if exists else "不存在")
        except Exception as e:
            self.log_test("Source表结构检查", False, str(e))
    
    def test_sink_table_structure(self):
        """测试 Sink 端表结构"""
        print("\n🏗️ 测试 Sink 端表结构...")
        
        required_tables = [
            'ads_power_quality',
            'ads_equipment_health',
            'ads_alert_statistics', 
            'ads_energy_efficiency'
        ]
        
        try:
            with self.data_manager.get_sink_connection() as conn:
                with conn.cursor() as cursor:
                    for table in required_tables:
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = '{table}'
                            )
                        """)
                        exists = cursor.fetchone()[0]
                        self.log_test(f"Sink表 {table}", exists, "存在" if exists else "不存在")
        except Exception as e:
            self.log_test("Sink表结构检查", False, str(e))
    
    def test_data_statistics(self):
        """测试数据统计功能"""
        print("\n📊 测试数据统计功能...")
        
        try:
            with self.data_manager.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    # 测试客户信息统计
                    cursor.execute("SELECT COUNT(*) FROM customer_info")
                    customer_count = cursor.fetchone()[0]
                    self.log_test("客户信息统计", True, f"{customer_count} 条记录")
                    
                    # 测试设备信息统计
                    cursor.execute("SELECT COUNT(*) FROM equipment_info")
                    equipment_count = cursor.fetchone()[0]
                    self.log_test("设备信息统计", True, f"{equipment_count} 条记录")
                    
                    # 测试电力消耗统计
                    cursor.execute("SELECT COUNT(*) FROM power_consumption")
                    power_count = cursor.fetchone()[0]
                    self.log_test("电力消耗统计", True, f"{power_count} 条记录")
                    
                    # 测试告警记录统计
                    cursor.execute("SELECT COUNT(*) FROM alert_records")
                    alert_count = cursor.fetchone()[0]
                    self.log_test("告警记录统计", True, f"{alert_count} 条记录")
                    
        except Exception as e:
            self.log_test("数据统计功能", False, str(e))
    
    def test_data_insertion(self):
        """测试数据插入功能"""
        print("\n📝 测试数据插入功能...")
        
        try:
            with self.data_manager.get_source_connection() as conn:
                with conn.cursor() as cursor:
                    # 测试插入客户信息
                    test_customer_id = f"TEST_CUST_{int(time.time())}"
                    cursor.execute("""
                        INSERT INTO customer_info 
                        (customer_id, customer_name, customer_type, contact_person, contact_phone,
                         address, contract_capacity, voltage_level, tariff_type, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
                    """, (
                        test_customer_id, "测试客户", "COMMERCIAL", "测试联系人", "13800000000",
                        "测试地址", 100.0, 380, "STANDARD"
                    ))
                    
                    # 测试插入设备信息
                    test_equipment_id = f"TEST_EQ_{int(time.time())}"
                    cursor.execute("""
                        INSERT INTO equipment_info 
                        (equipment_id, equipment_name, equipment_type, location, voltage_level,
                         capacity, manufacturer, install_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, 'NORMAL')
                    """, (
                        test_equipment_id, "测试设备", "变压器", "测试位置", 380, 100.0, "测试厂商"
                    ))
                    
                    # 测试插入电力消耗数据
                    cursor.execute("""
                        INSERT INTO power_consumption 
                        (customer_id, equipment_id, record_time, active_power, reactive_power,
                         voltage_a, voltage_b, voltage_c, current_a, current_b, current_c,
                         power_factor, frequency, energy_consumption)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        test_customer_id, test_equipment_id, 50.0, 25.0, 230.0, 230.0, 230.0,
                        10.0, 10.0, 10.0, 0.9, 50.0, 100.0
                    ))
                    
                    # 测试插入告警记录
                    cursor.execute("""
                        INSERT INTO alert_records 
                        (equipment_id, customer_id, alert_type, alert_level, alert_title,
                         alert_description, alert_time, alert_value, threshold_value, status)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, 'OPEN')
                    """, (
                        test_equipment_id, test_customer_id, "VOLTAGE_SAG", "WARNING", "测试告警",
                        "测试告警描述", 220.0, 230.0
                    ))
                    
                    conn.commit()
                    self.log_test("数据插入功能", True, "所有测试数据插入成功")
                    
        except Exception as e:
            self.log_test("数据插入功能", False, str(e))
    
    def test_data_generation_logic(self):
        """测试数据生成逻辑（不实际执行）"""
        print("\n🤖 测试数据生成逻辑...")
        
        try:
            # 检查数据生成方法是否存在
            has_auto_generate = hasattr(self.data_manager, 'auto_generate_data')
            self.log_test("自动生成数据方法", has_auto_generate, "方法存在" if has_auto_generate else "方法不存在")
            
            has_generate_logic = hasattr(self.data_manager, '_generate_test_data')
            self.log_test("数据生成核心逻辑", has_generate_logic, "方法存在" if has_generate_logic else "方法不存在")
            
        except Exception as e:
            self.log_test("数据生成逻辑检查", False, str(e))
    
    def test_monitoring_functionality(self):
        """测试监控功能"""
        print("\n📡 测试监控功能...")
        
        try:
            # 检查监控相关方法
            has_sink_monitoring = hasattr(self.data_manager, 'monitor_sink_data')
            self.log_test("Sink端监控方法", has_sink_monitoring, "方法存在" if has_sink_monitoring else "方法不存在")
            
            has_latency_analysis = hasattr(self.data_manager, 'analyze_data_flow')
            self.log_test("延迟分析方法", has_latency_analysis, "方法存在" if has_latency_analysis else "方法不存在")
            
            has_real_time_monitoring = hasattr(self.data_manager, 'start_real_time_monitoring')
            self.log_test("实时监控方法", has_real_time_monitoring, "方法存在" if has_real_time_monitoring else "方法不存在")
            
            has_table_structure = hasattr(self.data_manager, 'show_table_structures')
            self.log_test("表结构查看方法", has_table_structure, "方法存在" if has_table_structure else "方法不存在")
            
            has_cleanup = hasattr(self.data_manager, 'cleanup_test_data')
            self.log_test("数据清理方法", has_cleanup, "方法存在" if has_cleanup else "方法不存在")
            
        except Exception as e:
            self.log_test("监控功能检查", False, str(e))
    
    def generate_test_report(self):
        """生成测试报告"""
        print("\n" + "="*60)
        print("📋 数据管理脚本测试报告")
        print("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['success'])
        failed_tests = total_tests - passed_tests
        
        print(f"总测试数: {total_tests}")
        print(f"通过: {passed_tests}")
        print(f"失败: {failed_tests}")
        print(f"成功率: {(passed_tests/total_tests*100):.1f}%")
        
        if failed_tests > 0:
            print("\n❌ 失败的测试:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  - {result['test']}: {result['message']}")
        
        print("\n" + "="*60)
        
        return passed_tests == total_tests
    
    def run_all_tests(self):
        """运行所有测试"""
        print("🚀 开始测试数据管理脚本...")
        
        # 运行各项测试
        self.test_database_connections()
        self.test_source_table_structure()
        self.test_sink_table_structure()
        self.test_data_statistics()
        self.test_data_insertion()
        self.test_data_generation_logic()
        self.test_monitoring_functionality()
        
        # 生成报告
        success = self.generate_test_report()
        
        return success

def main():
    """主函数"""
    tester = DataManagerTester()
    
    try:
        success = tester.run_all_tests()
        
        if success:
            print("\n🎉 所有测试通过！数据管理脚本功能正常")
            return 0
        else:
            print("\n⚠️ 部分测试失败，请检查相关功能")
            return 1
            
    except KeyboardInterrupt:
        print("\n⏹️ 测试被用户中断")
        return 1
    except Exception as e:
        print(f"\n💥 测试过程中发生错误: {e}")
        return 1

if __name__ == "__main__":
    exit(main())