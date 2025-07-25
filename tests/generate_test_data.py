#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网风控数仓 - 大数据量CRUD测试脚本
用于生成大量测试数据并测试数据传输和计算延迟
"""

import psycopg2
import random
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json
from typing import List, Dict, Any

class SGCCDataGenerator:
    def __init__(self, source_config: Dict[str, Any], sink_config: Dict[str, Any]):
        self.source_config = source_config
        self.sink_config = sink_config
        self.lock = threading.Lock()
        self.stats = {
            'insert_count': 0,
            'update_count': 0,
            'delete_count': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
        
    def get_connection(self, config: Dict[str, Any]):
        """获取数据库连接"""
        return psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
    
    def generate_equipment_data(self, count: int) -> List[Dict[str, Any]]:
        """生成设备信息数据"""
        equipment_types = ['变压器', '开关柜', '电缆', '母线', '断路器', '隔离开关']
        voltage_levels = ['10kV', '35kV', '110kV', '220kV', '500kV']
        locations = ['北京', '上海', '广州', '深圳', '杭州', '南京', '武汉', '成都']
        
        data = []
        for i in range(count):
            equipment_data = {
                'equipment_id': f'EQ{str(i+1).zfill(8)}',
                'equipment_name': f'{random.choice(equipment_types)}{i+1:04d}',
                'equipment_type': random.choice(equipment_types),
                'voltage_level': random.choice(voltage_levels),
                'location': random.choice(locations),
                'installation_date': (datetime.now() - timedelta(days=random.randint(30, 3650))).date(),
                'manufacturer': f'厂商{random.randint(1, 20)}',
                'model': f'型号{random.randint(100, 999)}',
                'rated_capacity': round(random.uniform(100, 10000), 2),
                'status': random.choice(['运行', '检修', '停运']),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            data.append(equipment_data)
        return data
    
    def generate_customer_data(self, count: int) -> List[Dict[str, Any]]:
        """生成客户信息数据"""
        customer_types = ['居民', '工业', '商业', '农业']
        regions = ['华北', '华东', '华南', '华中', '西北', '东北', '西南']
        
        data = []
        for i in range(count):
            customer_data = {
                'customer_id': f'CU{str(i+1).zfill(8)}',
                'customer_name': f'客户{i+1:06d}',
                'customer_type': random.choice(customer_types),
                'region': random.choice(regions),
                'contract_capacity': round(random.uniform(50, 5000), 2),
                'voltage_level': random.choice(['380V', '10kV', '35kV', '110kV']),
                'contact_person': f'联系人{i+1}',
                'phone': f'1{random.randint(3,9)}{random.randint(0,9):08d}',
                'address': f'{random.choice(regions)}地区地址{i+1}',
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            data.append(customer_data)
        return data
    
    def generate_power_consumption_data(self, count: int, customer_ids: List[str]) -> List[Dict[str, Any]]:
        """生成电力消耗数据"""
        data = []
        base_time = datetime.now() - timedelta(hours=1)
        
        for i in range(count):
            consumption_data = {
                'customer_id': random.choice(customer_ids),
                'record_time': base_time + timedelta(seconds=random.randint(0, 3600)),
                'active_power': round(random.uniform(10, 1000), 2),
                'reactive_power': round(random.uniform(5, 500), 2),
                'voltage': round(random.uniform(220, 240), 1),
                'current': round(random.uniform(10, 100), 2),
                'power_factor': round(random.uniform(0.8, 1.0), 3),
                'frequency': round(random.uniform(49.8, 50.2), 2),
                'created_at': datetime.now()
            }
            data.append(consumption_data)
        return data
    
    def generate_equipment_status_data(self, count: int, equipment_ids: List[str]) -> List[Dict[str, Any]]:
        """生成设备状态数据"""
        data = []
        base_time = datetime.now() - timedelta(hours=1)
        
        for i in range(count):
            status_data = {
                'equipment_id': random.choice(equipment_ids),
                'status_time': base_time + timedelta(seconds=random.randint(0, 3600)),
                'temperature': round(random.uniform(20, 80), 1),
                'humidity': round(random.uniform(30, 90), 1),
                'vibration': round(random.uniform(0, 10), 2),
                'load_rate': round(random.uniform(0.1, 0.95), 3),
                'health_score': random.randint(60, 100),
                'status': random.choice(['正常', '预警', '告警', '故障']),
                'created_at': datetime.now()
            }
            data.append(status_data)
        return data
    
    def generate_alert_data(self, count: int, equipment_ids: List[str]) -> List[Dict[str, Any]]:
        """生成告警记录数据"""
        alert_types = ['温度异常', '负载过高', '振动异常', '电压异常', '频率异常']
        alert_levels = ['INFO', 'WARNING', 'CRITICAL']
        
        data = []
        base_time = datetime.now() - timedelta(hours=2)
        
        for i in range(count):
            alert_time = base_time + timedelta(seconds=random.randint(0, 7200))
            resolved = random.choice([True, False])
            
            alert_data = {
                'equipment_id': random.choice(equipment_ids),
                'alert_type': random.choice(alert_types),
                'alert_level': random.choice(alert_levels),
                'alert_time': alert_time,
                'description': f'设备{random.choice(alert_types)}告警',
                'status': 'RESOLVED' if resolved else random.choice(['ACTIVE', 'ACKNOWLEDGED']),
                'resolved_at': alert_time + timedelta(minutes=random.randint(5, 120)) if resolved else None,
                'resolver': f'运维人员{random.randint(1, 10)}' if resolved else None,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            data.append(alert_data)
        return data
    
    def batch_insert(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 1000):
        """批量插入数据"""
        conn = self.get_connection(self.source_config)
        cursor = conn.cursor()
        
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                if table_name == 'equipment_info':
                    insert_sql = """
                        INSERT INTO equipment_info 
                        (equipment_id, equipment_name, equipment_type, voltage_level, location, 
                         installation_date, manufacturer, model, rated_capacity, status, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (equipment_id) DO UPDATE SET
                        equipment_name = EXCLUDED.equipment_name,
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at
                    """
                    values = [(item['equipment_id'], item['equipment_name'], item['equipment_type'],
                              item['voltage_level'], item['location'], item['installation_date'],
                              item['manufacturer'], item['model'], item['rated_capacity'],
                              item['status'], item['created_at'], item['updated_at']) for item in batch]
                
                elif table_name == 'customer_info':
                    insert_sql = """
                        INSERT INTO customer_info 
                        (customer_id, customer_name, customer_type, region, contract_capacity, 
                         voltage_level, contact_person, phone, address, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (customer_id) DO UPDATE SET
                        customer_name = EXCLUDED.customer_name,
                        contact_person = EXCLUDED.contact_person,
                        phone = EXCLUDED.phone,
                        updated_at = EXCLUDED.updated_at
                    """
                    values = [(item['customer_id'], item['customer_name'], item['customer_type'],
                              item['region'], item['contract_capacity'], item['voltage_level'],
                              item['contact_person'], item['phone'], item['address'],
                              item['created_at'], item['updated_at']) for item in batch]
                
                elif table_name == 'power_consumption':
                    insert_sql = """
                        INSERT INTO power_consumption 
                        (customer_id, record_time, active_power, reactive_power, voltage, 
                         current, power_factor, frequency, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = [(item['customer_id'], item['record_time'], item['active_power'],
                              item['reactive_power'], item['voltage'], item['current'],
                              item['power_factor'], item['frequency'], item['created_at']) for item in batch]
                
                elif table_name == 'equipment_status':
                    insert_sql = """
                        INSERT INTO equipment_status 
                        (equipment_id, status_time, temperature, humidity, vibration, 
                         load_rate, health_score, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = [(item['equipment_id'], item['status_time'], item['temperature'],
                              item['humidity'], item['vibration'], item['load_rate'],
                              item['health_score'], item['status'], item['created_at']) for item in batch]
                
                elif table_name == 'alert_records':
                    insert_sql = """
                        INSERT INTO alert_records 
                        (equipment_id, alert_type, alert_level, alert_time, description, 
                         status, resolved_at, resolver, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = [(item['equipment_id'], item['alert_type'], item['alert_level'],
                              item['alert_time'], item['description'], item['status'],
                              item['resolved_at'], item['resolver'], item['created_at'],
                              item['updated_at']) for item in batch]
                
                cursor.executemany(insert_sql, values)
                conn.commit()
                
                with self.lock:
                    self.stats['insert_count'] += len(batch)
                
                print(f"已插入 {table_name} 数据: {self.stats['insert_count']} 条")
                
        except Exception as e:
            conn.rollback()
            with self.lock:
                self.stats['errors'].append(f"插入 {table_name} 失败: {str(e)}")
            print(f"插入 {table_name} 失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    
    def update_random_data(self, table_name: str, count: int):
        """随机更新数据"""
        conn = self.get_connection(self.source_config)
        cursor = conn.cursor()
        
        try:
            if table_name == 'equipment_info':
                for _ in range(count):
                    cursor.execute("""
                        UPDATE equipment_info 
                        SET status = %s, updated_at = %s 
                        WHERE equipment_id = (SELECT equipment_id FROM equipment_info ORDER BY RANDOM() LIMIT 1)
                    """, (random.choice(['运行', '检修', '停运']), datetime.now()))
            
            elif table_name == 'alert_records':
                for _ in range(count):
                    cursor.execute("""
                        UPDATE alert_records 
                        SET status = 'RESOLVED', resolved_at = %s, resolver = %s, updated_at = %s 
                        WHERE id = (SELECT id FROM alert_records WHERE status != 'RESOLVED' ORDER BY RANDOM() LIMIT 1)
                    """, (datetime.now(), f'运维人员{random.randint(1, 10)}', datetime.now()))
            
            conn.commit()
            with self.lock:
                self.stats['update_count'] += count
            
            print(f"已更新 {table_name} 数据: {count} 条")
            
        except Exception as e:
            conn.rollback()
            with self.lock:
                self.stats['errors'].append(f"更新 {table_name} 失败: {str(e)}")
            print(f"更新 {table_name} 失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    
    def check_sink_data(self) -> Dict[str, Any]:
        """检查sink端数据"""
        conn = self.get_connection(self.sink_config)
        cursor = conn.cursor()
        
        sink_stats = {}
        
        try:
            # 检查各个ADS表的数据量
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
                sink_stats[table] = count
                
                # 获取最新更新时间
                if table == 'ads_realtime_dashboard':
                    cursor.execute(f"SELECT MAX(update_time) FROM {table}")
                elif table in ['ads_equipment_health', 'ads_customer_behavior', 'ads_power_quality', 'ads_energy_efficiency']:
                    cursor.execute(f"SELECT MAX(analysis_time) FROM {table}")
                elif table == 'ads_alert_statistics':
                    cursor.execute(f"SELECT MAX(stat_time) FROM {table}")
                elif table == 'ads_risk_assessment':
                    cursor.execute(f"SELECT MAX(assessment_time) FROM {table}")
                
                latest_time = cursor.fetchone()[0]
                sink_stats[f"{table}_latest"] = latest_time
            
        except Exception as e:
            sink_stats['error'] = str(e)
        finally:
            cursor.close()
            conn.close()
        
        return sink_stats
    
    def calculate_latency(self) -> Dict[str, float]:
        """计算数据传输延迟"""
        source_conn = self.get_connection(self.source_config)
        sink_conn = self.get_connection(self.sink_config)
        
        latency_stats = {}
        
        try:
            # 获取source端最新数据时间
            source_cursor = source_conn.cursor()
            source_cursor.execute("SELECT MAX(created_at) FROM power_consumption")
            source_latest = source_cursor.fetchone()[0]
            
            # 获取sink端最新数据时间
            sink_cursor = sink_conn.cursor()
            sink_cursor.execute("SELECT MAX(update_time) FROM ads_realtime_dashboard")
            sink_latest = sink_cursor.fetchone()[0]
            
            if source_latest and sink_latest:
                latency = (sink_latest - source_latest).total_seconds()
                latency_stats['data_latency_seconds'] = latency
                latency_stats['source_latest'] = source_latest.isoformat()
                latency_stats['sink_latest'] = sink_latest.isoformat()
            
        except Exception as e:
            latency_stats['error'] = str(e)
        finally:
            source_cursor.close()
            sink_cursor.close()
            source_conn.close()
            sink_conn.close()
        
        return latency_stats
    
    def run_load_test(self, equipment_count: int = 1000, customer_count: int = 500, 
                     power_records: int = 10000, status_records: int = 5000, 
                     alert_records: int = 1000, update_count: int = 100):
        """运行负载测试"""
        print("开始大数据量CRUD测试...")
        self.stats['start_time'] = datetime.now()
        
        # 1. 生成基础数据
        print("\n=== 生成基础数据 ===")
        equipment_data = self.generate_equipment_data(equipment_count)
        customer_data = self.generate_customer_data(customer_count)
        
        # 2. 批量插入基础数据
        print("\n=== 插入基础数据 ===")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(self.batch_insert, 'equipment_info', equipment_data),
                executor.submit(self.batch_insert, 'customer_info', customer_data)
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"基础数据插入失败: {e}")
        
        # 3. 生成时序数据
        print("\n=== 生成时序数据 ===")
        equipment_ids = [item['equipment_id'] for item in equipment_data]
        customer_ids = [item['customer_id'] for item in customer_data]
        
        power_data = self.generate_power_consumption_data(power_records, customer_ids)
        status_data = self.generate_equipment_status_data(status_records, equipment_ids)
        alert_data = self.generate_alert_data(alert_records, equipment_ids)
        
        # 4. 批量插入时序数据
        print("\n=== 插入时序数据 ===")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(self.batch_insert, 'power_consumption', power_data),
                executor.submit(self.batch_insert, 'equipment_status', status_data),
                executor.submit(self.batch_insert, 'alert_records', alert_data)
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"时序数据插入失败: {e}")
        
        # 5. 执行更新操作
        print("\n=== 执行更新操作 ===")
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.update_random_data, 'equipment_info', update_count),
                executor.submit(self.update_random_data, 'alert_records', update_count)
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"数据更新失败: {e}")
        
        # 6. 等待数据处理
        print("\n=== 等待数据处理 ===")
        time.sleep(30)  # 等待30秒让Flink处理数据
        
        # 7. 检查结果
        print("\n=== 检查处理结果 ===")
        sink_stats = self.check_sink_data()
        latency_stats = self.calculate_latency()
        
        self.stats['end_time'] = datetime.now()
        
        # 8. 输出统计结果
        self.print_test_results(sink_stats, latency_stats)
    
    def print_test_results(self, sink_stats: Dict[str, Any], latency_stats: Dict[str, Any]):
        """打印测试结果"""
        print("\n" + "="*60)
        print("           国网风控数仓 - 大数据量测试结果")
        print("="*60)
        
        # 基本统计
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        print(f"\n📊 测试统计:")
        print(f"   测试时长: {duration:.2f} 秒")
        print(f"   插入记录: {self.stats['insert_count']} 条")
        print(f"   更新记录: {self.stats['update_count']} 条")
        print(f"   插入速度: {self.stats['insert_count']/duration:.2f} 条/秒")
        
        # Sink端数据统计
        print(f"\n📈 Sink端数据统计:")
        for table, count in sink_stats.items():
            if not table.endswith('_latest') and table != 'error':
                print(f"   {table}: {count} 条")
        
        # 延迟统计
        print(f"\n⏱️  数据传输延迟:")
        if 'data_latency_seconds' in latency_stats:
            latency = latency_stats['data_latency_seconds']
            print(f"   数据延迟: {latency:.2f} 秒")
            print(f"   Source最新时间: {latency_stats['source_latest']}")
            print(f"   Sink最新时间: {latency_stats['sink_latest']}")
        else:
            print(f"   延迟计算失败: {latency_stats.get('error', '未知错误')}")
        
        # 错误统计
        if self.stats['errors']:
            print(f"\n❌ 错误信息:")
            for error in self.stats['errors']:
                print(f"   {error}")
        else:
            print(f"\n✅ 测试完成，无错误")
        
        print("\n" + "="*60)

def main():
    """主函数"""
    # 数据库配置
    source_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'source_db',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    sink_config = {
        'host': 'localhost',
        'port': 5433,
        'database': 'sink_db',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    # 创建测试实例
    generator = SGCCDataGenerator(source_config, sink_config)
    
    # 运行负载测试
    # 可以根据需要调整数据量
    generator.run_load_test(
        equipment_count=2000,    # 设备数量
        customer_count=1000,     # 客户数量
        power_records=20000,     # 电力消耗记录
        status_records=10000,    # 设备状态记录
        alert_records=2000,      # 告警记录
        update_count=200         # 更新操作数量
    )

if __name__ == "__main__":
    main()