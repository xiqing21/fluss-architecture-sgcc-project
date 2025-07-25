#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网风控数仓 - 延迟监控和性能测试脚本
实时监控数据传输延迟和系统性能指标
"""

import psycopg2
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import statistics
from dataclasses import dataclass
import signal
import sys

@dataclass
class LatencyMetric:
    """延迟指标数据类"""
    timestamp: datetime
    source_time: Optional[datetime]
    sink_time: Optional[datetime]
    latency_seconds: Optional[float]
    record_count: int
    table_name: str

class LatencyMonitor:
    """延迟监控器"""
    
    def __init__(self, source_config: Dict[str, Any], sink_config: Dict[str, Any]):
        self.source_config = source_config
        self.sink_config = sink_config
        self.metrics: List[LatencyMetric] = []
        self.running = False
        self.lock = threading.Lock()
        
        # 监控配置
        self.monitor_interval = 10  # 监控间隔（秒）
        self.max_metrics = 1000     # 最大保存指标数量
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """信号处理器"""
        print("\n收到停止信号，正在优雅关闭...")
        self.stop_monitoring()
        sys.exit(0)
    
    def get_connection(self, config: Dict[str, Any]):
        """获取数据库连接"""
        return psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
    
    def measure_table_latency(self, source_table: str, sink_table: str, 
                            source_time_col: str, sink_time_col: str) -> LatencyMetric:
        """测量特定表的延迟"""
        source_conn = None
        sink_conn = None
        
        try:
            # 连接数据库
            source_conn = self.get_connection(self.source_config)
            sink_conn = self.get_connection(self.sink_config)
            
            source_cursor = source_conn.cursor()
            sink_cursor = sink_conn.cursor()
            
            # 获取source端最新数据
            source_cursor.execute(f"""
                SELECT MAX({source_time_col}) as latest_time, COUNT(*) as record_count
                FROM {source_table}
                WHERE {source_time_col} > NOW() - INTERVAL '1 hour'
            """)
            source_result = source_cursor.fetchone()
            source_time = source_result[0] if source_result[0] else None
            source_count = source_result[1] if source_result[1] else 0
            
            # 获取sink端最新数据
            sink_cursor.execute(f"""
                SELECT MAX({sink_time_col}) as latest_time, COUNT(*) as record_count
                FROM {sink_table}
                WHERE {sink_time_col} > NOW() - INTERVAL '1 hour'
            """)
            sink_result = sink_cursor.fetchone()
            sink_time = sink_result[0] if sink_result[0] else None
            sink_count = sink_result[1] if sink_result[1] else 0
            
            # 计算延迟
            latency = None
            if source_time and sink_time:
                latency = (sink_time - source_time).total_seconds()
            
            return LatencyMetric(
                timestamp=datetime.now(),
                source_time=source_time,
                sink_time=sink_time,
                latency_seconds=latency,
                record_count=max(source_count, sink_count),
                table_name=f"{source_table}->{sink_table}"
            )
            
        except Exception as e:
            print(f"测量延迟失败 {source_table}->{sink_table}: {e}")
            return LatencyMetric(
                timestamp=datetime.now(),
                source_time=None,
                sink_time=None,
                latency_seconds=None,
                record_count=0,
                table_name=f"{source_table}->{sink_table}"
            )
        finally:
            if source_conn:
                source_conn.close()
            if sink_conn:
                sink_conn.close()
    
    def measure_end_to_end_latency(self) -> List[LatencyMetric]:
        """测量端到端延迟"""
        metrics = []
        
        # 定义监控的表映射关系
        table_mappings = [
            {
                'source_table': 'power_consumption',
                'sink_table': 'ads_realtime_dashboard',
                'source_time_col': 'created_at',
                'sink_time_col': 'update_time'
            },
            {
                'source_table': 'equipment_status',
                'sink_table': 'ads_equipment_health',
                'source_time_col': 'created_at',
                'sink_time_col': 'analysis_time'
            },
            {
                'source_table': 'power_consumption',
                'sink_table': 'ads_power_quality',
                'source_time_col': 'created_at',
                'sink_time_col': 'analysis_time'
            },
            {
                'source_table': 'alert_records',
                'sink_table': 'ads_alert_statistics',
                'source_time_col': 'created_at',
                'sink_time_col': 'stat_time'
            }
        ]
        
        for mapping in table_mappings:
            metric = self.measure_table_latency(
                mapping['source_table'],
                mapping['sink_table'],
                mapping['source_time_col'],
                mapping['sink_time_col']
            )
            metrics.append(metric)
        
        return metrics
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """获取系统性能指标"""
        source_conn = None
        sink_conn = None
        
        try:
            source_conn = self.get_connection(self.source_config)
            sink_conn = self.get_connection(self.sink_config)
            
            source_cursor = source_conn.cursor()
            sink_cursor = sink_conn.cursor()
            
            # Source端指标
            source_cursor.execute("""
                SELECT 
                    'source' as db_type,
                    COUNT(*) as total_connections,
                    SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) as active_connections
                FROM pg_stat_activity 
                WHERE datname = current_database()
            """)
            source_stats = source_cursor.fetchone()
            
            # 获取source端表大小
            source_cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """)
            source_table_sizes = source_cursor.fetchall()
            
            # Sink端指标
            sink_cursor.execute("""
                SELECT 
                    'sink' as db_type,
                    COUNT(*) as total_connections,
                    SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) as active_connections
                FROM pg_stat_activity 
                WHERE datname = current_database()
            """)
            sink_stats = sink_cursor.fetchone()
            
            # 获取sink端表大小
            sink_cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """)
            sink_table_sizes = sink_cursor.fetchall()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'source_db': {
                    'total_connections': source_stats[1],
                    'active_connections': source_stats[2],
                    'table_sizes': [{
                        'schema': row[0],
                        'table': row[1],
                        'size_pretty': row[2],
                        'size_bytes': row[3]
                    } for row in source_table_sizes]
                },
                'sink_db': {
                    'total_connections': sink_stats[1],
                    'active_connections': sink_stats[2],
                    'table_sizes': [{
                        'schema': row[0],
                        'table': row[1],
                        'size_pretty': row[2],
                        'size_bytes': row[3]
                    } for row in sink_table_sizes]
                }
            }
            
        except Exception as e:
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
        finally:
            if source_conn:
                source_conn.close()
            if sink_conn:
                sink_conn.close()
    
    def calculate_statistics(self, window_minutes: int = 10) -> Dict[str, Any]:
        """计算统计指标"""
        with self.lock:
            # 获取指定时间窗口内的指标
            cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
            recent_metrics = [m for m in self.metrics if m.timestamp > cutoff_time]
        
        if not recent_metrics:
            return {'error': '没有足够的数据进行统计'}
        
        # 按表分组计算统计
        table_stats = {}
        for table_name in set(m.table_name for m in recent_metrics):
            table_metrics = [m for m in recent_metrics if m.table_name == table_name]
            latencies = [m.latency_seconds for m in table_metrics if m.latency_seconds is not None]
            
            if latencies:
                table_stats[table_name] = {
                    'count': len(latencies),
                    'avg_latency': statistics.mean(latencies),
                    'min_latency': min(latencies),
                    'max_latency': max(latencies),
                    'median_latency': statistics.median(latencies),
                    'std_latency': statistics.stdev(latencies) if len(latencies) > 1 else 0
                }
        
        # 整体统计
        all_latencies = [m.latency_seconds for m in recent_metrics if m.latency_seconds is not None]
        overall_stats = {}
        if all_latencies:
            overall_stats = {
                'total_measurements': len(all_latencies),
                'avg_latency': statistics.mean(all_latencies),
                'min_latency': min(all_latencies),
                'max_latency': max(all_latencies),
                'median_latency': statistics.median(all_latencies),
                'p95_latency': sorted(all_latencies)[int(len(all_latencies) * 0.95)] if len(all_latencies) > 20 else max(all_latencies),
                'p99_latency': sorted(all_latencies)[int(len(all_latencies) * 0.99)] if len(all_latencies) > 100 else max(all_latencies)
            }
        
        return {
            'window_minutes': window_minutes,
            'timestamp': datetime.now().isoformat(),
            'overall_stats': overall_stats,
            'table_stats': table_stats
        }
    
    def print_real_time_stats(self):
        """打印实时统计信息"""
        stats = self.calculate_statistics(window_minutes=5)
        system_metrics = self.get_system_metrics()
        
        # 清屏
        print("\033[2J\033[H", end="")
        
        print("="*80)
        print("           国网风控数仓 - 实时延迟监控")
        print(f"           监控时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # 整体统计
        if 'overall_stats' in stats and stats['overall_stats']:
            overall = stats['overall_stats']
            print(f"\n📊 整体延迟统计 (最近5分钟):")
            print(f"   测量次数: {overall['total_measurements']}")
            print(f"   平均延迟: {overall['avg_latency']:.2f} 秒")
            print(f"   最小延迟: {overall['min_latency']:.2f} 秒")
            print(f"   最大延迟: {overall['max_latency']:.2f} 秒")
            print(f"   中位延迟: {overall['median_latency']:.2f} 秒")
            if 'p95_latency' in overall:
                print(f"   P95延迟:  {overall['p95_latency']:.2f} 秒")
            if 'p99_latency' in overall:
                print(f"   P99延迟:  {overall['p99_latency']:.2f} 秒")
        
        # 分表统计
        if 'table_stats' in stats and stats['table_stats']:
            print(f"\n📈 分表延迟统计:")
            for table_name, table_stat in stats['table_stats'].items():
                print(f"   {table_name}:")
                print(f"     平均: {table_stat['avg_latency']:.2f}s | "
                      f"最小: {table_stat['min_latency']:.2f}s | "
                      f"最大: {table_stat['max_latency']:.2f}s")
        
        # 系统指标
        if 'error' not in system_metrics:
            print(f"\n🖥️  系统指标:")
            source_db = system_metrics['source_db']
            sink_db = system_metrics['sink_db']
            print(f"   Source DB: {source_db['active_connections']}/{source_db['total_connections']} 活跃连接")
            print(f"   Sink DB:   {sink_db['active_connections']}/{sink_db['total_connections']} 活跃连接")
            
            # 显示最大的几个表
            print(f"\n💾 表大小 (Top 3):")
            print(f"   Source端:")
            for table in source_db['table_sizes'][:3]:
                print(f"     {table['table']}: {table['size_pretty']}")
            print(f"   Sink端:")
            for table in sink_db['table_sizes'][:3]:
                print(f"     {table['table']}: {table['size_pretty']}")
        
        # 最新延迟
        with self.lock:
            if self.metrics:
                latest_metrics = self.metrics[-4:]  # 显示最新4条
                print(f"\n⏱️  最新延迟测量:")
                for metric in latest_metrics:
                    if metric.latency_seconds is not None:
                        print(f"   {metric.timestamp.strftime('%H:%M:%S')} | "
                              f"{metric.table_name}: {metric.latency_seconds:.2f}s")
        
        print(f"\n按 Ctrl+C 停止监控")
        print("="*80)
    
    def monitor_loop(self):
        """监控循环"""
        while self.running:
            try:
                # 测量延迟
                new_metrics = self.measure_end_to_end_latency()
                
                with self.lock:
                    self.metrics.extend(new_metrics)
                    # 保持指标数量在限制内
                    if len(self.metrics) > self.max_metrics:
                        self.metrics = self.metrics[-self.max_metrics:]
                
                # 打印实时统计
                self.print_real_time_stats()
                
                # 等待下次监控
                time.sleep(self.monitor_interval)
                
            except Exception as e:
                print(f"监控循环错误: {e}")
                time.sleep(5)
    
    def start_monitoring(self):
        """开始监控"""
        print("开始延迟监控...")
        self.running = True
        
        # 启动监控线程
        monitor_thread = threading.Thread(target=self.monitor_loop)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        try:
            # 主线程等待
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """停止监控"""
        print("\n停止监控...")
        self.running = False
        
        # 保存最终统计
        final_stats = self.calculate_statistics(window_minutes=60)
        
        print("\n" + "="*60)
        print("           最终统计报告")
        print("="*60)
        
        if 'overall_stats' in final_stats and final_stats['overall_stats']:
            overall = final_stats['overall_stats']
            print(f"\n📊 整体性能 (最近1小时):")
            print(f"   总测量次数: {overall['total_measurements']}")
            print(f"   平均延迟: {overall['avg_latency']:.2f} 秒")
            print(f"   最佳延迟: {overall['min_latency']:.2f} 秒")
            print(f"   最差延迟: {overall['max_latency']:.2f} 秒")
            print(f"   中位延迟: {overall['median_latency']:.2f} 秒")
        
        # 保存详细报告到文件
        report_file = f"latency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump({
                'final_stats': final_stats,
                'all_metrics': [{
                    'timestamp': m.timestamp.isoformat(),
                    'table_name': m.table_name,
                    'latency_seconds': m.latency_seconds,
                    'record_count': m.record_count
                } for m in self.metrics]
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 详细报告已保存到: {report_file}")
        print("监控结束。")

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
    
    # 创建监控器
    monitor = LatencyMonitor(source_config, sink_config)
    
    # 开始监控
    monitor.start_monitoring()

if __name__ == "__main__":
    main()