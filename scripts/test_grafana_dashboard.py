#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电力监控实时大屏 Grafana 测试脚本
测试 Grafana 大屏与 PostgreSQL Sink 数据库的集成
"""

import psycopg2
import json
import sys
from datetime import datetime
from typing import Dict, List, Any

# PostgreSQL Sink 数据库连接配置
SINK_DB_CONFIG = {
    'host': 'localhost',
    'port': 5443,  # sink 端端口
    'database': 'sgcc_dw_db',
    'user': 'sgcc_user',
    'password': 'sgcc_pass_2024'
}

class GrafanaDashboardTester:
    def __init__(self):
        self.sink_conn = None
        self.test_results = []
        
    def connect_to_sink_db(self) -> bool:
        """连接到 PostgreSQL Sink 数据库"""
        try:
            self.sink_conn = psycopg2.connect(**SINK_DB_CONFIG)
            print("✅ 成功连接到 PostgreSQL Sink 数据库")
            return True
        except Exception as e:
            print(f"❌ 连接 PostgreSQL Sink 数据库失败: {e}")
            return False
    
    def test_table_existence(self) -> Dict[str, bool]:
        """测试 ADS 表是否存在"""
        print("\n🔍 检查 ADS 表是否存在...")
        
        required_tables = [
            'ads_power_quality',
            'ads_equipment_health', 
            'ads_alert_statistics',
            'ads_energy_efficiency'
        ]
        
        table_status = {}
        cursor = self.sink_conn.cursor()
        
        for table in required_tables:
            try:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{table}'
                    )
                """)
                exists = cursor.fetchone()[0]
                table_status[table] = exists
                status = "✅" if exists else "❌"
                print(f"  {status} {table}: {'存在' if exists else '不存在'}")
            except Exception as e:
                table_status[table] = False
                print(f"  ❌ {table}: 检查失败 - {e}")
        
        cursor.close()
        return table_status
    
    def test_table_data(self) -> Dict[str, int]:
        """测试表中是否有数据"""
        print("\n📊 检查表中数据量...")
        
        tables = [
            'ads_power_quality',
            'ads_equipment_health', 
            'ads_alert_statistics',
            'ads_energy_efficiency'
        ]
        
        data_counts = {}
        cursor = self.sink_conn.cursor()
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                data_counts[table] = count
                status = "✅" if count > 0 else "⚠️"
                print(f"  {status} {table}: {count} 条记录")
            except Exception as e:
                data_counts[table] = 0
                print(f"  ❌ {table}: 查询失败 - {e}")
        
        cursor.close()
        return data_counts
    
    def test_grafana_sql_queries(self) -> List[Dict[str, Any]]:
        """测试 Grafana 仪表板中的 SQL 查询"""
        print("\n🔍 测试 Grafana SQL 查询...")
        
        # 从 Grafana 配置中提取的关键 SQL 查询
        test_queries = [
            {
                'name': '电力质量实时监控',
                'sql': '''
                    SELECT 
                        equipment_id,
                        customer_id,
                        voltage_stability,
                        frequency_stability,
                        power_factor_quality,
                        overall_quality,
                        quality_grade,
                        analysis_time
                    FROM ads_power_quality 
                    ORDER BY analysis_time DESC 
                    LIMIT 10
                '''
            },
            {
                'name': '设备健康度统计',
                'sql': '''
                    SELECT AVG(health_score) as avg_health_score
                    FROM ads_equipment_health
                    WHERE health_score IS NOT NULL
                '''
            },
            {
                'name': '严重告警数量',
                'sql': '''
                    SELECT SUM(critical_alerts) as critical_alerts
                    FROM ads_alert_statistics
                    WHERE critical_alerts IS NOT NULL
                '''
            },
            {
                'name': '能效评分',
                'sql': '''
                    SELECT AVG(efficiency_ratio) as avg_efficiency
                    FROM ads_energy_efficiency
                    WHERE efficiency_ratio IS NOT NULL
                '''
            },
            {
                'name': '告警级别分布',
                'sql': '''
                    SELECT 
                        'CRITICAL' as alert_level,
                        SUM(critical_alerts) as count
                    FROM ads_alert_statistics
                    UNION ALL
                    SELECT 
                        'ERROR' as alert_level,
                        SUM(error_alerts) as count
                    FROM ads_alert_statistics
                    UNION ALL
                    SELECT 
                        'WARNING' as alert_level,
                        SUM(warning_alerts) as count
                    FROM ads_alert_statistics
                '''
            }
        ]
        
        query_results = []
        cursor = self.sink_conn.cursor()
        
        for query_info in test_queries:
            try:
                cursor.execute(query_info['sql'])
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                query_results.append({
                    'name': query_info['name'],
                    'status': 'success',
                    'row_count': len(results),
                    'columns': columns,
                    'sample_data': results[:3] if results else []
                })
                
                print(f"  ✅ {query_info['name']}: {len(results)} 行结果")
                
            except Exception as e:
                query_results.append({
                    'name': query_info['name'],
                    'status': 'error',
                    'error': str(e)
                })
                print(f"  ❌ {query_info['name']}: 查询失败 - {e}")
        
        cursor.close()
        return query_results
    
    def test_data_freshness(self) -> Dict[str, Any]:
        """测试数据新鲜度"""
        print("\n⏰ 检查数据新鲜度...")
        
        freshness_results = {}
        cursor = self.sink_conn.cursor()
        
        # 检查各表最新数据时间
        time_columns = {
            'ads_power_quality': 'analysis_time',
            'ads_equipment_health': 'analysis_time',
            'ads_alert_statistics': 'stat_time',
            'ads_energy_efficiency': 'analysis_time'
        }
        
        for table, time_col in time_columns.items():
            try:
                cursor.execute(f"""
                    SELECT MAX({time_col}) as latest_time
                    FROM {table}
                    WHERE {time_col} IS NOT NULL
                """)
                result = cursor.fetchone()
                latest_time = result[0] if result and result[0] else None
                
                if latest_time:
                    time_diff = datetime.now() - latest_time
                    minutes_old = time_diff.total_seconds() / 60
                    
                    freshness_results[table] = {
                        'latest_time': latest_time.isoformat(),
                        'minutes_old': round(minutes_old, 2),
                        'is_fresh': minutes_old < 60  # 1小时内为新鲜
                    }
                    
                    status = "✅" if minutes_old < 60 else "⚠️"
                    print(f"  {status} {table}: 最新数据 {round(minutes_old, 1)} 分钟前")
                else:
                    freshness_results[table] = {
                        'latest_time': None,
                        'minutes_old': None,
                        'is_fresh': False
                    }
                    print(f"  ❌ {table}: 无时间数据")
                    
            except Exception as e:
                freshness_results[table] = {'error': str(e)}
                print(f"  ❌ {table}: 检查失败 - {e}")
        
        cursor.close()
        return freshness_results
    
    def generate_test_report(self, table_status: Dict[str, bool], 
                           data_counts: Dict[str, int],
                           query_results: List[Dict[str, Any]],
                           freshness_results: Dict[str, Any]) -> Dict[str, Any]:
        """生成测试报告"""
        print("\n📋 生成测试报告...")
        
        # 计算总体状态
        tables_exist = all(table_status.values())
        has_data = any(count > 0 for count in data_counts.values())
        queries_work = all(q['status'] == 'success' for q in query_results)
        data_is_fresh = any(
            result.get('is_fresh', False) 
            for result in freshness_results.values() 
            if isinstance(result, dict) and 'is_fresh' in result
        )
        
        overall_status = tables_exist and has_data and queries_work
        
        report = {
            'test_time': datetime.now().isoformat(),
            'overall_status': 'PASS' if overall_status else 'FAIL',
            'summary': {
                'tables_exist': tables_exist,
                'has_data': has_data,
                'queries_work': queries_work,
                'data_is_fresh': data_is_fresh
            },
            'details': {
                'table_status': table_status,
                'data_counts': data_counts,
                'query_results': query_results,
                'freshness_results': freshness_results
            },
            'recommendations': []
        }
        
        # 生成建议
        if not tables_exist:
            report['recommendations'].append("需要运行 Flink SQL 作业创建 ADS 表")
        
        if not has_data:
            report['recommendations'].append("需要运行数据生成脚本或检查数据流")
        
        if not queries_work:
            report['recommendations'].append("需要检查表结构和字段名是否匹配")
        
        if not data_is_fresh:
            report['recommendations'].append("需要检查实时数据流是否正常")
        
        return report
    
    def run_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        print("🚀 开始测试 Grafana 大屏与 PostgreSQL Sink 集成...")
        
        if not self.connect_to_sink_db():
            return {'status': 'FAIL', 'error': '无法连接到数据库'}
        
        try:
            # 运行各项测试
            table_status = self.test_table_existence()
            data_counts = self.test_table_data()
            query_results = self.test_grafana_sql_queries()
            freshness_results = self.test_data_freshness()
            
            # 生成报告
            report = self.generate_test_report(
                table_status, data_counts, query_results, freshness_results
            )
            
            return report
            
        except Exception as e:
            return {'status': 'ERROR', 'error': str(e)}
        
        finally:
            if self.sink_conn:
                self.sink_conn.close()
                print("\n🔌 数据库连接已关闭")
    
    def print_report(self, report: Dict[str, Any]):
        """打印测试报告"""
        print("\n" + "="*60)
        print("📊 GRAFANA 大屏测试报告")
        print("="*60)
        
        if 'error' in report:
            print(f"❌ 测试失败: {report['error']}")
            return
        
        status_icon = "✅" if report['overall_status'] == 'PASS' else "❌"
        print(f"{status_icon} 总体状态: {report['overall_status']}")
        print(f"🕐 测试时间: {report['test_time']}")
        
        print("\n📋 测试摘要:")
        summary = report['summary']
        print(f"  表结构: {'✅ 正常' if summary['tables_exist'] else '❌ 异常'}")
        print(f"  数据可用: {'✅ 有数据' if summary['has_data'] else '❌ 无数据'}")
        print(f"  查询正常: {'✅ 正常' if summary['queries_work'] else '❌ 异常'}")
        print(f"  数据新鲜: {'✅ 新鲜' if summary['data_is_fresh'] else '⚠️ 过期'}")
        
        if report['recommendations']:
            print("\n💡 建议:")
            for i, rec in enumerate(report['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        print("\n" + "="*60)

def main():
    """主函数"""
    tester = GrafanaDashboardTester()
    
    try:
        report = tester.run_tests()
        tester.print_report(report)
        
        # 保存详细报告到文件
        with open('grafana_test_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print("\n💾 详细报告已保存到 grafana_test_report.json")
        
        # 返回适当的退出码
        if report.get('overall_status') == 'PASS':
            print("\n🎉 所有测试通过！Grafana 大屏可以正常使用")
            sys.exit(0)
        else:
            print("\n⚠️ 部分测试失败，请查看建议进行修复")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️ 测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 测试过程中发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()