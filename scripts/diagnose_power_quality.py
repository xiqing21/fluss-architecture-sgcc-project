#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断ads_power_quality表无数据的问题
"""

import psycopg2
from datetime import datetime, timedelta

class PowerQualityDiagnostic:
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
    
    def get_source_connection(self):
        return psycopg2.connect(**self.source_params)
    
    def get_sink_connection(self):
        return psycopg2.connect(**self.sink_params)
    
    def diagnose_power_quality_issue(self):
        """诊断ads_power_quality表无数据的问题"""
        print("\n🔍 诊断ads_power_quality表无数据问题")
        print("="*60)
        
        try:
            # 1. 检查ads_power_quality表本身
            print("\n1️⃣ 检查ads_power_quality表:")
            with self.get_sink_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM ads_power_quality")
                    count = cursor.fetchone()[0]
                    print(f"   ads_power_quality表记录数: {count}")
                    
                    if count > 0:
                        cursor.execute("SELECT * FROM ads_power_quality LIMIT 3")
                        rows = cursor.fetchall()
                        print(f"   最新3条记录: {len(rows)}条")
                        return
            
            # 2. 检查依赖的数据源表
            print("\n2️⃣ 检查依赖的数据源表:")
            self._check_dependency_tables()
            
            # 3. 检查数据流转逻辑
            print("\n3️⃣ 检查数据流转逻辑:")
            self._check_data_flow_logic()
            
            # 4. 模拟INSERT语句执行
            print("\n4️⃣ 模拟INSERT语句执行:")
            self._simulate_insert_execution()
            
        except Exception as e:
            print(f"❌ 诊断过程出错: {e}")
    
    def _check_dependency_tables(self):
        """检查依赖表的数据情况"""
        dependency_tables = [
            'dwd_dim_equipment',
            'dwd_dim_customer', 
            'dwd_fact_power_consumption',
            'dws_customer_hour_power',
            'dws_alert_hour_stats'
        ]
        
        with self.get_sink_connection() as conn:
            with conn.cursor() as cursor:
                for table in dependency_tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"   {table}: {count} 条记录")
                        
                        # 检查最近的数据
                        if table == 'dws_customer_hour_power':
                            cursor.execute(f"""
                                SELECT COUNT(*) FROM {table} 
                                WHERE stat_date >= CURRENT_DATE - INTERVAL '1' DAY
                            """)
                            recent_count = cursor.fetchone()[0]
                            print(f"     最近1天数据: {recent_count} 条")
                            
                        elif table == 'dwd_fact_power_consumption':
                            cursor.execute(f"""
                                SELECT COUNT(*) FROM {table} 
                                WHERE record_date >= CURRENT_DATE - INTERVAL '1' DAY
                            """)
                            recent_count = cursor.fetchone()[0]
                            print(f"     最近1天数据: {recent_count} 条")
                            
                    except Exception as e:
                        print(f"   {table}: ❌ 检查失败 - {e}")
    
    def _check_data_flow_logic(self):
        """检查数据流转逻辑"""
        with self.get_sink_connection() as conn:
            with conn.cursor() as cursor:
                # 检查设备和客户的关联关系
                print("   检查设备-客户关联关系:")
                try:
                    cursor.execute("""
                        SELECT COUNT(DISTINCT e.equipment_id), COUNT(DISTINCT ec.customer_id)
                        FROM dwd_dim_equipment e
                        LEFT JOIN (
                            SELECT DISTINCT equipment_id, customer_id
                            FROM dwd_fact_power_consumption
                            WHERE record_date >= CURRENT_DATE - INTERVAL '1' DAY
                        ) ec ON e.equipment_id = ec.equipment_id
                        WHERE e.is_active = true
                    """)
                    equipment_count, customer_count = cursor.fetchone()
                    print(f"     活跃设备数: {equipment_count}")
                    print(f"     关联客户数: {customer_count}")
                except Exception as e:
                    print(f"     ❌ 关联关系检查失败: {e}")
                
                # 检查电力质量数据的可用性
                print("   检查电力质量数据可用性:")
                try:
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(avg_voltage) as voltage_records,
                            COUNT(avg_power_factor) as power_factor_records,
                            COUNT(voltage_unbalance_avg) as unbalance_records,
                            COUNT(frequency_deviation_avg) as frequency_records
                        FROM dws_customer_hour_power
                        WHERE stat_date >= CURRENT_DATE - INTERVAL '1' DAY
                    """)
                    result = cursor.fetchone()
                    print(f"     总记录数: {result[0]}")
                    print(f"     电压数据: {result[1]}")
                    print(f"     功率因数数据: {result[2]}")
                    print(f"     电压不平衡数据: {result[3]}")
                    print(f"     频率偏差数据: {result[4]}")
                except Exception as e:
                    print(f"     ❌ 电力质量数据检查失败: {e}")
    
    def _simulate_insert_execution(self):
        """模拟INSERT语句执行"""
        with self.get_sink_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # 执行简化版的INSERT查询，只检查能否返回结果
                    cursor.execute("""
                        SELECT COUNT(*)
                        FROM dwd_dim_equipment e
                        LEFT JOIN (
                            SELECT DISTINCT equipment_id, customer_id
                            FROM dwd_fact_power_consumption
                            WHERE record_date >= CURRENT_DATE - INTERVAL '1' DAY
                        ) ec ON e.equipment_id = ec.equipment_id
                        LEFT JOIN dwd_dim_customer c ON ec.customer_id = c.customer_id
                        LEFT JOIN (
                            SELECT 
                                customer_id,
                                AVG(avg_voltage) as avg_voltage,
                                AVG(avg_power_factor) as avg_power_factor,
                                AVG(voltage_unbalance_avg) as voltage_unbalance_avg,
                                AVG(frequency_deviation_avg) as frequency_deviation_avg
                            FROM dws_customer_hour_power
                            WHERE stat_date >= CURRENT_DATE - INTERVAL '1' DAY
                            GROUP BY customer_id
                        ) p ON ec.customer_id = p.customer_id
                        LEFT JOIN (
                            SELECT 
                                equipment_id,
                                SUM(CASE WHEN alert_type = 'POWER_INTERRUPTION' THEN total_alerts ELSE 0 END) as interruption_count,
                                SUM(CASE WHEN alert_type = 'VOLTAGE_SAG' THEN total_alerts ELSE 0 END) as voltage_sag_count,
                                SUM(CASE WHEN alert_type = 'VOLTAGE_SWELL' THEN total_alerts ELSE 0 END) as voltage_swell_count
                            FROM dws_alert_hour_stats
                            WHERE stat_date >= CURRENT_DATE - INTERVAL '1' DAY
                            GROUP BY equipment_id
                        ) a ON e.equipment_id = a.equipment_id
                        WHERE e.is_active = true
                    """)
                    
                    result_count = cursor.fetchone()[0]
                    print(f"   模拟查询结果数量: {result_count}")
                    
                    if result_count == 0:
                        print("   ❌ 查询结果为空，这是ads_power_quality表无数据的原因")
                        print("   💡 建议检查:")
                        print("      - dwd_dim_equipment表中是否有is_active=true的设备")
                        print("      - dwd_fact_power_consumption表中是否有最近1天的数据")
                        print("      - dws_customer_hour_power表中是否有最近1天的汇总数据")
                    else:
                        print(f"   ✅ 查询可以返回{result_count}条结果")
                        print("   💡 建议检查Fluss任务是否正常运行")
                        
                except Exception as e:
                    print(f"   ❌ 模拟执行失败: {e}")
    
    def fix_power_quality_data(self):
        """尝试修复ads_power_quality表数据"""
        print("\n🔧 尝试修复ads_power_quality表数据")
        print("="*60)
        
        try:
            with self.get_sink_connection() as conn:
                with conn.cursor() as cursor:
                    # 手动执行INSERT语句
                    print("   执行INSERT语句...")
                    cursor.execute("""
                        INSERT INTO ads_power_quality
                        SELECT 
                            CAST(HASH_CODE(CONCAT(e.equipment_id, CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as quality_id,
                            e.equipment_id,
                            e.equipment_name,
                            COALESCE(ec.customer_id, 'UNKNOWN') as customer_id,
                            COALESCE(c.customer_name, 'UNKNOWN') as customer_name,
                            CURRENT_TIMESTAMP as analysis_time,
                            CASE 
                                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
                                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
                                WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
                                WHEN p.avg_voltage IS NULL THEN 80.0
                                ELSE 50.0
                            END as voltage_quality_score,
                            CASE 
                                WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
                                WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
                                WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
                                WHEN p.frequency_deviation_avg IS NULL THEN 80.0
                                ELSE 50.0
                            END as frequency_quality_score,
                            CASE 
                                WHEN p.avg_power_factor >= 0.95 THEN 95.0
                                WHEN p.avg_power_factor >= 0.90 THEN 85.0
                                WHEN p.avg_power_factor >= 0.85 THEN 70.0
                                WHEN p.avg_power_factor IS NULL THEN 80.0
                                ELSE 50.0
                            END as power_factor_score,
                            CASE 
                                WHEN e.equipment_type LIKE '%变压器%' THEN 90.0
                                WHEN e.equipment_type LIKE '%断路器%' THEN 95.0
                                WHEN e.equipment_type LIKE '%线路%' THEN 85.0
                                ELSE 80.0
                            END as harmonic_distortion,
                            CASE 
                                WHEN p.voltage_unbalance_avg <= 2.0 THEN 95.0
                                WHEN p.voltage_unbalance_avg <= 4.0 THEN 85.0
                                WHEN p.voltage_unbalance_avg <= 6.0 THEN 70.0
                                WHEN p.voltage_unbalance_avg IS NULL THEN 80.0
                                ELSE 50.0
                            END as voltage_unbalance,
                            CASE 
                                WHEN p.avg_voltage BETWEEN 215 AND 225 THEN 95.0
                                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 85.0
                                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 70.0
                                WHEN p.avg_voltage IS NULL THEN 80.0
                                ELSE 50.0
                            END as flicker_severity,
                            COALESCE(a.interruption_count, 0) as interruption_count,
                            COALESCE(a.voltage_sag_count, 0) as voltage_sag_count,
                            COALESCE(a.voltage_swell_count, 0) as voltage_swell_count,
                            80.0 as overall_quality_score,
                            'GOOD' as quality_grade,
                            '电力质量良好，保持现状' as improvement_suggestions,
                            CURRENT_TIMESTAMP as created_at
                        FROM dwd_dim_equipment e
                        LEFT JOIN (
                            SELECT DISTINCT equipment_id, customer_id
                            FROM dwd_fact_power_consumption
                            WHERE record_date >= CURRENT_DATE - INTERVAL '7' DAY
                        ) ec ON e.equipment_id = ec.equipment_id
                        LEFT JOIN dwd_dim_customer c ON ec.customer_id = c.customer_id
                        LEFT JOIN (
                            SELECT 
                                customer_id,
                                AVG(avg_voltage) as avg_voltage,
                                AVG(avg_power_factor) as avg_power_factor,
                                AVG(voltage_unbalance_avg) as voltage_unbalance_avg,
                                AVG(frequency_deviation_avg) as frequency_deviation_avg
                            FROM dws_customer_hour_power
                            WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
                            GROUP BY customer_id
                        ) p ON ec.customer_id = p.customer_id
                        LEFT JOIN (
                            SELECT 
                                equipment_id,
                                SUM(CASE WHEN alert_type = 'POWER_INTERRUPTION' THEN total_alerts ELSE 0 END) as interruption_count,
                                SUM(CASE WHEN alert_type = 'VOLTAGE_SAG' THEN total_alerts ELSE 0 END) as voltage_sag_count,
                                SUM(CASE WHEN alert_type = 'VOLTAGE_SWELL' THEN total_alerts ELSE 0 END) as voltage_swell_count
                            FROM dws_alert_hour_stats
                            WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
                            GROUP BY equipment_id
                        ) a ON e.equipment_id = a.equipment_id
                        WHERE e.is_active = true
                        AND NOT EXISTS (
                            SELECT 1 FROM ads_power_quality pq 
                            WHERE pq.equipment_id = e.equipment_id 
                            AND pq.analysis_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
                        )
                    """)
                    
                    inserted_count = cursor.rowcount
                    conn.commit()
                    print(f"   ✅ 成功插入 {inserted_count} 条记录")
                    
                    # 验证插入结果
                    cursor.execute("SELECT COUNT(*) FROM ads_power_quality")
                    total_count = cursor.fetchone()[0]
                    print(f"   📊 ads_power_quality表当前总记录数: {total_count}")
                    
        except Exception as e:
            print(f"   ❌ 修复失败: {e}")

def main():
    diagnostic = PowerQualityDiagnostic()
    
    print("🔍 ads_power_quality表诊断工具")
    print("1. 诊断问题")
    print("2. 尝试修复")
    print("0. 退出")
    
    choice = input("请选择操作: ").strip()
    
    if choice == '1':
        diagnostic.diagnose_power_quality_issue()
    elif choice == '2':
        diagnostic.fix_power_quality_data()
    else:
        print("👋 再见!")

if __name__ == "__main__":
    main()