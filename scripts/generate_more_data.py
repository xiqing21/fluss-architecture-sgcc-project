#!/usr/bin/env python3
import psycopg2
import random
from datetime import datetime, timedelta
import uuid

def generate_more_data():
    """生成更多的历史数据用于Grafana显示"""
    
    try:
        # 连接数据库
        conn = psycopg2.connect(
            host='localhost',
            port=5443,
            user='sgcc_user',
            password='sgcc_pass_2024',
            database='sgcc_dw_db'
        )
        cursor = conn.cursor()
        
        print("📊 开始生成更多历史数据...")
        
        # 生成过去24小时的数据，每小时4个数据点
        base_time = datetime.now() - timedelta(hours=24)
        
        for hour in range(24):
            for minute in [0, 15, 30, 45]:  # 每小时4个数据点
                current_time = base_time + timedelta(hours=hour, minutes=minute)
                
                # 生成电力质量数据
                for i in range(3):  # 每个时间点3条记录
                    equipment_id = f"EQ_{1000 + i}"
                    customer_id = f"CUST_{2000 + i}"
                    
                    cursor.execute("""
                        INSERT INTO ads_power_quality (
                            equipment_id, customer_id, analysis_time, 
                            overall_quality, voltage_stability, frequency_stability, 
                            power_factor_quality, harmonic_distortion, voltage_unbalance,
                            quality_grade
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        equipment_id, customer_id, current_time,
                        round(random.uniform(85, 98), 2),  # overall_quality
                        round(random.uniform(90, 100), 2), # voltage_stability
                        round(random.uniform(95, 100), 2), # frequency_stability
                        round(random.uniform(85, 98), 2),  # power_factor_quality
                        round(random.uniform(2, 8), 2),    # harmonic_distortion
                        round(random.uniform(1, 5), 2),    # voltage_unbalance
                        random.choice(['EXCELLENT', 'GOOD', 'FAIR'])  # quality_grade
                    ))
                
                # 生成设备健康数据
                for i in range(3):
                    equipment_id = f"EQ_{1000 + i}"
                    customer_id = f"CUST_{2000 + i}"
                    
                    cursor.execute("""
                        INSERT INTO ads_equipment_health (
                            equipment_id, equipment_name, equipment_type, location,
                            health_score, risk_level, temperature_avg, load_rate_avg,
                            efficiency_avg, fault_count, maintenance_days, prediction_score,
                            recommendation, analysis_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        equipment_id, f"设备_{equipment_id}", "变压器", f"位置_{i+1}",
                        round(random.uniform(80, 95), 2),  # health_score
                        random.choice(['LOW', 'MEDIUM', 'HIGH']),  # risk_level
                        round(random.uniform(35, 65), 1),  # temperature_avg
                        round(random.uniform(60, 90), 2),  # load_rate_avg
                        round(random.uniform(85, 98), 2),  # efficiency_avg
                        random.randint(0, 3),              # fault_count
                        random.randint(10, 180),           # maintenance_days
                        round(random.uniform(70, 90), 2),  # prediction_score
                        f"建议_{random.randint(1, 5)}",      # recommendation
                        current_time
                    ))
                
                # 生成告警统计数据
                cursor.execute("""
                    INSERT INTO ads_alert_statistics (
                        stat_period, stat_time, critical_alerts, error_alerts, 
                        warning_alerts, info_alerts, total_alerts
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    'HOURLY',               # stat_period
                    current_time,
                    random.randint(0, 5),   # critical_alerts
                    random.randint(2, 15),  # error_alerts
                    random.randint(5, 25),  # warning_alerts
                    random.randint(10, 50), # info_alerts
                    random.randint(17, 95)  # total_alerts
                ))
                
                # 生成能效分析数据
                for i in range(3):
                    equipment_id = f"EQ_{1000 + i}"
                    customer_id = f"CUST_{2000 + i}"
                    
                    cursor.execute("""
                        INSERT INTO ads_energy_efficiency (
                            analysis_scope, scope_id, scope_name, analysis_period, analysis_time,
                            energy_input, energy_output, energy_loss, efficiency_ratio,
                            benchmark_efficiency, efficiency_gap, carbon_intensity,
                            cost_per_kwh, potential_savings, efficiency_grade, optimization_suggestions
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        'EQUIPMENT',  # analysis_scope
                        str(equipment_id),  # scope_id
                        f'设备{equipment_id}',  # scope_name
                        'DAILY',  # analysis_period
                        current_time,
                        round(random.uniform(100.0, 1000.0), 2),  # energy_input
                        round(random.uniform(80.0, 900.0), 2),   # energy_output
                        round(random.uniform(5.0, 50.0), 2),     # energy_loss
                        round(random.uniform(75, 95), 2),    # efficiency_ratio
                        round(random.uniform(80.0, 90.0), 2),    # benchmark_efficiency
                        round(random.uniform(-10.0, 10.0), 2),   # efficiency_gap
                        round(random.uniform(0.5, 1.2), 4),      # carbon_intensity
                        round(random.uniform(0.6, 1.0), 4),      # cost_per_kwh
                        round(random.uniform(5, 25), 2),     # potential_savings
                        random.choice(['EXCELLENT', 'GOOD', 'FAIR', 'POOR']),  # efficiency_grade
                        f"优化建议_{random.randint(1, 10)}"      # optimization_suggestions
                    ))
        
        conn.commit()
        
        # 检查生成的数据量
        tables = ['ads_power_quality', 'ads_equipment_health', 'ads_alert_statistics', 'ads_energy_efficiency']
        print("\n=== 数据生成完成，最新统计 ===")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count} 条记录")
        
        cursor.close()
        conn.close()
        
        print("\n✅ 历史数据生成完成!")
        print("   - 生成了过去24小时的数据")
        print("   - 每小时4个数据点")
        print("   - 总计: 288个时间点的数据")
        print("   - 现在Grafana大屏应该能显示趋势图表了")
        
    except Exception as e:
        print(f"❌ 数据生成失败: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == '__main__':
    generate_more_data()