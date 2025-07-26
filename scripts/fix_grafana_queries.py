#!/usr/bin/env python3
import json
import re

def fix_grafana_queries():
    """修复Grafana配置文件中的SQL查询错误"""
    
    grafana_file = '/Users/felix/cloud_enviroment/realtime/fluss/trae-fluss-base/grafana/dashboards/sgcc-power-monitoring.json'
    
    print("🔧 开始修复Grafana配置文件中的SQL查询...")
    
    # 读取配置文件
    with open(grafana_file, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # 修复计数器
    fixes_applied = 0
    
    # 遍历所有面板
    for panel in config.get('panels', []):
        if 'targets' in panel:
            for target in panel['targets']:
                if 'rawSql' in target:
                    original_sql = target['rawSql']
                    fixed_sql = original_sql
                    
                    # 修复1: 电力质量与设备健康趋势 - 简化JOIN查询
                    if '电力质量与设备健康趋势' in panel.get('title', ''):
                        print("  📊 修复电力质量与设备健康趋势查询...")
                        fixed_sql = """SELECT 
  analysis_time as time,
  AVG(overall_quality) as "电力质量评分"
FROM ads_power_quality 
WHERE analysis_time >= NOW() - INTERVAL '24 hours'
GROUP BY analysis_time
ORDER BY analysis_time"""
                        fixes_applied += 1
                    
                    # 修复2: 能效分析趋势 - 修复字段名
                    elif '能效分析趋势' in panel.get('title', ''):
                        print("  ⚡ 修复能效分析趋势查询...")
                        fixed_sql = """SELECT 
  analysis_time as time,
  AVG(efficiency_ratio) as "能效评分",
  AVG(potential_savings) as "节能潜力"
FROM ads_energy_efficiency 
WHERE analysis_time >= NOW() - INTERVAL '24 hours'
GROUP BY analysis_time
ORDER BY analysis_time"""
                        fixes_applied += 1
                    
                    # 修复3: 高风险设备监控 - 修复字段名
                    elif '高风险设备监控' in panel.get('title', ''):
                        print("  ⚠️  修复高风险设备监控查询...")
                        fixed_sql = """SELECT 
  equipment_id as "设备ID",
  customer_id as "客户ID",
  overall_risk_score as "风险评分",
  risk_level as "风险等级",
  analysis_time as "分析时间"
FROM ads_risk_assessment 
WHERE risk_level IN ('HIGH', 'CRITICAL')
ORDER BY overall_risk_score DESC, analysis_time DESC 
LIMIT 10"""
                        fixes_applied += 1
                    
                    # 修复4: 数据新鲜度监控 - 修复字段名
                    elif '数据新鲜度监控' in panel.get('title', ''):
                        print("  🕐 修复数据新鲜度监控查询...")
                        fixed_sql = """SELECT
  NOW() AS time,
  EXTRACT(EPOCH FROM (NOW() - MAX(update_time))) as "实时监控表数据延迟(秒)"
FROM ads_realtime_dashboard
UNION ALL
SELECT
  NOW() AS time,
  EXTRACT(EPOCH FROM (NOW() - MAX(analysis_time))) as "设备健康表数据延迟(秒)"
FROM ads_equipment_health
UNION ALL
SELECT
  NOW() AS time,
  EXTRACT(EPOCH FROM (NOW() - MAX(analysis_time))) as "电力质量表数据延迟(秒)"
FROM ads_power_quality"""
                        fixes_applied += 1
                    
                    # 修复5: 实时数据更新频率 - 修复字段名
                    elif 'rawSql' in target and 'ads_realtime_dashboard' in target['rawSql'] and 'create_time' in target['rawSql']:
                        print("  📈 修复实时数据更新频率查询...")
                        fixed_sql = """SELECT
  DATE_TRUNC('minute', update_time) AS time,
  COUNT(*) as "数据更新次数"
FROM ads_realtime_dashboard
WHERE update_time >= NOW() - INTERVAL '1 hour'
GROUP BY DATE_TRUNC('minute', update_time)
ORDER BY time"""
                        fixes_applied += 1
                    
                    # 应用修复
                    if fixed_sql != original_sql:
                        target['rawSql'] = fixed_sql
    
    # 保存修复后的配置文件
    with open(grafana_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Grafana配置文件修复完成!")
    print(f"   - 应用了 {fixes_applied} 个修复")
    print(f"   - 修复了字段名错误和复杂JOIN查询")
    print(f"   - 简化了查询以确保数据显示")
    
if __name__ == '__main__':
    fix_grafana_queries()