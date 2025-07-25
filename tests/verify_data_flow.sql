-- 国网风控数仓 - 数据流验证脚本
-- 验证从PostgreSQL Source到Sink的数据传输

-- ========================================
-- PostgreSQL Source数据库验证
-- ========================================

-- 连接到source数据库后执行以下查询

-- 1. 验证Source端数据总量
SELECT 
    'SOURCE DATABASE SUMMARY' as check_type,
    'equipment_info' as table_name, 
    COUNT(*) as record_count,
    MAX(updated_at) as last_update
FROM equipment_info
UNION ALL
SELECT 
    'SOURCE DATABASE SUMMARY',
    'customer_info', 
    COUNT(*), 
    MAX(updated_at)
FROM customer_info
UNION ALL
SELECT 
    'SOURCE DATABASE SUMMARY',
    'power_consumption', 
    COUNT(*), 
    MAX(created_at)
FROM power_consumption
UNION ALL
SELECT 
    'SOURCE DATABASE SUMMARY',
    'equipment_status', 
    COUNT(*), 
    MAX(created_at)
FROM equipment_status
UNION ALL
SELECT 
    'SOURCE DATABASE SUMMARY',
    'alert_records', 
    COUNT(*), 
    MAX(updated_at)
FROM alert_records;

-- 2. 验证最近1小时的数据变化
SELECT 
    'RECENT CHANGES (1H)' as check_type,
    'power_consumption' as table_name,
    COUNT(*) as new_records,
    MIN(record_time) as earliest_time,
    MAX(record_time) as latest_time
FROM power_consumption 
WHERE record_time > NOW() - INTERVAL '1 hour'
UNION ALL
SELECT 
    'RECENT CHANGES (1H)',
    'equipment_status',
    COUNT(*),
    MIN(status_time),
    MAX(status_time)
FROM equipment_status 
WHERE status_time > NOW() - INTERVAL '1 hour'
UNION ALL
SELECT 
    'RECENT CHANGES (1H)',
    'alert_records',
    COUNT(*),
    MIN(alert_time),
    MAX(alert_time)
FROM alert_records 
WHERE alert_time > NOW() - INTERVAL '1 hour';

-- 3. 验证数据质量
SELECT 
    'DATA QUALITY CHECK' as check_type,
    'power_consumption' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN active_power IS NULL THEN 1 END) as null_power,
    COUNT(CASE WHEN power_factor < 0.8 OR power_factor > 1.0 THEN 1 END) as invalid_pf,
    AVG(active_power) as avg_power
FROM power_consumption
UNION ALL
SELECT 
    'DATA QUALITY CHECK',
    'equipment_status',
    COUNT(*),
    COUNT(CASE WHEN health_score IS NULL THEN 1 END),
    COUNT(CASE WHEN health_score < 0 OR health_score > 100 THEN 1 END),
    AVG(health_score)
FROM equipment_status;

-- ========================================
-- PostgreSQL Sink数据库验证
-- ========================================

-- 连接到sink数据库后执行以下查询

-- 1. 验证ADS层表数据
SELECT 
    'SINK DATABASE SUMMARY' as check_type,
    'ads_realtime_dashboard' as table_name,
    COUNT(*) as record_count,
    MAX(update_time) as last_update
FROM ads_realtime_dashboard
UNION ALL
SELECT 
    'SINK DATABASE SUMMARY',
    'ads_equipment_health',
    COUNT(*),
    MAX(analysis_time)
FROM ads_equipment_health
UNION ALL
SELECT 
    'SINK DATABASE SUMMARY',
    'ads_customer_behavior',
    COUNT(*),
    MAX(analysis_time)
FROM ads_customer_behavior
UNION ALL
SELECT 
    'SINK DATABASE SUMMARY',
    'ads_alert_statistics',
    COUNT(*),
    MAX(stat_time)
FROM ads_alert_statistics
UNION ALL
SELECT 
    'SINK DATABASE SUMMARY',
    'ads_power_quality',
    COUNT(*),
    MAX(analysis_time)
FROM ads_power_quality
UNION ALL
SELECT 
    'SINK DATABASE SUMMARY',
    'ads_risk_assessment',
    COUNT(*),
    MAX(assessment_time)
FROM ads_risk_assessment
UNION ALL
SELECT 
    'SINK DATABASE SUMMARY',
    'ads_energy_efficiency',
    COUNT(*),
    MAX(analysis_time)
FROM ads_energy_efficiency;

-- 2. 验证最新的分析结果
SELECT 
    'LATEST ANALYSIS' as check_type,
    metric_name,
    metric_value,
    metric_unit,
    update_time
FROM ads_realtime_dashboard 
ORDER BY update_time DESC 
LIMIT 10;

-- 3. 验证设备健康度分析
SELECT 
    'EQUIPMENT HEALTH' as check_type,
    equipment_id,
    equipment_name,
    health_score,
    risk_level,
    analysis_time
FROM ads_equipment_health 
ORDER BY analysis_time DESC 
LIMIT 5;

-- 4. 验证告警统计
SELECT 
    'ALERT STATISTICS' as check_type,
    stat_period,
    total_alerts,
    critical_alerts,
    resolved_alerts,
    resolution_rate,
    stat_time
FROM ads_alert_statistics 
ORDER BY stat_time DESC 
LIMIT 5;

-- ========================================
-- 数据延迟检查
-- ========================================

-- 检查数据传输延迟（需要在source和sink都有时间戳的情况下）
SELECT 
    'LATENCY CHECK' as check_type,
    'Data Processing Delay' as metric_name,
    EXTRACT(EPOCH FROM (NOW() - MAX(update_time))) as delay_seconds,
    'seconds' as unit
FROM ads_realtime_dashboard
WHERE update_time IS NOT NULL;

-- ========================================
-- 数据一致性检查
-- ========================================

-- 检查设备数量一致性（需要手动对比source和sink的结果）
SELECT 
    'CONSISTENCY CHECK' as check_type,
    'Equipment Count Comparison' as description,
    'Check if equipment count matches between source and sink' as instruction;

-- 在source端执行：
-- SELECT COUNT(DISTINCT equipment_id) as source_equipment_count FROM equipment_info;

-- 在sink端执行：
-- SELECT COUNT(DISTINCT equipment_id) as sink_equipment_count FROM ads_equipment_health;

-- ========================================
-- 性能监控查询
-- ========================================

-- 查看数据处理吞吐量
SELECT 
    'THROUGHPUT CHECK' as check_type,
    DATE_TRUNC('hour', record_time) as hour_bucket,
    COUNT(*) as records_per_hour
FROM power_consumption 
WHERE record_time > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', record_time)
ORDER BY hour_bucket DESC
LIMIT 24;

-- 查看告警处理效率
SELECT 
    'ALERT EFFICIENCY' as check_type,
    alert_level,
    COUNT(*) as total_alerts,
    COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) as resolved_alerts,
    ROUND(COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) * 100.0 / COUNT(*), 2) as resolution_rate_pct,
    AVG(EXTRACT(EPOCH FROM (resolved_at - alert_time))/60) as avg_resolution_minutes
FROM alert_records 
WHERE alert_time > NOW() - INTERVAL '24 hours'
GROUP BY alert_level
ORDER BY alert_level;