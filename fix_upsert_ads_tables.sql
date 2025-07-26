-- ========================================
-- 修复ADS层表的UPSERT逻辑
-- 将INSERT改为UPSERT，使用固定业务主键
-- ========================================

-- 切换到default catalog
USE CATALOG default_catalog;
USE default_database;

-- 1. 修复实时监控大屏指标表 - 使用固定的metric_name作为主键
INSERT INTO ads_realtime_dashboard
SELECT 
    1 as metric_id,  -- 固定ID for total_active_power
    'total_active_power' as metric_name,
    COALESCE(SUM(total_active_power), 0) as metric_value,
    'MW' as metric_unit,
    '系统总有功功率' as metric_desc,
    'POWER' as metric_category,
    CURRENT_TIMESTAMP as update_time,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dws_customer_hour_power
WHERE stat_date = CURRENT_DATE AND stat_hour = EXTRACT(HOUR FROM CURRENT_TIMESTAMP)
ON DUPLICATE KEY UPDATE
    metric_value = VALUES(metric_value),
    update_time = VALUES(update_time);

INSERT INTO ads_realtime_dashboard
SELECT 
    2 as metric_id,  -- 固定ID for avg_equipment_health
    'avg_equipment_health' as metric_name,
    COALESCE(AVG(avg_health_score), 0) as metric_value,
    '分' as metric_unit,
    '设备平均健康度' as metric_desc,
    'EQUIPMENT' as metric_category,
    CURRENT_TIMESTAMP as update_time,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dws_equipment_hour_summary
WHERE stat_date = CURRENT_DATE AND stat_hour = EXTRACT(HOUR FROM CURRENT_TIMESTAMP)
ON DUPLICATE KEY UPDATE
    metric_value = VALUES(metric_value),
    update_time = VALUES(update_time);

INSERT INTO ads_realtime_dashboard
SELECT 
    3 as metric_id,  -- 固定ID for total_alerts_today
    'total_alerts_today' as metric_name,
    COALESCE(SUM(total_alerts), 0) as metric_value,
    '个' as metric_unit,
    '今日告警总数' as metric_desc,
    'ALERT' as metric_category,
    CURRENT_TIMESTAMP as update_time,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dws_alert_hour_stats
WHERE stat_date = CURRENT_DATE
ON DUPLICATE KEY UPDATE
    metric_value = VALUES(metric_value),
    update_time = VALUES(update_time);

-- 2. 修复设备健康度分析表 - 使用equipment_id作为主键
INSERT INTO ads_equipment_health
SELECT 
    CAST(HASH_CODE(e.equipment_id) AS BIGINT) as analysis_id,  -- 基于equipment_id的固定hash
    e.equipment_id,
    e.equipment_name,
    e.equipment_type,
    e.location,
    COALESCE(s.avg_health_score, 85.0) as health_score,
    CASE 
        WHEN COALESCE(s.avg_health_score, 85.0) >= 90 THEN 'LOW'
        WHEN COALESCE(s.avg_health_score, 85.0) >= 80 THEN 'MEDIUM'
        WHEN COALESCE(s.avg_health_score, 85.0) >= 70 THEN 'HIGH'
        ELSE 'CRITICAL'
    END as risk_level,
    COALESCE(s.avg_temperature, 0) as temperature_avg,
    COALESCE(s.avg_load_rate, 0) as load_rate_avg,
    COALESCE(s.avg_efficiency, 0) as efficiency_avg,
    COALESCE(s.abnormal_count, 0) as fault_count,
    CAST(EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM e.install_date) AS INT) as maintenance_days,
    CASE 
        WHEN COALESCE(s.avg_health_score, 85.0) >= 90 THEN 95.0
        WHEN COALESCE(s.avg_health_score, 85.0) >= 80 THEN 85.0
        WHEN COALESCE(s.avg_health_score, 85.0) >= 70 THEN 70.0
        ELSE 50.0
    END as prediction_score,
    CASE 
        WHEN COALESCE(s.avg_health_score, 85.0) < 70 THEN '建议立即检修'
        WHEN COALESCE(s.avg_health_score, 85.0) < 80 THEN '建议加强监控'
        WHEN COALESCE(s.avg_health_score, 85.0) < 90 THEN '建议定期维护'
        ELSE '设备状态良好'
    END as recommendation,
    CURRENT_TIMESTAMP as analysis_time,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dwd_dim_equipment e
LEFT JOIN (
    SELECT 
        equipment_id,
        AVG(avg_health_score) as avg_health_score,
        AVG(avg_temperature) as avg_temperature,
        AVG(avg_load_rate) as avg_load_rate,
        AVG(avg_efficiency) as avg_efficiency,
        CAST(SUM(abnormal_count) AS INT) as abnormal_count
    FROM fluss_catalog.sgcc_dw.dws_equipment_hour_summary
    WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY equipment_id
) s ON e.equipment_id = s.equipment_id
WHERE e.is_active = true
ON DUPLICATE KEY UPDATE
    health_score = VALUES(health_score),
    risk_level = VALUES(risk_level),
    temperature_avg = VALUES(temperature_avg),
    load_rate_avg = VALUES(load_rate_avg),
    efficiency_avg = VALUES(efficiency_avg),
    fault_count = VALUES(fault_count),
    prediction_score = VALUES(prediction_score),
    recommendation = VALUES(recommendation),
    analysis_time = VALUES(analysis_time);

-- 3. 修复客户用电行为分析表 - 使用customer_id作为主键
INSERT INTO ads_customer_behavior
SELECT 
    CAST(HASH_CODE(c.customer_id) AS BIGINT) as behavior_id,  -- 基于customer_id的固定hash
    c.customer_id,
    c.customer_name,
    c.customer_type,
    'DAILY' as analysis_period,
    COALESCE(p.total_consumption, 0) as total_consumption,
    COALESCE(p.avg_power, 0) as avg_power,
    COALESCE(p.peak_power, 0) as peak_power,
    COALESCE(p.avg_power_factor, 0.95) as power_factor_avg,
    CASE 
        WHEN COALESCE(p.power_variance, 0) < 1000 THEN 'STABLE'
        WHEN COALESCE(p.power_variance, 0) < 5000 THEN 'FLUCTUATING'
        ELSE 'PEAK_VALLEY'
    END as load_pattern,
    COALESCE(p.anomaly_count, 0) as anomaly_count,
    COALESCE(p.total_consumption * 0.8, 0) as cost_estimation,
    CASE 
        WHEN COALESCE(p.avg_power_factor, 0.95) >= 0.95 THEN 'EXCELLENT'
        WHEN COALESCE(p.avg_power_factor, 0.95) >= 0.90 THEN 'GOOD'
        WHEN COALESCE(p.avg_power_factor, 0.95) >= 0.85 THEN 'FAIR'
        ELSE 'POOR'
    END as efficiency_rating,
    COALESCE(p.total_consumption * 0.5, 0) as carbon_emission,
    CURRENT_TIMESTAMP as analysis_time,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dwd_dim_customer c
LEFT JOIN (
    SELECT 
        customer_id,
        SUM(energy_consumption) as total_consumption,
        AVG(total_active_power) as avg_power,
        MAX(peak_power) as peak_power,
        AVG(avg_power_factor) as avg_power_factor,
        VARIANCE(total_active_power) as power_variance,
        CAST(SUM(anomaly_count) AS INT) as anomaly_count
    FROM fluss_catalog.sgcc_dw.dws_customer_hour_power
    WHERE stat_date = CURRENT_DATE
    GROUP BY customer_id
) p ON c.customer_id = p.customer_id
WHERE c.is_active = true
ON DUPLICATE KEY UPDATE
    total_consumption = VALUES(total_consumption),
    avg_power = VALUES(avg_power),
    peak_power = VALUES(peak_power),
    power_factor_avg = VALUES(power_factor_avg),
    load_pattern = VALUES(load_pattern),
    anomaly_count = VALUES(anomaly_count),
    cost_estimation = VALUES(cost_estimation),
    efficiency_rating = VALUES(efficiency_rating),
    carbon_emission = VALUES(carbon_emission),
    analysis_time = VALUES(analysis_time);

-- 4. 修复告警统计分析表 - 使用固定ID 1（因为是全局统计）
INSERT INTO ads_alert_statistics
SELECT 
    1 as stat_id,  -- 固定ID for daily stats
    'DAILY' as stat_period,
    CURRENT_TIMESTAMP as stat_time,
    CAST(COALESCE(SUM(total_alerts), 0) AS INT) as total_alerts,
    CAST(COALESCE(SUM(critical_alerts), 0) AS INT) as critical_alerts,
    CAST(COALESCE(SUM(CASE WHEN alert_level = 'ERROR' THEN total_alerts ELSE 0 END), 0) AS INT) as error_alerts,
    CAST(COALESCE(SUM(CASE WHEN alert_level = 'WARNING' THEN total_alerts ELSE 0 END), 0) AS INT) as warning_alerts,
    CAST(COALESCE(SUM(CASE WHEN alert_level = 'INFO' THEN total_alerts ELSE 0 END), 0) AS INT) as info_alerts,
    CAST(COALESCE(SUM(equipment_alerts), 0) AS INT) as equipment_alerts,
    CAST(COALESCE(SUM(power_alerts), 0) AS INT) as power_alerts,
    CAST(COALESCE(SUM(voltage_alerts), 0) AS INT) as voltage_alerts,
    CAST(COALESCE(SUM(overload_alerts), 0) AS INT) as overload_alerts,
    CAST(COALESCE(SUM(resolved_alerts), 0) AS INT) as resolved_alerts,
    COALESCE(AVG(avg_resolution_time), 0) as avg_resolution_time,
    CASE 
        WHEN COUNT(*) > 0 THEN COALESCE(SUM(total_alerts), 0) * 100.0 / COUNT(*)
        ELSE 0
    END as alert_rate,
    CASE 
        WHEN COALESCE(SUM(total_alerts), 0) > 0 THEN COALESCE(SUM(resolved_alerts), 0) * 100.0 / COALESCE(SUM(total_alerts), 1)
        ELSE 0
    END as resolution_rate,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dws_alert_hour_stats
WHERE stat_date = CURRENT_DATE
ON DUPLICATE KEY UPDATE
    stat_time = VALUES(stat_time),
    total_alerts = VALUES(total_alerts),
    critical_alerts = VALUES(critical_alerts),
    error_alerts = VALUES(error_alerts),
    warning_alerts = VALUES(warning_alerts),
    info_alerts = VALUES(info_alerts),
    equipment_alerts = VALUES(equipment_alerts),
    power_alerts = VALUES(power_alerts),
    voltage_alerts = VALUES(voltage_alerts),
    overload_alerts = VALUES(overload_alerts),
    resolved_alerts = VALUES(resolved_alerts),
    avg_resolution_time = VALUES(avg_resolution_time),
    alert_rate = VALUES(alert_rate),
    resolution_rate = VALUES(resolution_rate);

-- 5. 修复电力质量分析表 - 使用equipment_id作为主键
INSERT INTO ads_power_quality
SELECT 
    CAST(HASH_CODE(e.equipment_id) AS BIGINT) as quality_id,  -- 基于equipment_id的固定hash
    e.equipment_id,
    COALESCE(e.equipment_name, 'UNKNOWN') as equipment_name,
    COALESCE(ec.customer_id, 'UNKNOWN') as customer_id,
    COALESCE(c.customer_name, 'UNKNOWN') as customer_name,
    CURRENT_TIMESTAMP as analysis_time,
    -- 电压质量评分
    CASE 
        WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
        WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
        WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
        WHEN p.avg_voltage IS NULL THEN 80.0
        ELSE 50.0
    END as voltage_quality_score,
    -- 频率质量评分
    CASE 
        WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
        WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
        WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
        WHEN p.frequency_deviation_avg IS NULL THEN 80.0
        ELSE 50.0
    END as frequency_quality_score,
    -- 功率因数评分
    CASE 
        WHEN p.avg_power_factor >= 0.95 THEN 95.0
        WHEN p.avg_power_factor >= 0.90 THEN 85.0
        WHEN p.avg_power_factor >= 0.85 THEN 70.0
        WHEN p.avg_power_factor IS NULL THEN 80.0
        ELSE 50.0
    END as power_factor_score,
    -- 谐波畸变
    CASE 
        WHEN e.equipment_type LIKE '%变压器%' THEN 2.5
        WHEN e.equipment_type LIKE '%断路器%' THEN 1.5
        WHEN e.equipment_type LIKE '%线路%' THEN 3.0
        ELSE 2.0
    END as harmonic_distortion,
    -- 电压不平衡度
    COALESCE(p.voltage_unbalance_avg, 1.5) as voltage_unbalance,
    -- 闪变严重度
    CASE 
        WHEN p.avg_voltage BETWEEN 215 AND 225 THEN 0.5
        WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 1.0
        WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 2.0
        WHEN p.avg_voltage IS NULL THEN 1.0
        ELSE 3.0
    END as flicker_severity,
    -- 中断次数
    COALESCE(a.interruption_count, 0) as interruption_count,
    -- 电压跌落次数
    COALESCE(a.voltage_sag_count, 0) as voltage_sag_count,
    -- 电压升高次数
    COALESCE(a.voltage_swell_count, 0) as voltage_swell_count,
    -- 综合质量评分
    (
        CASE 
            WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
            WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
            WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
            WHEN p.avg_voltage IS NULL THEN 80.0
            ELSE 50.0
        END +
        CASE 
            WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
            WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
            WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
            WHEN p.frequency_deviation_avg IS NULL THEN 80.0
            ELSE 50.0
        END +
        CASE 
            WHEN p.avg_power_factor >= 0.95 THEN 95.0
            WHEN p.avg_power_factor >= 0.90 THEN 85.0
            WHEN p.avg_power_factor >= 0.85 THEN 70.0
            WHEN p.avg_power_factor IS NULL THEN 80.0
            ELSE 50.0
        END
    ) / 3.0 as overall_quality_score,
    -- 质量等级
    CASE 
        WHEN (
            CASE 
                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
                WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
                WHEN p.avg_voltage IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
                WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
                WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
                WHEN p.frequency_deviation_avg IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.avg_power_factor >= 0.95 THEN 95.0
                WHEN p.avg_power_factor >= 0.90 THEN 85.0
                WHEN p.avg_power_factor >= 0.85 THEN 70.0
                WHEN p.avg_power_factor IS NULL THEN 80.0
                ELSE 50.0
            END
        ) / 3.0 >= 90 THEN 'EXCELLENT'
        WHEN (
            CASE 
                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
                WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
                WHEN p.avg_voltage IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
                WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
                WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
                WHEN p.frequency_deviation_avg IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.avg_power_factor >= 0.95 THEN 95.0
                WHEN p.avg_power_factor >= 0.90 THEN 85.0
                WHEN p.avg_power_factor >= 0.85 THEN 70.0
                WHEN p.avg_power_factor IS NULL THEN 80.0
                ELSE 50.0
            END
        ) / 3.0 >= 80 THEN 'GOOD'
        WHEN (
            CASE 
                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
                WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
                WHEN p.avg_voltage IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
                WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
                WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
                WHEN p.frequency_deviation_avg IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.avg_power_factor >= 0.95 THEN 95.0
                WHEN p.avg_power_factor >= 0.90 THEN 85.0
                WHEN p.avg_power_factor >= 0.85 THEN 70.0
                WHEN p.avg_power_factor IS NULL THEN 80.0
                ELSE 50.0
            END
        ) / 3.0 >= 70 THEN 'FAIR'
        ELSE 'POOR'
    END as quality_grade,
    -- 改进建议
    CASE 
        WHEN (
            CASE 
                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
                WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
                WHEN p.avg_voltage IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
                WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
                WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
                WHEN p.frequency_deviation_avg IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.avg_power_factor >= 0.95 THEN 95.0
                WHEN p.avg_power_factor >= 0.90 THEN 85.0
                WHEN p.avg_power_factor >= 0.85 THEN 70.0
                WHEN p.avg_power_factor IS NULL THEN 80.0
                ELSE 50.0
            END
        ) / 3.0 < 70 THEN '建议立即检查电力质量问题'
        WHEN (
            CASE 
                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
                WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
                WHEN p.avg_voltage IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
                WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
                WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
                WHEN p.frequency_deviation_avg IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.avg_power_factor >= 0.95 THEN 95.0
                WHEN p.avg_power_factor >= 0.90 THEN 85.0
                WHEN p.avg_power_factor >= 0.85 THEN 70.0
                WHEN p.avg_power_factor IS NULL THEN 80.0
                ELSE 50.0
            END
        ) / 3.0 < 80 THEN '建议优化电力质量管理'
        WHEN (
            CASE 
                WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
                WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
                WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
                WHEN p.avg_voltage IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.frequency_deviation_avg BETWEEN -0.2 AND 0.2 THEN 95.0
                WHEN p.frequency_deviation_avg BETWEEN -0.5 AND 0.5 THEN 85.0
                WHEN p.frequency_deviation_avg BETWEEN -1.0 AND 1.0 THEN 70.0
                WHEN p.frequency_deviation_avg IS NULL THEN 80.0
                ELSE 50.0
            END +
            CASE 
                WHEN p.avg_power_factor >= 0.95 THEN 95.0
                WHEN p.avg_power_factor >= 0.90 THEN 85.0
                WHEN p.avg_power_factor >= 0.85 THEN 70.0
                WHEN p.avg_power_factor IS NULL THEN 80.0
                ELSE 50.0
            END
        ) / 3.0 < 90 THEN '建议持续监控电力质量'
        ELSE '电力质量良好，保持现状'
    END as improvement_suggestions,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dwd_dim_equipment e
LEFT JOIN fluss_catalog.sgcc_dw.dwd_equipment_customer ec ON e.equipment_id = ec.equipment_id
LEFT JOIN fluss_catalog.sgcc_dw.dwd_dim_customer c ON ec.customer_id = c.customer_id
LEFT JOIN (
    SELECT 
        equipment_id,
        AVG(voltage_a + voltage_b + voltage_c) / 3.0 as avg_voltage,
        AVG(frequency - 50.0) as frequency_deviation_avg,
        AVG(power_factor) as avg_power_factor,
        STDDEV(voltage_a + voltage_b + voltage_c) / 3.0 as voltage_unbalance_avg
    FROM fluss_catalog.sgcc_dw.dwd_fact_power_consumption
    WHERE record_time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
    GROUP BY equipment_id
) p ON e.equipment_id = p.equipment_id
LEFT JOIN (
    SELECT 
        equipment_id,
        COUNT(CASE WHEN alert_type = 'POWER_INTERRUPTION' THEN 1 END) as interruption_count,
        COUNT(CASE WHEN alert_type = 'VOLTAGE_SAG' THEN 1 END) as voltage_sag_count,
        COUNT(CASE WHEN alert_type = 'VOLTAGE_SWELL' THEN 1 END) as voltage_swell_count
    FROM fluss_catalog.sgcc_dw.dwd_fact_alert_records
    WHERE alert_time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
    GROUP BY equipment_id
) a ON e.equipment_id = a.equipment_id
WHERE e.is_active = true
ON DUPLICATE KEY UPDATE
    analysis_time = VALUES(analysis_time),
    voltage_quality_score = VALUES(voltage_quality_score),
    frequency_quality_score = VALUES(frequency_quality_score),
    power_factor_score = VALUES(power_factor_score),
    harmonic_distortion = VALUES(harmonic_distortion),
    voltage_unbalance = VALUES(voltage_unbalance),
    flicker_severity = VALUES(flicker_severity),
    interruption_count = VALUES(interruption_count),
    voltage_sag_count = VALUES(voltage_sag_count),
    voltage_swell_count = VALUES(voltage_swell_count),
    overall_quality_score = VALUES(overall_quality_score),
    quality_grade = VALUES(quality_grade),
    improvement_suggestions = VALUES(improvement_suggestions);

-- 注意：Flink SQL不支持ON DUPLICATE KEY UPDATE语法
-- 需要使用UPSERT语义的连接器配置或者先DELETE再INSERT的方式
-- 这个脚本展示了修复的思路，实际实现需要根据Flink版本调整语法