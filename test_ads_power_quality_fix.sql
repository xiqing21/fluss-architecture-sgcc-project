-- ========================================
-- 测试 ads_power_quality 表数据修复
-- ========================================

-- 1. 首先检查基础表是否有数据
SELECT 'dwd_dim_equipment' as table_name, COUNT(*) as record_count FROM fluss_catalog.sgcc_dw.dwd_dim_equipment
UNION ALL
SELECT 'dwd_dim_customer' as table_name, COUNT(*) as record_count FROM fluss_catalog.sgcc_dw.dwd_dim_customer
UNION ALL
SELECT 'dwd_fact_power_consumption' as table_name, COUNT(*) as record_count FROM fluss_catalog.sgcc_dw.dwd_fact_power_consumption
UNION ALL
SELECT 'dws_customer_hour_power' as table_name, COUNT(*) as record_count FROM fluss_catalog.sgcc_dw.dws_customer_hour_power
UNION ALL
SELECT 'dws_alert_hour_stats' as table_name, COUNT(*) as record_count FROM fluss_catalog.sgcc_dw.dws_alert_hour_stats;

-- 2. 检查设备和客户的关联情况
SELECT 
    'equipment_customer_mapping' as check_name,
    COUNT(DISTINCT e.equipment_id) as total_equipment,
    COUNT(DISTINCT ec.equipment_id) as equipment_with_customer,
    COUNT(DISTINCT ec.customer_id) as unique_customers
FROM fluss_catalog.sgcc_dw.dwd_dim_equipment e
LEFT JOIN (
    SELECT DISTINCT equipment_id, customer_id
    FROM fluss_catalog.sgcc_dw.dwd_fact_power_consumption
) ec ON e.equipment_id = ec.equipment_id;

-- 3. 清空 ads_power_quality 表（如果存在数据）
DELETE FROM default_catalog.default_database.ads_power_quality;

-- 4. 重新执行 ads_power_quality 的插入语句
-- 切换到 default catalog
USE CATALOG default_catalog;
USE default_database;

INSERT INTO ads_power_quality
SELECT 
    CAST(HASH_CODE(CONCAT(e.equipment_id, CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as quality_id,
    e.equipment_id,
    COALESCE(e.equipment_name, 'UNKNOWN') as equipment_name,
    COALESCE(ec.customer_id, 'UNKNOWN') as customer_id,
    COALESCE(c.customer_name, 'UNKNOWN') as customer_name,
    CURRENT_TIMESTAMP as analysis_time,
    -- 电压质量评分（基于电压数据）
    CASE 
        WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 95.0
        WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 85.0
        WHEN p.avg_voltage BETWEEN 190 AND 250 THEN 70.0
        WHEN p.avg_voltage IS NULL THEN 80.0
        ELSE 50.0
    END as voltage_quality_score,
    -- 频率质量评分（基于频率偏差）
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
    -- 谐波畸变评分（基于设备类型估算）
    CASE 
        WHEN e.equipment_type LIKE '%变压器%' THEN 90.0
        WHEN e.equipment_type LIKE '%断路器%' THEN 95.0
        WHEN e.equipment_type LIKE '%线路%' THEN 85.0
        ELSE 80.0
    END as harmonic_distortion,
    -- 电压不平衡度评分
    CASE 
        WHEN p.voltage_unbalance_avg <= 2.0 THEN 95.0
        WHEN p.voltage_unbalance_avg <= 4.0 THEN 85.0
        WHEN p.voltage_unbalance_avg <= 6.0 THEN 70.0
        WHEN p.voltage_unbalance_avg IS NULL THEN 80.0
        ELSE 50.0
    END as voltage_unbalance,
    -- 闪变严重度评分（基于电压稳定性）
    CASE 
        WHEN p.avg_voltage BETWEEN 215 AND 225 THEN 95.0
        WHEN p.avg_voltage BETWEEN 210 AND 230 THEN 85.0
        WHEN p.avg_voltage BETWEEN 200 AND 240 THEN 70.0
        WHEN p.avg_voltage IS NULL THEN 80.0
        ELSE 50.0
    END as flicker_severity,
    -- 中断次数（基于告警数据）
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
        ) / 3.0 < 70 THEN '建议检查电力质量设备，优化电网参数'
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
        ) / 3.0 < 80 THEN '建议定期监控电力质量指标'
        ELSE '电力质量良好，保持现状'
    END as improvement_suggestions,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dwd_dim_equipment e
LEFT JOIN (
    -- 通过用电数据获取设备对应的客户ID
    SELECT DISTINCT equipment_id, customer_id
    FROM fluss_catalog.sgcc_dw.dwd_fact_power_consumption
    -- 移除时间限制以获取所有数据
) ec ON e.equipment_id = ec.equipment_id
LEFT JOIN fluss_catalog.sgcc_dw.dwd_dim_customer c ON ec.customer_id = c.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        AVG(avg_voltage) as avg_voltage,
        AVG(avg_power_factor) as avg_power_factor,
        AVG(voltage_unbalance_avg) as voltage_unbalance_avg,
        AVG(frequency_deviation_avg) as frequency_deviation_avg
    FROM fluss_catalog.sgcc_dw.dws_customer_hour_power
    -- 移除时间限制以获取所有数据
    GROUP BY customer_id
) p ON ec.customer_id = p.customer_id
LEFT JOIN (
    SELECT 
        equipment_id,
        SUM(CASE WHEN alert_type = 'POWER_INTERRUPTION' THEN total_alerts ELSE 0 END) as interruption_count,
        SUM(CASE WHEN alert_type = 'VOLTAGE_SAG' THEN total_alerts ELSE 0 END) as voltage_sag_count,
        SUM(CASE WHEN alert_type = 'VOLTAGE_SWELL' THEN total_alerts ELSE 0 END) as voltage_swell_count
    FROM fluss_catalog.sgcc_dw.dws_alert_hour_stats
    -- 移除时间限制以获取所有数据
    GROUP BY equipment_id
) a ON e.equipment_id = a.equipment_id
WHERE e.is_active = true;

-- 5. 检查插入后的数据量
SELECT 'ads_power_quality_after_insert' as table_name, COUNT(*) as record_count 
FROM ads_power_quality;

-- 6. 查看前5条记录示例
SELECT 
    quality_id,
    equipment_id,
    equipment_name,
    customer_id,
    customer_name,
    voltage_quality_score,
    frequency_quality_score,
    power_factor_score,
    overall_quality_score,
    quality_grade,
    improvement_suggestions
FROM ads_power_quality
LIMIT 5;

-- 7. 按质量等级统计
SELECT 
    quality_grade,
    COUNT(*) as count,
    AVG(overall_quality_score) as avg_score
FROM ads_power_quality
GROUP BY quality_grade
ORDER BY avg_score DESC;