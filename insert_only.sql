-- ========================================
-- SGCC 数仓 INSERT 脚本 (只执行数据插入)
-- 前提：已执行ddl_only.sql创建所有表结构
-- ========================================

-- 设置执行环境
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

-- 创建 Fluss Catalog（如果不存在）
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- ========================================
-- 第一步：在default catalog创建CDC源表和JDBC sink表
-- （这些表无法持久化，每次执行都需要重新创建）
-- ========================================

USE CATALOG default_catalog;
USE default_database;

-- 1.1 创建设备信息源表
CREATE TABLE IF NOT EXISTS ods_equipment_info (
    equipment_id STRING,
    equipment_name STRING,
    equipment_type STRING,
    location STRING,
    voltage_level INT,
    capacity DECIMAL(10,2),
    manufacturer STRING,
    install_date DATE,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (equipment_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'equipment_info',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'equipment_info_slot'
);

-- 1.2 创建客户信息源表
CREATE TABLE IF NOT EXISTS ods_customer_info (
    customer_id STRING,
    customer_name STRING,
    customer_type STRING,
    contact_person STRING,
    contact_phone STRING,
    address STRING,
    contract_capacity DECIMAL(10,2),
    voltage_level INT,
    tariff_type STRING,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'customer_info',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'customer_info_slot'
);

-- 1.3 创建用电数据源表
CREATE TABLE IF NOT EXISTS ods_power_consumption (
    consumption_id BIGINT,
    customer_id STRING,
    equipment_id STRING,
    record_time TIMESTAMP(3),
    active_power DECIMAL(10,4),
    reactive_power DECIMAL(10,4),
    voltage_a DECIMAL(8,2),
    voltage_b DECIMAL(8,2),
    voltage_c DECIMAL(8,2),
    current_a DECIMAL(8,2),
    current_b DECIMAL(8,2),
    current_c DECIMAL(8,2),
    power_factor DECIMAL(4,3),
    frequency DECIMAL(5,2),
    energy_consumption DECIMAL(12,4),
    created_at TIMESTAMP(3),
    PRIMARY KEY (consumption_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'power_consumption',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'power_consumption_slot'
);

-- 1.4 创建设备状态源表
CREATE TABLE IF NOT EXISTS ods_equipment_status (
    status_id BIGINT,
    equipment_id STRING,
    status_time TIMESTAMP(3),
    operating_status STRING,
    temperature DECIMAL(5,2),
    pressure DECIMAL(8,2),
    vibration DECIMAL(6,3),
    oil_level DECIMAL(5,2),
    insulation_resistance DECIMAL(10,2),
    load_rate DECIMAL(5,2),
    efficiency DECIMAL(5,2),
    health_score DECIMAL(5,2),
    risk_level STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (status_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'equipment_status',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'equipment_status_slot'
);

-- 1.5 创建告警记录源表
CREATE TABLE IF NOT EXISTS ods_alert_records (
    alert_id BIGINT,
    equipment_id STRING,
    customer_id STRING,
    alert_type STRING,
    alert_level STRING,
    alert_title STRING,
    alert_description STRING,
    alert_time TIMESTAMP(3),
    alert_value DECIMAL(12,4),
    threshold_value DECIMAL(12,4),
    status STRING,
    acknowledged_by STRING,
    acknowledged_at TIMESTAMP(3),
    resolved_by STRING,
    resolved_at TIMESTAMP(3),
    resolution_notes STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'alert_records',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'alert_records_slot'
);

-- 1.6 创建ADS电能质量分析表（JDBC sink）
CREATE TABLE IF NOT EXISTS ads_power_quality (
    quality_id BIGINT,
    equipment_id STRING,
    equipment_name STRING,
    customer_id STRING,
    customer_name STRING,
    analysis_time TIMESTAMP(3),
    voltage_stability DECIMAL(5,2),
    frequency_stability DECIMAL(5,2),
    power_factor_quality DECIMAL(5,2),
    harmonic_distortion DECIMAL(5,2),
    voltage_unbalance DECIMAL(5,2),
    flicker_severity DECIMAL(5,2),
    interruption_count INT,
    sag_count INT,
    swell_count INT,
    overall_quality DECIMAL(5,2),
    quality_grade STRING,
    improvement_suggestions STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (quality_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'table-name' = 'ads_power_quality'
);

-- ========================================
-- 第二步：切换到Fluss catalog进行数据处理
-- ========================================

-- 切换到Fluss catalog
USE CATALOG fluss_catalog;
USE sgcc_dw;

-- ========================================
-- 第三步：DWD层数据清洗和标准化
-- ========================================

-- 1.1 插入设备维度数据
INSERT INTO dwd_dim_equipment
SELECT 
    equipment_id,
    equipment_name,
    equipment_type,
    CASE 
        WHEN equipment_type LIKE '%变压器%' OR equipment_type LIKE '%TRANSFORMER%' THEN 'TRANSFORMER'
        WHEN equipment_type LIKE '%开关%' OR equipment_type LIKE '%BREAKER%' THEN 'BREAKER'
        WHEN equipment_type LIKE '%线路%' OR equipment_type LIKE '%LINE%' THEN 'LINE'
        ELSE 'OTHER'
    END as equipment_category,
    location,
    voltage_level,
    CASE 
        WHEN voltage_level >= 220 THEN 'HIGH'
        WHEN voltage_level >= 35 THEN 'MEDIUM'
        ELSE 'LOW'
    END as voltage_category,
    capacity,
    manufacturer,
    install_date,
    CAST(YEAR(install_date) AS INT) as install_year,
    CAST(YEAR(CURRENT_DATE) - YEAR(install_date) AS INT) as service_years,
    status,
    CASE WHEN status = 'ACTIVE' THEN true ELSE false END as is_active,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as etl_time
FROM default_catalog.default_database.ods_equipment_info
WHERE equipment_id IS NOT NULL;

-- 1.2 插入客户维度数据
INSERT INTO dwd_dim_customer
SELECT 
    customer_id,
    customer_name,
    customer_type,
    CASE 
        WHEN customer_type = 'INDUSTRIAL' AND contract_capacity > 1000 THEN 'LARGE_INDUSTRIAL'
        WHEN customer_type = 'INDUSTRIAL' THEN 'SMALL_INDUSTRIAL'
        WHEN customer_type = 'COMMERCIAL' THEN 'COMMERCIAL'
        ELSE 'RESIDENTIAL'
    END as customer_category,
    contact_person,
    contact_phone,
    address,
    CASE 
        WHEN address LIKE '%北京%' THEN '北京'
        WHEN address LIKE '%上海%' THEN '上海'
        WHEN address LIKE '%广州%' THEN '广州'
        WHEN address LIKE '%深圳%' THEN '深圳'
        ELSE '其他'
    END as region,
    contract_capacity,
    CASE 
        WHEN contract_capacity > 1000 THEN 'LARGE'
        WHEN contract_capacity > 100 THEN 'MEDIUM'
        ELSE 'SMALL'
    END as capacity_category,
    voltage_level,
    tariff_type,
    status,
    CASE WHEN status = 'ACTIVE' THEN true ELSE false END as is_active,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as etl_time
FROM default_catalog.default_database.ods_customer_info
WHERE customer_id IS NOT NULL;

-- 1.3 插入用电事实数据
INSERT INTO dwd_fact_power_consumption
SELECT 
    consumption_id,
    customer_id,
    equipment_id,
    record_time,
    DATE(record_time) as record_date,
    HOUR(record_time) as record_hour,
    active_power,
    reactive_power,
    SQRT(active_power * active_power + reactive_power * reactive_power) as apparent_power,
    voltage_a,
    voltage_b,
    voltage_c,
    (voltage_a + voltage_b + voltage_c) / 3 as avg_voltage,
    current_a,
    current_b,
    current_c,
    (current_a + current_b + current_c) / 3 as avg_current,
    power_factor,
    CASE 
        WHEN power_factor >= 0.95 THEN 'EXCELLENT'
        WHEN power_factor >= 0.85 THEN 'GOOD'
        WHEN power_factor >= 0.7 THEN 'FAIR'
        ELSE 'POOR'
    END as power_factor_grade,
    frequency,
    ABS(frequency - 50.0) as frequency_deviation,
    energy_consumption,
    CASE 
        WHEN active_power > 0 THEN energy_consumption / active_power
        ELSE 0
    END as load_duration,
    created_at,
    CURRENT_TIMESTAMP as etl_time
FROM default_catalog.default_database.ods_power_consumption
WHERE consumption_id IS NOT NULL
  AND record_time IS NOT NULL;

-- ========================================
-- 第四步：DWS层数据聚合
-- ========================================

-- 2.1 客户小时用电汇总
INSERT INTO dws_customer_hour_power
SELECT 
    ROW_NUMBER() OVER (ORDER BY customer_id, stat_date, stat_hour) as id,
    customer_id,
    stat_date,
    stat_hour,
    total_active_power,
    total_reactive_power,
    total_apparent_power,
    avg_voltage,
    avg_current,
    avg_power_factor,
    avg_frequency,
    total_energy_consumption,
    peak_active_power,
    min_active_power,
    power_factor_qualified_rate,
    voltage_qualified_rate,
    frequency_qualified_rate,
    load_rate,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM (
    SELECT 
        customer_id,
        record_date as stat_date,
        record_hour as stat_hour,
        SUM(active_power) as total_active_power,
        SUM(reactive_power) as total_reactive_power,
        SUM(apparent_power) as total_apparent_power,
        AVG(avg_voltage) as avg_voltage,
        AVG(avg_current) as avg_current,
        AVG(power_factor) as avg_power_factor,
        AVG(frequency) as avg_frequency,
        SUM(energy_consumption) as total_energy_consumption,
        MAX(active_power) as peak_active_power,
        MIN(active_power) as min_active_power,
        AVG(CASE WHEN power_factor >= 0.85 THEN 1.0 ELSE 0.0 END) as power_factor_qualified_rate,
        AVG(CASE WHEN avg_voltage BETWEEN 200 AND 240 THEN 1.0 ELSE 0.0 END) as voltage_qualified_rate,
        AVG(CASE WHEN ABS(frequency - 50.0) <= 0.5 THEN 1.0 ELSE 0.0 END) as frequency_qualified_rate,
        AVG(active_power) / MAX(active_power) as load_rate
    FROM dwd_fact_power_consumption
    WHERE record_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    GROUP BY customer_id, record_date, record_hour
) t;

-- ========================================
-- 第五步：ADS层电能质量分析
-- ========================================

-- 切换到default catalog执行ADS插入
USE CATALOG default_catalog;
USE default_database;

-- 插入电能质量分析数据
INSERT INTO ads_power_quality
SELECT 
    ROW_NUMBER() OVER (ORDER BY e.equipment_id) as quality_id,
    e.equipment_id,
    e.equipment_name,
    COALESCE(c.customer_id, 'UNKNOWN') as customer_id,
    COALESCE(c.customer_name, 'Unknown Customer') as customer_name,
    CURRENT_TIMESTAMP as analysis_time,
    -- 电压稳定性（基于电压合格率）
    CAST(COALESCE(p.voltage_qualified_rate * 100, 85.0) AS DECIMAL(5,2)) as voltage_stability,
    -- 频率稳定性（基于频率合格率）
    CAST(COALESCE(p.frequency_qualified_rate * 100, 90.0) AS DECIMAL(5,2)) as frequency_stability,
    -- 功率因数质量（基于功率因数合格率）
    CAST(COALESCE(p.power_factor_qualified_rate * 100, 88.0) AS DECIMAL(5,2)) as power_factor_quality,
    -- 谐波畸变率（模拟计算）
    CAST(2.0 + (RAND() * 3) AS DECIMAL(5,2)) as harmonic_distortion,
    -- 电压不平衡度（模拟计算）
    CAST(0.5 + (RAND() * 2) AS DECIMAL(5,2)) as voltage_unbalance,
    -- 闪变严重度（模拟计算）
    CAST(0.3 + (RAND() * 1) AS DECIMAL(5,2)) as flicker_severity,
    -- 中断次数（基于告警统计）
    COALESCE(a.interruption_count, 0) as interruption_count,
    -- 电压暂降次数（基于告警统计）
    COALESCE(a.sag_count, 0) as sag_count,
    -- 电压暂升次数（基于告警统计）
    COALESCE(a.swell_count, 0) as swell_count,
    -- 综合质量评分
    CAST((
        COALESCE(p.voltage_qualified_rate * 100, 85.0) * 0.3 +
        COALESCE(p.frequency_qualified_rate * 100, 90.0) * 0.2 +
        COALESCE(p.power_factor_qualified_rate * 100, 88.0) * 0.3 +
        (100 - GREATEST(COALESCE(a.interruption_count, 0) * 5, 20)) * 0.2
    ) AS DECIMAL(5,2)) as overall_quality,
    -- 质量等级
    CASE 
        WHEN (
            COALESCE(p.voltage_qualified_rate * 100, 85.0) * 0.3 +
            COALESCE(p.frequency_qualified_rate * 100, 90.0) * 0.2 +
            COALESCE(p.power_factor_qualified_rate * 100, 88.0) * 0.3 +
            (100 - GREATEST(COALESCE(a.interruption_count, 0) * 5, 20)) * 0.2
        ) >= 95 THEN 'EXCELLENT'
        WHEN (
            COALESCE(p.voltage_qualified_rate * 100, 85.0) * 0.3 +
            COALESCE(p.frequency_qualified_rate * 100, 90.0) * 0.2 +
            COALESCE(p.power_factor_qualified_rate * 100, 88.0) * 0.3 +
            (100 - GREATEST(COALESCE(a.interruption_count, 0) * 5, 20)) * 0.2
        ) >= 85 THEN 'GOOD'
        WHEN (
            COALESCE(p.voltage_qualified_rate * 100, 85.0) * 0.3 +
            COALESCE(p.frequency_qualified_rate * 100, 90.0) * 0.2 +
            COALESCE(p.power_factor_qualified_rate * 100, 88.0) * 0.3 +
            (100 - GREATEST(COALESCE(a.interruption_count, 0) * 5, 20)) * 0.2
        ) >= 70 THEN 'FAIR'
        ELSE 'POOR'
    END as quality_grade,
    -- 改进建议
    CASE 
        WHEN COALESCE(p.power_factor_qualified_rate, 0.85) < 0.8 THEN '建议安装无功补偿装置提高功率因数'
        WHEN COALESCE(p.voltage_qualified_rate, 0.85) < 0.9 THEN '建议检查电压调节设备，优化电压质量'
        WHEN COALESCE(a.interruption_count, 0) > 2 THEN '建议加强设备维护，减少供电中断'
        ELSE '电能质量良好，继续保持现有运行状态'
    END as improvement_suggestions,
    CURRENT_TIMESTAMP as created_at
FROM fluss_catalog.sgcc_dw.dwd_dim_equipment e
LEFT JOIN (
    SELECT DISTINCT equipment_id, customer_id
    FROM fluss_catalog.sgcc_dw.dwd_fact_power_consumption
    WHERE record_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
) pc ON e.equipment_id = pc.equipment_id
LEFT JOIN fluss_catalog.sgcc_dw.dwd_dim_customer c ON pc.customer_id = c.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        AVG(voltage_qualified_rate) as voltage_qualified_rate,
        AVG(frequency_qualified_rate) as frequency_qualified_rate,
        AVG(power_factor_qualified_rate) as power_factor_qualified_rate
    FROM fluss_catalog.sgcc_dw.dws_customer_hour_power
    WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY customer_id
) p ON pc.customer_id = p.customer_id
LEFT JOIN (
    SELECT 
        equipment_id,
        COUNT(CASE WHEN alert_type = 'POWER_INTERRUPTION' THEN 1 END) as interruption_count,
        COUNT(CASE WHEN alert_type = 'VOLTAGE_SAG' THEN 1 END) as sag_count,
        COUNT(CASE WHEN alert_type = 'VOLTAGE_SWELL' THEN 1 END) as swell_count
    FROM fluss_catalog.sgcc_dw.dwd_fact_alert
    WHERE alert_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    GROUP BY equipment_id
) a ON e.equipment_id = a.equipment_id
WHERE e.is_active = true
LIMIT 100;

-- 显示执行结果
SELECT 'INSERT脚本执行完成，数据已插入到各层表中' as status;
SELECT COUNT(*) as ads_power_quality_count FROM ads_power_quality;