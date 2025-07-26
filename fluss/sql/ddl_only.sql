-- ========================================
-- SGCC 数仓 DDL 脚本 (只创建表结构)
-- 用于初始化所有表定义，支持重复执行
-- ========================================

-- 设置执行环境
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

-- 创建 Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- ========================================
-- 切换到Fluss catalog（表结构会持久化保存）
-- ========================================

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS sgcc_dw;
USE sgcc_dw;

-- ========================================
-- DWD层表定义（数据清洗和标准化层）
-- ========================================

-- 2.1 DWD设备维度表
CREATE TABLE IF NOT EXISTS dwd_dim_equipment (
    equipment_id STRING,
    equipment_name STRING,
    equipment_type STRING,
    equipment_category STRING, -- 根据type分类：TRANSFORMER, BREAKER, LINE等
    location STRING,
    voltage_level INT,
    voltage_category STRING, -- 根据电压等级分类：HIGH, MEDIUM, LOW
    capacity DECIMAL(10,2),
    manufacturer STRING,
    install_date DATE,
    install_year INT,
    service_years INT, -- 服役年限
    status STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),
    PRIMARY KEY (equipment_id) NOT ENFORCED
);

-- 2.2 DWD客户维度表
CREATE TABLE IF NOT EXISTS dwd_dim_customer (
    customer_id STRING,
    customer_name STRING,
    customer_type STRING,
    customer_category STRING, -- 根据type和capacity分类
    contact_person STRING,
    contact_phone STRING,
    address STRING,
    region STRING, -- 从地址提取区域信息
    contract_capacity DECIMAL(10,2),
    capacity_level STRING, -- 根据容量分级：LARGE, MEDIUM, SMALL
    voltage_level INT,
    tariff_type STRING,
    status STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
);

-- 2.3 DWD用电事实表
CREATE TABLE IF NOT EXISTS dwd_fact_power_consumption (
    consumption_id BIGINT,
    customer_id STRING,
    equipment_id STRING,
    record_time TIMESTAMP(3),
    record_date DATE,
    record_hour INT,
    active_power DECIMAL(10,4),
    reactive_power DECIMAL(10,4),
    apparent_power DECIMAL(10,4), -- 视在功率，计算得出
    voltage_avg DECIMAL(8,2), -- 三相电压平均值
    voltage_unbalance DECIMAL(5,2), -- 电压不平衡度
    current_avg DECIMAL(8,2), -- 三相电流平均值
    current_unbalance DECIMAL(5,2), -- 电流不平衡度
    power_factor DECIMAL(4,3),
    power_factor_grade STRING, -- 功率因数等级
    frequency DECIMAL(5,2),
    frequency_deviation DECIMAL(5,2), -- 频率偏差
    energy_consumption DECIMAL(12,4),
    load_rate DECIMAL(5,2), -- 负载率
    is_peak_hour BOOLEAN, -- 是否峰时
    is_anomaly BOOLEAN, -- 是否异常数据
    data_quality_score DECIMAL(3,2), -- 数据质量评分
    created_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),
    PRIMARY KEY (consumption_id) NOT ENFORCED
);

-- 2.4 DWD设备状态事实表
CREATE TABLE IF NOT EXISTS dwd_fact_equipment_status (
    status_id BIGINT,
    equipment_id STRING,
    status_time TIMESTAMP(3),
    status_date DATE,
    status_hour INT,
    operating_status STRING,
    temperature DECIMAL(5,2),
    temperature_grade STRING, -- 温度等级
    pressure DECIMAL(8,2),
    pressure_grade STRING, -- 压力等级
    vibration DECIMAL(6,3),
    vibration_grade STRING, -- 振动等级
    oil_level DECIMAL(5,2),
    oil_level_grade STRING, -- 油位等级
    insulation_resistance DECIMAL(10,2),
    insulation_grade STRING, -- 绝缘等级
    load_rate DECIMAL(5,2),
    load_grade STRING, -- 负载等级
    efficiency DECIMAL(5,2),
    efficiency_grade STRING, -- 效率等级
    health_score DECIMAL(5,2),
    health_grade STRING, -- 健康度等级
    risk_level STRING,
    risk_score DECIMAL(5,2), -- 风险评分
    is_abnormal BOOLEAN, -- 是否异常状态
    created_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),
    PRIMARY KEY (status_id) NOT ENFORCED
);

-- 2.5 DWD告警事实表
CREATE TABLE IF NOT EXISTS dwd_fact_alert (
    alert_id BIGINT,
    equipment_id STRING,
    customer_id STRING,
    alert_type STRING,
    alert_category STRING, -- 告警分类
    alert_level STRING,
    alert_priority INT, -- 告警优先级数值
    alert_title STRING,
    alert_description STRING,
    alert_time TIMESTAMP(3),
    alert_date DATE,
    alert_hour INT,
    alert_value DECIMAL(12,4),
    threshold_value DECIMAL(12,4),
    deviation_rate DECIMAL(5,2), -- 偏差率
    status STRING,
    acknowledged_by STRING,
    acknowledged_at TIMESTAMP(3),
    resolved_by STRING,
    resolved_at TIMESTAMP(3),
    resolution_time_minutes DECIMAL(10,2), -- 解决时长（分钟）
    resolution_notes STRING,
    is_resolved BOOLEAN,
    is_critical BOOLEAN, -- 是否严重告警
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),
    PRIMARY KEY (alert_id) NOT ENFORCED
);

-- ========================================
-- DWS层表定义（数据汇总层）
-- ========================================

-- 3.1 DWS设备小时汇总表
CREATE TABLE IF NOT EXISTS dws_equipment_hour_summary (
    equipment_id STRING,
    stat_date DATE,
    stat_hour INT,
    avg_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    avg_load_rate DECIMAL(5,2),
    max_load_rate DECIMAL(5,2),
    avg_efficiency DECIMAL(5,2),
    min_efficiency DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    min_health_score DECIMAL(5,2),
    abnormal_count INT,
    total_records INT,
    abnormal_rate DECIMAL(5,2),
    risk_level STRING,
    etl_time TIMESTAMP(3),
    PRIMARY KEY (equipment_id, stat_date, stat_hour) NOT ENFORCED
);

-- 3.2 DWS客户小时用电汇总表
CREATE TABLE IF NOT EXISTS dws_customer_hour_power (
    customer_id STRING,
    stat_date DATE,
    stat_hour INT,
    total_active_power DECIMAL(12,4),
    total_reactive_power DECIMAL(12,4),
    avg_power_factor DECIMAL(4,3),
    min_power_factor DECIMAL(4,3),
    peak_power DECIMAL(10,4),
    avg_voltage DECIMAL(8,2),
    voltage_unbalance_avg DECIMAL(5,2),
    frequency_deviation_avg DECIMAL(5,2),
    energy_consumption DECIMAL(12,4),
    anomaly_count INT,
    total_records INT,
    data_quality_avg DECIMAL(3,2),
    etl_time TIMESTAMP(3),
    PRIMARY KEY (customer_id, stat_date, stat_hour) NOT ENFORCED
);

-- 3.3 DWS告警小时统计表
CREATE TABLE IF NOT EXISTS dws_alert_hour_stats (
    stat_date DATE,
    stat_hour INT,
    equipment_id STRING,
    alert_type STRING,
    alert_level STRING,
    total_alerts INT,
    resolved_alerts INT,
    pending_alerts INT,
    avg_resolution_time DECIMAL(10,2),
    critical_alerts INT,
    equipment_alerts INT,
    power_alerts INT,
    voltage_alerts INT,
    overload_alerts INT,
    etl_time TIMESTAMP(3),
    PRIMARY KEY (stat_date, stat_hour, equipment_id, alert_type, alert_level) NOT ENFORCED
);

-- 显示当前环境状态
SHOW CATALOGS;
SHOW TABLES;

-- DDL脚本执行完成
SELECT 'DDL脚本执行完成，所有表结构已创建' as status;