-- 国网风控数仓分层架构 - PostgreSQL Sink数据库初始化脚本
-- 创建ADS层分析表，用于存储最终的业务指标和分析结果

-- 1. 实时监控大屏指标表 (ads_realtime_dashboard)
CREATE TABLE IF NOT EXISTS ads_realtime_dashboard (
    metric_id BIGSERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL, -- 指标名称
    metric_value DECIMAL(15,4) NOT NULL, -- 指标数值
    metric_unit VARCHAR(20), -- 指标单位
    metric_desc VARCHAR(200), -- 指标描述
    metric_category VARCHAR(50), -- 指标分类：POWER, EQUIPMENT, ALERT, CUSTOMER
    update_time TIMESTAMP NOT NULL, -- 更新时间
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 设备健康度分析表 (ads_equipment_health)
CREATE TABLE IF NOT EXISTS ads_equipment_health (
    analysis_id BIGSERIAL PRIMARY KEY,
    equipment_id VARCHAR(50) NOT NULL,
    equipment_name VARCHAR(100) NOT NULL,
    equipment_type VARCHAR(50) NOT NULL,
    location VARCHAR(200) NOT NULL,
    health_score DECIMAL(5,2) NOT NULL, -- 健康度评分(0-100)
    risk_level VARCHAR(20) NOT NULL, -- 风险等级
    temperature_avg DECIMAL(5,2), -- 平均温度
    load_rate_avg DECIMAL(5,2), -- 平均负载率
    efficiency_avg DECIMAL(5,2), -- 平均效率
    fault_count INTEGER DEFAULT 0, -- 故障次数
    maintenance_days INTEGER, -- 距离上次维护天数
    prediction_score DECIMAL(5,2), -- 故障预测评分
    recommendation TEXT, -- 维护建议
    analysis_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. 客户用电行为分析表 (ads_customer_behavior)
CREATE TABLE IF NOT EXISTS ads_customer_behavior (
    behavior_id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_type VARCHAR(50) NOT NULL,
    analysis_period VARCHAR(20) NOT NULL, -- DAILY, WEEKLY, MONTHLY
    total_consumption DECIMAL(12,4), -- 总用电量(kWh)
    avg_power DECIMAL(10,4), -- 平均功率(kW)
    peak_power DECIMAL(10,4), -- 峰值功率(kW)
    power_factor_avg DECIMAL(4,3), -- 平均功率因数
    load_pattern VARCHAR(50), -- 负荷模式：STABLE, FLUCTUATING, PEAK_VALLEY
    anomaly_count INTEGER DEFAULT 0, -- 异常次数
    cost_estimation DECIMAL(12,2), -- 预估电费
    efficiency_rating VARCHAR(20), -- 用电效率等级
    carbon_emission DECIMAL(10,4), -- 碳排放量(kg)
    analysis_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. 告警统计分析表 (ads_alert_statistics)
CREATE TABLE IF NOT EXISTS ads_alert_statistics (
    stat_id BIGSERIAL PRIMARY KEY,
    stat_period VARCHAR(20) NOT NULL, -- HOURLY, DAILY, WEEKLY, MONTHLY
    stat_time TIMESTAMP NOT NULL,
    total_alerts INTEGER DEFAULT 0, -- 总告警数
    critical_alerts INTEGER DEFAULT 0, -- 严重告警数
    error_alerts INTEGER DEFAULT 0, -- 错误告警数
    warning_alerts INTEGER DEFAULT 0, -- 警告告警数
    info_alerts INTEGER DEFAULT 0, -- 信息告警数
    equipment_alerts INTEGER DEFAULT 0, -- 设备告警数
    power_alerts INTEGER DEFAULT 0, -- 电力告警数
    voltage_alerts INTEGER DEFAULT 0, -- 电压告警数
    overload_alerts INTEGER DEFAULT 0, -- 过载告警数
    resolved_alerts INTEGER DEFAULT 0, -- 已解决告警数
    avg_resolution_time DECIMAL(8,2), -- 平均解决时间(小时)
    alert_rate DECIMAL(5,2), -- 告警率(%)
    resolution_rate DECIMAL(5,2), -- 解决率(%)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. 电力质量分析表 (ads_power_quality)
CREATE TABLE IF NOT EXISTS ads_power_quality (
    quality_id BIGSERIAL PRIMARY KEY,
    equipment_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    analysis_time TIMESTAMP NOT NULL,
    voltage_stability DECIMAL(5,2), -- 电压稳定性评分(0-100)
    frequency_stability DECIMAL(5,2), -- 频率稳定性评分(0-100)
    power_factor_quality DECIMAL(5,2), -- 功率因数质量评分(0-100)
    harmonic_distortion DECIMAL(5,2), -- 谐波畸变率(%)
    voltage_unbalance DECIMAL(5,2), -- 电压不平衡度(%)
    flicker_severity DECIMAL(5,2), -- 闪变严重度
    interruption_count INTEGER DEFAULT 0, -- 中断次数
    sag_count INTEGER DEFAULT 0, -- 电压暂降次数
    swell_count INTEGER DEFAULT 0, -- 电压暂升次数
    overall_quality DECIMAL(5,2), -- 综合电能质量评分(0-100)
    quality_grade VARCHAR(20), -- 质量等级：EXCELLENT, GOOD, FAIR, POOR
    improvement_suggestions TEXT, -- 改善建议
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. 风险评估汇总表 (ads_risk_assessment)
CREATE TABLE IF NOT EXISTS ads_risk_assessment (
    assessment_id BIGSERIAL PRIMARY KEY,
    assessment_time TIMESTAMP NOT NULL,
    overall_risk_score DECIMAL(5,2) NOT NULL, -- 整体风险评分(0-100)
    equipment_risk_score DECIMAL(5,2), -- 设备风险评分
    power_risk_score DECIMAL(5,2), -- 电力风险评分
    customer_risk_score DECIMAL(5,2), -- 客户风险评分
    high_risk_equipment_count INTEGER DEFAULT 0, -- 高风险设备数量
    critical_alerts_24h INTEGER DEFAULT 0, -- 24小时内严重告警数
    power_quality_issues INTEGER DEFAULT 0, -- 电能质量问题数
    load_forecast_accuracy DECIMAL(5,2), -- 负荷预测准确率(%)
    system_stability_index DECIMAL(5,2), -- 系统稳定性指数
    emergency_response_time DECIMAL(8,2), -- 应急响应时间(分钟)
    risk_trend VARCHAR(20), -- 风险趋势：INCREASING, STABLE, DECREASING
    mitigation_actions TEXT, -- 风险缓解措施
    next_assessment_time TIMESTAMP, -- 下次评估时间
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. 能效分析表 (ads_energy_efficiency)
CREATE TABLE IF NOT EXISTS ads_energy_efficiency (
    efficiency_id BIGSERIAL PRIMARY KEY,
    analysis_scope VARCHAR(50) NOT NULL, -- EQUIPMENT, CUSTOMER, REGION, SYSTEM
    scope_id VARCHAR(50) NOT NULL, -- 对应的设备ID、客户ID等
    scope_name VARCHAR(100) NOT NULL,
    analysis_period VARCHAR(20) NOT NULL, -- DAILY, WEEKLY, MONTHLY, YEARLY
    analysis_time TIMESTAMP NOT NULL,
    energy_input DECIMAL(12,4), -- 输入能量(kWh)
    energy_output DECIMAL(12,4), -- 输出能量(kWh)
    energy_loss DECIMAL(12,4), -- 能量损失(kWh)
    efficiency_ratio DECIMAL(5,2), -- 效率比(%)
    benchmark_efficiency DECIMAL(5,2), -- 基准效率(%)
    efficiency_gap DECIMAL(5,2), -- 效率差距(%)
    carbon_intensity DECIMAL(8,4), -- 碳强度(kg CO2/kWh)
    cost_per_kwh DECIMAL(8,4), -- 单位电量成本(元/kWh)
    potential_savings DECIMAL(12,2), -- 潜在节约(元)
    efficiency_grade VARCHAR(20), -- 效率等级
    optimization_suggestions TEXT, -- 优化建议
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_ads_realtime_dashboard_name_time ON ads_realtime_dashboard(metric_name, update_time);
CREATE INDEX IF NOT EXISTS idx_ads_equipment_health_equipment_time ON ads_equipment_health(equipment_id, analysis_time);
CREATE INDEX IF NOT EXISTS idx_ads_equipment_health_risk_level ON ads_equipment_health(risk_level);
CREATE INDEX IF NOT EXISTS idx_ads_customer_behavior_customer_time ON ads_customer_behavior(customer_id, analysis_time);
CREATE INDEX IF NOT EXISTS idx_ads_customer_behavior_type_period ON ads_customer_behavior(customer_type, analysis_period);
CREATE INDEX IF NOT EXISTS idx_ads_alert_statistics_period_time ON ads_alert_statistics(stat_period, stat_time);
CREATE INDEX IF NOT EXISTS idx_ads_power_quality_equipment_time ON ads_power_quality(equipment_id, analysis_time);
CREATE INDEX IF NOT EXISTS idx_ads_power_quality_grade ON ads_power_quality(quality_grade);
CREATE INDEX IF NOT EXISTS idx_ads_risk_assessment_time ON ads_risk_assessment(assessment_time);
CREATE INDEX IF NOT EXISTS idx_ads_risk_assessment_trend ON ads_risk_assessment(risk_trend);
CREATE INDEX IF NOT EXISTS idx_ads_energy_efficiency_scope_time ON ads_energy_efficiency(analysis_scope, scope_id, analysis_time);
CREATE INDEX IF NOT EXISTS idx_ads_energy_efficiency_grade ON ads_energy_efficiency(efficiency_grade);

-- 创建视图用于快速查询最新数据
CREATE OR REPLACE VIEW v_latest_equipment_health AS
SELECT DISTINCT ON (equipment_id) *
FROM ads_equipment_health
ORDER BY equipment_id, analysis_time DESC;

CREATE OR REPLACE VIEW v_latest_customer_behavior AS
SELECT DISTINCT ON (customer_id, analysis_period) *
FROM ads_customer_behavior
ORDER BY customer_id, analysis_period, analysis_time DESC;

CREATE OR REPLACE VIEW v_latest_power_quality AS
SELECT DISTINCT ON (equipment_id) *
FROM ads_power_quality
ORDER BY equipment_id, analysis_time DESC;

CREATE OR REPLACE VIEW v_current_risk_assessment AS
SELECT *
FROM ads_risk_assessment
ORDER BY assessment_time DESC
LIMIT 1;

-- 创建实时监控大屏的汇总视图
CREATE OR REPLACE VIEW v_dashboard_summary AS
SELECT 
    'TOTAL_POWER' as metric_name,
    COALESCE(SUM(CASE WHEN metric_name = 'total_active_power' THEN metric_value END), 0) as metric_value,
    'MW' as metric_unit,
    '系统总有功功率' as metric_desc,
    'POWER' as metric_category,
    MAX(update_time) as update_time
FROM ads_realtime_dashboard
WHERE metric_name LIKE '%power%'
UNION ALL
SELECT 
    'EQUIPMENT_HEALTH_AVG' as metric_name,
    COALESCE(AVG(health_score), 0) as metric_value,
    '分' as metric_unit,
    '设备平均健康度' as metric_desc,
    'EQUIPMENT' as metric_category,
    MAX(analysis_time) as update_time
FROM v_latest_equipment_health
UNION ALL
SELECT 
    'ACTIVE_ALERTS' as metric_name,
    COALESCE(SUM(total_alerts - resolved_alerts), 0) as metric_value,
    '个' as metric_unit,
    '活跃告警数量' as metric_desc,
    'ALERT' as metric_category,
    MAX(stat_time) as update_time
FROM ads_alert_statistics
WHERE stat_period = 'DAILY' AND stat_time >= CURRENT_DATE;

COMMIT;