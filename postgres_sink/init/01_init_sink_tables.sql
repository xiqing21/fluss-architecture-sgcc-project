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
    equipment_name VARCHAR(100),
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
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

-- 为表和字段添加中文注释
-- 实时监控大屏指标表注释
COMMENT ON TABLE ads_realtime_dashboard IS '实时监控大屏指标表 - 存储实时监控大屏展示的各类业务指标数据';
COMMENT ON COLUMN ads_realtime_dashboard.metric_id IS '指标ID - 指标记录的唯一标识';
COMMENT ON COLUMN ads_realtime_dashboard.metric_name IS '指标名称 - 监控指标的标识名称';
COMMENT ON COLUMN ads_realtime_dashboard.metric_value IS '指标数值 - 指标的实际数值';
COMMENT ON COLUMN ads_realtime_dashboard.metric_unit IS '指标单位 - 指标数值的计量单位';
COMMENT ON COLUMN ads_realtime_dashboard.metric_desc IS '指标描述 - 指标的详细说明';
COMMENT ON COLUMN ads_realtime_dashboard.metric_category IS '指标分类 - POWER电力/EQUIPMENT设备/ALERT告警/CUSTOMER客户';
COMMENT ON COLUMN ads_realtime_dashboard.update_time IS '更新时间 - 指标数据的更新时间戳';
COMMENT ON COLUMN ads_realtime_dashboard.created_at IS '创建时间 - 记录创建时间戳';

-- 设备健康度分析表注释
COMMENT ON TABLE ads_equipment_health IS '设备健康度分析表 - 存储设备健康状况分析结果和预测性维护建议';
COMMENT ON COLUMN ads_equipment_health.analysis_id IS '分析ID - 健康度分析记录的唯一标识';
COMMENT ON COLUMN ads_equipment_health.equipment_id IS '设备编号 - 被分析设备的唯一标识';
COMMENT ON COLUMN ads_equipment_health.equipment_name IS '设备名称 - 被分析设备的名称';
COMMENT ON COLUMN ads_equipment_health.equipment_type IS '设备类型 - 被分析设备的类型分类';
COMMENT ON COLUMN ads_equipment_health.location IS '设备位置 - 被分析设备的安装位置';
COMMENT ON COLUMN ads_equipment_health.health_score IS '健康度评分 - 设备健康状况综合评分(0-100分)';
COMMENT ON COLUMN ads_equipment_health.risk_level IS '风险等级 - 设备风险等级评估结果';
COMMENT ON COLUMN ads_equipment_health.temperature_avg IS '平均温度 - 分析周期内设备平均运行温度(℃)';
COMMENT ON COLUMN ads_equipment_health.load_rate_avg IS '平均负载率 - 分析周期内设备平均负载率(%)';
COMMENT ON COLUMN ads_equipment_health.efficiency_avg IS '平均效率 - 分析周期内设备平均运行效率(%)';
COMMENT ON COLUMN ads_equipment_health.fault_count IS '故障次数 - 分析周期内设备故障发生次数';
COMMENT ON COLUMN ads_equipment_health.maintenance_days IS '维护间隔天数 - 距离上次维护的天数';
COMMENT ON COLUMN ads_equipment_health.prediction_score IS '故障预测评分 - 设备故障风险预测评分(0-100分)';
COMMENT ON COLUMN ads_equipment_health.recommendation IS '维护建议 - 基于分析结果的维护建议';
COMMENT ON COLUMN ads_equipment_health.analysis_time IS '分析时间 - 健康度分析执行时间戳';
COMMENT ON COLUMN ads_equipment_health.created_at IS '创建时间 - 记录创建时间戳';

-- 客户用电行为分析表注释
COMMENT ON TABLE ads_customer_behavior IS '客户用电行为分析表 - 存储客户用电模式分析和行为特征数据';
COMMENT ON COLUMN ads_customer_behavior.behavior_id IS '行为分析ID - 客户行为分析记录的唯一标识';
COMMENT ON COLUMN ads_customer_behavior.customer_id IS '客户编号 - 被分析客户的唯一标识';
COMMENT ON COLUMN ads_customer_behavior.customer_name IS '客户名称 - 被分析客户的名称';
COMMENT ON COLUMN ads_customer_behavior.customer_type IS '客户类型 - 被分析客户的用电性质分类';
COMMENT ON COLUMN ads_customer_behavior.analysis_period IS '分析周期 - DAILY日/WEEKLY周/MONTHLY月度分析';
COMMENT ON COLUMN ads_customer_behavior.total_consumption IS '总用电量 - 分析周期内客户总用电量(kWh)';
COMMENT ON COLUMN ads_customer_behavior.avg_power IS '平均功率 - 分析周期内客户平均用电功率(kW)';
COMMENT ON COLUMN ads_customer_behavior.peak_power IS '峰值功率 - 分析周期内客户最大用电功率(kW)';
COMMENT ON COLUMN ads_customer_behavior.power_factor_avg IS '平均功率因数 - 分析周期内客户平均功率因数';
COMMENT ON COLUMN ads_customer_behavior.load_pattern IS '负荷模式 - STABLE稳定/FLUCTUATING波动/PEAK_VALLEY峰谷';
COMMENT ON COLUMN ads_customer_behavior.anomaly_count IS '异常次数 - 分析周期内用电异常事件次数';
COMMENT ON COLUMN ads_customer_behavior.cost_estimation IS '预估电费 - 基于用电量的电费预估(元)';
COMMENT ON COLUMN ads_customer_behavior.efficiency_rating IS '用电效率等级 - 客户用电效率评级';
COMMENT ON COLUMN ads_customer_behavior.carbon_emission IS '碳排放量 - 用电产生的碳排放量(kg)';
COMMENT ON COLUMN ads_customer_behavior.analysis_time IS '分析时间 - 行为分析执行时间戳';
COMMENT ON COLUMN ads_customer_behavior.created_at IS '创建时间 - 记录创建时间戳';

-- 告警统计分析表注释
COMMENT ON TABLE ads_alert_statistics IS '告警统计分析表 - 存储系统告警事件的统计分析数据';
COMMENT ON COLUMN ads_alert_statistics.stat_id IS '统计ID - 告警统计记录的唯一标识';
COMMENT ON COLUMN ads_alert_statistics.stat_period IS '统计周期 - HOURLY小时/DAILY日/WEEKLY周/MONTHLY月度统计';
COMMENT ON COLUMN ads_alert_statistics.stat_time IS '统计时间 - 告警统计的时间戳';
COMMENT ON COLUMN ads_alert_statistics.total_alerts IS '总告警数 - 统计周期内告警事件总数';
COMMENT ON COLUMN ads_alert_statistics.critical_alerts IS '严重告警数 - 统计周期内严重级别告警数量';
COMMENT ON COLUMN ads_alert_statistics.error_alerts IS '错误告警数 - 统计周期内错误级别告警数量';
COMMENT ON COLUMN ads_alert_statistics.warning_alerts IS '警告告警数 - 统计周期内警告级别告警数量';
COMMENT ON COLUMN ads_alert_statistics.info_alerts IS '信息告警数 - 统计周期内信息级别告警数量';
COMMENT ON COLUMN ads_alert_statistics.equipment_alerts IS '设备告警数 - 统计周期内设备相关告警数量';
COMMENT ON COLUMN ads_alert_statistics.power_alerts IS '电力告警数 - 统计周期内电力相关告警数量';
COMMENT ON COLUMN ads_alert_statistics.voltage_alerts IS '电压告警数 - 统计周期内电压异常告警数量';
COMMENT ON COLUMN ads_alert_statistics.overload_alerts IS '过载告警数 - 统计周期内过载告警数量';
COMMENT ON COLUMN ads_alert_statistics.resolved_alerts IS '已解决告警数 - 统计周期内已解决的告警数量';
COMMENT ON COLUMN ads_alert_statistics.avg_resolution_time IS '平均解决时间 - 告警平均处理解决时间(小时)';
COMMENT ON COLUMN ads_alert_statistics.alert_rate IS '告警率 - 告警发生频率百分比(%)';
COMMENT ON COLUMN ads_alert_statistics.resolution_rate IS '解决率 - 告警解决率百分比(%)';
COMMENT ON COLUMN ads_alert_statistics.created_at IS '创建时间 - 记录创建时间戳';

-- 电力质量分析表注释
COMMENT ON TABLE ads_power_quality IS '电力质量分析表 - 存储电能质量监测分析结果和改善建议';
COMMENT ON COLUMN ads_power_quality.quality_id IS '质量分析ID - 电力质量分析记录的唯一标识';
COMMENT ON COLUMN ads_power_quality.equipment_id IS '设备编号 - 被监测设备的唯一标识';
COMMENT ON COLUMN ads_power_quality.customer_id IS '客户编号 - 被监测客户的唯一标识';
COMMENT ON COLUMN ads_power_quality.analysis_time IS '分析时间 - 电力质量分析执行时间戳';
COMMENT ON COLUMN ads_power_quality.voltage_stability IS '电压稳定性评分 - 电压稳定性质量评分(0-100分)';
COMMENT ON COLUMN ads_power_quality.frequency_stability IS '频率稳定性评分 - 频率稳定性质量评分(0-100分)';
COMMENT ON COLUMN ads_power_quality.power_factor_quality IS '功率因数质量评分 - 功率因数质量评分(0-100分)';
COMMENT ON COLUMN ads_power_quality.harmonic_distortion IS '谐波畸变率 - 总谐波畸变率百分比(%)';
COMMENT ON COLUMN ads_power_quality.voltage_unbalance IS '电压不平衡度 - 三相电压不平衡度百分比(%)';
COMMENT ON COLUMN ads_power_quality.flicker_severity IS '闪变严重度 - 电压闪变严重程度指标';
COMMENT ON COLUMN ads_power_quality.interruption_count IS '中断次数 - 分析周期内供电中断次数';
COMMENT ON COLUMN ads_power_quality.sag_count IS '电压暂降次数 - 分析周期内电压暂降事件次数';
COMMENT ON COLUMN ads_power_quality.swell_count IS '电压暂升次数 - 分析周期内电压暂升事件次数';
COMMENT ON COLUMN ads_power_quality.overall_quality IS '综合电能质量评分 - 电能质量综合评分(0-100分)';
COMMENT ON COLUMN ads_power_quality.quality_grade IS '质量等级 - EXCELLENT优秀/GOOD良好/FAIR一般/POOR较差';
COMMENT ON COLUMN ads_power_quality.improvement_suggestions IS '改善建议 - 电能质量改善的技术建议';
COMMENT ON COLUMN ads_power_quality.created_at IS '创建时间 - 记录创建时间戳';

-- 风险评估汇总表注释
COMMENT ON TABLE ads_risk_assessment IS '风险评估汇总表 - 存储系统整体风险评估结果和缓解措施';
COMMENT ON COLUMN ads_risk_assessment.assessment_id IS '评估ID - 风险评估记录的唯一标识';
COMMENT ON COLUMN ads_risk_assessment.assessment_time IS '评估时间 - 风险评估执行时间戳';
COMMENT ON COLUMN ads_risk_assessment.overall_risk_score IS '整体风险评分 - 系统整体风险评分(0-100分)';
COMMENT ON COLUMN ads_risk_assessment.equipment_risk_score IS '设备风险评分 - 设备相关风险评分(0-100分)';
COMMENT ON COLUMN ads_risk_assessment.power_risk_score IS '电力风险评分 - 电力系统风险评分(0-100分)';
COMMENT ON COLUMN ads_risk_assessment.customer_risk_score IS '客户风险评分 - 客户相关风险评分(0-100分)';
COMMENT ON COLUMN ads_risk_assessment.high_risk_equipment_count IS '高风险设备数量 - 被评估为高风险的设备数量';
COMMENT ON COLUMN ads_risk_assessment.critical_alerts_24h IS '24小时严重告警数 - 近24小时内严重告警事件数量';
COMMENT ON COLUMN ads_risk_assessment.power_quality_issues IS '电能质量问题数 - 当前存在的电能质量问题数量';
COMMENT ON COLUMN ads_risk_assessment.load_forecast_accuracy IS '负荷预测准确率 - 负荷预测的准确率百分比(%)';
COMMENT ON COLUMN ads_risk_assessment.system_stability_index IS '系统稳定性指数 - 电力系统稳定性综合指数';
COMMENT ON COLUMN ads_risk_assessment.emergency_response_time IS '应急响应时间 - 平均应急响应时间(分钟)';
COMMENT ON COLUMN ads_risk_assessment.risk_trend IS '风险趋势 - INCREASING上升/STABLE稳定/DECREASING下降';
COMMENT ON COLUMN ads_risk_assessment.mitigation_actions IS '风险缓解措施 - 建议采取的风险缓解行动';
COMMENT ON COLUMN ads_risk_assessment.next_assessment_time IS '下次评估时间 - 计划的下次风险评估时间';
COMMENT ON COLUMN ads_risk_assessment.created_at IS '创建时间 - 记录创建时间戳';

-- 能效分析表注释
COMMENT ON TABLE ads_energy_efficiency IS '能效分析表 - 存储能源效率分析结果和节能建议';
COMMENT ON COLUMN ads_energy_efficiency.efficiency_id IS '能效分析ID - 能效分析记录的唯一标识';
COMMENT ON COLUMN ads_energy_efficiency.analysis_scope IS '分析范围 - EQUIPMENT设备/CUSTOMER客户/REGION区域/SYSTEM系统';
COMMENT ON COLUMN ads_energy_efficiency.scope_id IS '范围ID - 对应的设备ID、客户ID等标识';
COMMENT ON COLUMN ads_energy_efficiency.scope_name IS '范围名称 - 对应的设备名称、客户名称等';
COMMENT ON COLUMN ads_energy_efficiency.analysis_period IS '分析周期 - DAILY日/WEEKLY周/MONTHLY月/YEARLY年度分析';
COMMENT ON COLUMN ads_energy_efficiency.analysis_time IS '分析时间 - 能效分析执行时间戳';
COMMENT ON COLUMN ads_energy_efficiency.energy_input IS '输入能量 - 系统输入的总能量(kWh)';
COMMENT ON COLUMN ads_energy_efficiency.energy_output IS '输出能量 - 系统输出的有效能量(kWh)';
COMMENT ON COLUMN ads_energy_efficiency.energy_loss IS '能量损失 - 系统运行中的能量损失(kWh)';
COMMENT ON COLUMN ads_energy_efficiency.efficiency_ratio IS '效率比 - 输出能量与输入能量的比值(%)';
COMMENT ON COLUMN ads_energy_efficiency.benchmark_efficiency IS '基准效率 - 行业或历史基准效率(%)';
COMMENT ON COLUMN ads_energy_efficiency.efficiency_gap IS '效率差距 - 与基准效率的差距(%)';
COMMENT ON COLUMN ads_energy_efficiency.carbon_intensity IS '碳强度 - 单位能量的碳排放强度(kg CO2/kWh)';
COMMENT ON COLUMN ads_energy_efficiency.cost_per_kwh IS '单位电量成本 - 每千瓦时的成本(元/kWh)';
COMMENT ON COLUMN ads_energy_efficiency.potential_savings IS '潜在节约 - 通过效率提升可节约的成本(元)';
COMMENT ON COLUMN ads_energy_efficiency.efficiency_grade IS '效率等级 - A优秀/B良好/C一般/D较差/E差';
COMMENT ON COLUMN ads_energy_efficiency.optimization_suggestions IS '优化建议 - 具体的能效优化改进建议';
COMMENT ON COLUMN ads_energy_efficiency.created_at IS '创建时间 - 记录创建时间戳';

COMMIT;