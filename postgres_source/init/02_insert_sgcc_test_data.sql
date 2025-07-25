-- 国网风控数仓分层架构 - 测试数据插入脚本
-- 为5个核心业务表插入模拟数据

-- 1. 插入设备信息测试数据
INSERT INTO equipment_info (equipment_id, equipment_name, equipment_type, location, voltage_level, capacity, manufacturer, install_date, status) VALUES
('EQ001', '110kV主变压器#1', '变压器', '北京朝阳变电站', 110, 63.0, '西电集团', '2020-03-15', 'NORMAL'),
('EQ002', '220kV断路器#1', '断路器', '北京朝阳变电站', 220, NULL, '平高集团', '2019-08-20', 'NORMAL'),
('EQ003', '35kV配电变压器#1', '配电变压器', '海淀区配电房', 35, 1.6, '特变电工', '2021-06-10', 'WARNING'),
('EQ004', '10kV开关柜#1', '开关柜', '丰台工业园', 10, NULL, '森源电气', '2022-01-25', 'NORMAL'),
('EQ005', '500kV输电线路#1', '输电线路', '京津输电走廊', 500, NULL, '国网建设', '2018-12-05', 'NORMAL');

-- 2. 插入客户信息测试数据
INSERT INTO customer_info (customer_id, customer_name, customer_type, contact_person, contact_phone, address, contract_capacity, voltage_level, tariff_type, status) VALUES
('CU001', '北京钢铁集团', '工业', '张经理', '13800138001', '北京市朝阳区工业园区1号', 50000.0, 110, '大工业电价', 'ACTIVE'),
('CU002', '海淀科技园区', '商业', '李主任', '13800138002', '北京市海淀区中关村大街100号', 8000.0, 10, '一般工商业电价', 'ACTIVE'),
('CU003', '丰台居民小区A区', '居民', '王物业', '13800138003', '北京市丰台区丰台路200号', 2000.0, 0.4, '居民电价', 'ACTIVE'),
('CU004', '昌平农业合作社', '农业', '赵社长', '13800138004', '北京市昌平区农业园区50号', 1500.0, 10, '农业电价', 'ACTIVE'),
('CU005', '通州商业综合体', '商业', '陈总监', '13800138005', '北京市通州区新华大街300号', 12000.0, 10, '一般工商业电价', 'ACTIVE');

-- 3. 插入用电数据测试数据（最近24小时的数据）
INSERT INTO power_consumption (customer_id, equipment_id, record_time, active_power, reactive_power, voltage_a, voltage_b, voltage_c, current_a, current_b, current_c, power_factor, frequency, energy_consumption) VALUES
-- 北京钢铁集团的用电数据
('CU001', 'EQ001', NOW() - INTERVAL '1 hour', 45000.5, 12000.3, 110500.0, 110300.0, 110400.0, 236.4, 235.8, 236.1, 0.968, 50.02, 1250000.5),
('CU001', 'EQ001', NOW() - INTERVAL '2 hours', 46200.8, 12500.1, 110600.0, 110400.0, 110500.0, 242.1, 241.5, 241.8, 0.965, 50.01, 1248750.3),
('CU001', 'EQ001', NOW() - INTERVAL '3 hours', 44800.2, 11800.7, 110400.0, 110200.0, 110300.0, 234.2, 233.6, 233.9, 0.970, 50.03, 1247550.1),
-- 海淀科技园区的用电数据
('CU002', 'EQ003', NOW() - INTERVAL '1 hour', 7200.3, 2100.5, 35200.0, 35100.0, 35150.0, 118.5, 117.8, 118.2, 0.960, 49.98, 185000.8),
('CU002', 'EQ003', NOW() - INTERVAL '2 hours', 7500.6, 2200.8, 35300.0, 35200.0, 35250.0, 123.2, 122.5, 122.9, 0.958, 49.99, 184280.5),
-- 丰台居民小区的用电数据
('CU003', 'EQ004', NOW() - INTERVAL '1 hour', 1800.5, 450.2, 10200.0, 10150.0, 10180.0, 102.3, 101.8, 102.1, 0.970, 50.00, 45000.2),
('CU003', 'EQ004', NOW() - INTERVAL '2 hours', 1650.8, 420.1, 10180.0, 10130.0, 10160.0, 93.8, 93.2, 93.5, 0.975, 50.01, 44820.8),
-- 昌平农业合作社的用电数据
('CU004', 'EQ004', NOW() - INTERVAL '1 hour', 1200.2, 300.5, 10150.0, 10100.0, 10125.0, 68.2, 67.8, 68.0, 0.968, 49.97, 32000.5),
-- 通州商业综合体的用电数据
('CU005', 'EQ004', NOW() - INTERVAL '1 hour', 10500.8, 3200.1, 10300.0, 10250.0, 10275.0, 596.8, 595.2, 596.0, 0.955, 50.02, 280000.3);

-- 4. 插入设备状态测试数据
INSERT INTO equipment_status (equipment_id, status_time, operating_status, temperature, pressure, vibration, oil_level, insulation_resistance, load_rate, efficiency, health_score, risk_level) VALUES
-- 110kV主变压器状态
('EQ001', NOW() - INTERVAL '30 minutes', 'RUNNING', 65.5, 0.15, 2.1, 850.0, 2500.0, 85.2, 98.5, 92.0, 'LOW'),
('EQ001', NOW() - INTERVAL '1 hour', 'RUNNING', 64.8, 0.14, 2.0, 851.0, 2520.0, 87.1, 98.6, 93.0, 'LOW'),
-- 220kV断路器状态
('EQ002', NOW() - INTERVAL '30 minutes', 'RUNNING', 45.2, NULL, 1.5, NULL, 5000.0, 0.0, 99.8, 95.0, 'LOW'),
-- 35kV配电变压器状态（有告警）
('EQ003', NOW() - INTERVAL '30 minutes', 'RUNNING', 78.5, 0.18, 3.2, 820.0, 1800.0, 92.5, 96.8, 75.0, 'MEDIUM'),
('EQ003', NOW() - INTERVAL '1 hour', 'RUNNING', 76.2, 0.17, 3.0, 825.0, 1850.0, 90.8, 97.2, 78.0, 'MEDIUM'),
-- 10kV开关柜状态
('EQ004', NOW() - INTERVAL '30 minutes', 'RUNNING', 42.0, NULL, 1.2, NULL, 3500.0, 65.5, 98.2, 88.0, 'LOW'),
-- 500kV输电线路状态
('EQ005', NOW() - INTERVAL '30 minutes', 'RUNNING', 25.8, NULL, 0.8, NULL, 8000.0, 45.2, 99.2, 96.0, 'LOW');

-- 5. 插入告警记录测试数据
INSERT INTO alert_records (equipment_id, customer_id, alert_type, alert_level, alert_title, alert_description, alert_time, alert_value, threshold_value, status, acknowledged_by, acknowledged_at) VALUES
-- 设备温度告警
('EQ003', 'CU002', 'EQUIPMENT_FAULT', 'WARNING', '配电变压器温度异常', '35kV配电变压器#1温度超过正常范围，当前温度78.5℃，正常范围应低于75℃', NOW() - INTERVAL '25 minutes', 78.5, 75.0, 'ACKNOWLEDGED', '运维人员001', NOW() - INTERVAL '20 minutes'),
-- 负载率告警
('EQ003', 'CU002', 'OVERLOAD', 'WARNING', '设备负载率过高', '35kV配电变压器#1负载率达到92.5%，建议关注设备运行状态', NOW() - INTERVAL '30 minutes', 92.5, 90.0, 'ACKNOWLEDGED', '运维人员001', NOW() - INTERVAL '25 minutes'),
-- 电压异常告警
('EQ001', 'CU001', 'VOLTAGE_ABNORMAL', 'INFO', '电压轻微波动', '110kV主变压器电压出现轻微波动，A相电压110.6kV', NOW() - INTERVAL '2 hours', 110.6, 110.0, 'RESOLVED', '运维人员002', NOW() - INTERVAL '1.5 hours'),
-- 功率因数告警
('EQ004', 'CU005', 'POWER_ANOMALY', 'WARNING', '功率因数偏低', '通州商业综合体功率因数为0.955，低于标准值0.96', NOW() - INTERVAL '1 hour', 0.955, 0.960, 'OPEN', NULL, NULL),
-- 绝缘电阻告警
('EQ003', 'CU002', 'EQUIPMENT_FAULT', 'ERROR', '绝缘电阻下降', '35kV配电变压器#1绝缘电阻降至1800MΩ，需要检查绝缘状况', NOW() - INTERVAL '1 hour', 1800.0, 2000.0, 'OPEN', NULL, NULL);

-- 插入更多历史数据以便测试趋势分析
-- 插入过去7天的设备状态数据
INSERT INTO equipment_status (equipment_id, status_time, operating_status, temperature, pressure, vibration, oil_level, insulation_resistance, load_rate, efficiency, health_score, risk_level)
SELECT 
    'EQ001',
    NOW() - INTERVAL '1 day' * generate_series(1, 7),
    'RUNNING',
    60.0 + random() * 10, -- 温度在60-70度之间
    0.12 + random() * 0.06, -- 压力在0.12-0.18之间
    1.8 + random() * 0.4, -- 振动在1.8-2.2之间
    840.0 + random() * 20, -- 油位在840-860之间
    2400.0 + random() * 200, -- 绝缘电阻在2400-2600之间
    80.0 + random() * 15, -- 负载率在80-95%之间
    98.0 + random() * 1.5, -- 效率在98-99.5%之间
    90.0 + random() * 8, -- 健康度在90-98之间
    'LOW';

-- 插入过去7天的用电数据
INSERT INTO power_consumption (customer_id, equipment_id, record_time, active_power, reactive_power, voltage_a, voltage_b, voltage_c, current_a, current_b, current_c, power_factor, frequency, energy_consumption)
SELECT 
    'CU001',
    'EQ001',
    NOW() - INTERVAL '1 day' * generate_series(1, 7),
    44000.0 + random() * 4000, -- 有功功率在44-48MW之间
    11000.0 + random() * 2000, -- 无功功率在11-13MVar之间
    110000.0 + random() * 1000, -- 电压在110±0.5kV之间
    110000.0 + random() * 1000,
    110000.0 + random() * 1000,
    230.0 + random() * 20, -- 电流在230-250A之间
    230.0 + random() * 20,
    230.0 + random() * 20,
    0.96 + random() * 0.02, -- 功率因数在0.96-0.98之间
    49.98 + random() * 0.06, -- 频率在49.98-50.04Hz之间
    1240000.0 + random() * 20000; -- 累计电量

COMMIT;