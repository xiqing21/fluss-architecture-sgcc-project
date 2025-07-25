-- 国网风控数仓 - CRUD操作测试脚本
-- 测试从PostgreSQL Source到Sink的数据流

-- ========================================
-- 1. 插入测试数据到Source数据库
-- ========================================

-- 插入设备信息
INSERT INTO equipment_info (equipment_id, equipment_name, equipment_type, location, voltage_level, capacity, manufacturer, install_date, status) VALUES
('EQ001', '主变压器1号', '变压器', '北京变电站A', 220, 150.0, '西门子', '2020-01-15', 'NORMAL'),
('EQ002', '开关柜2号', '开关', '北京变电站A', 35, 50.0, 'ABB', '2021-03-20', 'NORMAL'),
('EQ003', '线路保护装置', '保护装置', '北京变电站B', 110, 25.0, '南瑞', '2019-08-10', 'WARNING');

-- 插入客户信息
INSERT INTO customer_info (customer_id, customer_name, customer_type, contact_person, contact_phone, address, contract_capacity, voltage_level, tariff_type, status) VALUES
('CU001', '北京钢铁厂', '工业', '张经理', '13800138001', '北京市朝阳区工业园区1号', 5000.0, 35, '大工业电价', 'ACTIVE'),
('CU002', '朝阳商业广场', '商业', '李经理', '13800138002', '北京市朝阳区商业街88号', 1200.0, 10, '一般工商业电价', 'ACTIVE'),
('CU003', '居民小区A', '居民', '王主任', '13800138003', '北京市海淀区住宅区10号', 800.0, 0.4, '居民电价', 'ACTIVE');

-- 插入用电数据
INSERT INTO power_consumption (customer_id, equipment_id, record_time, active_power, reactive_power, voltage_a, voltage_b, voltage_c, current_a, current_b, current_c, power_factor, frequency, energy_consumption) VALUES
('CU001', 'EQ001', NOW() - INTERVAL '1 hour', 4500.50, 1200.30, 35000, 35100, 34900, 128.5, 129.2, 127.8, 0.965, 50.02, 4500.50),
('CU001', 'EQ001', NOW() - INTERVAL '30 minutes', 4650.75, 1250.45, 35050, 35150, 34950, 132.1, 133.0, 131.5, 0.968, 50.01, 9151.25),
('CU002', 'EQ002', NOW() - INTERVAL '1 hour', 1150.25, 280.15, 10000, 10050, 9950, 115.0, 116.2, 114.8, 0.970, 49.98, 1150.25),
('CU002', 'EQ002', NOW() - INTERVAL '30 minutes', 1200.80, 295.20, 10020, 10070, 9970, 120.1, 121.0, 119.5, 0.972, 49.99, 2351.05),
('CU003', 'EQ003', NOW() - INTERVAL '1 hour', 750.60, 180.25, 380, 385, 375, 1973.7, 1980.5, 1965.2, 0.972, 50.00, 750.60),
('CU003', 'EQ003', NOW() - INTERVAL '30 minutes', 780.90, 185.50, 382, 387, 377, 2043.4, 2050.1, 2035.8, 0.973, 50.01, 1531.50);

-- 插入设备状态数据
INSERT INTO equipment_status (equipment_id, status_time, operating_status, temperature, pressure, vibration, oil_level, insulation_resistance, load_rate, efficiency, health_score, risk_level) VALUES
('EQ001', NOW() - INTERVAL '1 hour', 'RUNNING', 65.5, 0.15, 2.1, 850.0, 1500.0, 75.2, 98.5, 92.0, 'LOW'),
('EQ001', NOW() - INTERVAL '30 minutes', 'RUNNING', 66.2, 0.16, 2.2, 849.5, 1498.5, 77.8, 98.3, 91.5, 'LOW'),
('EQ002', NOW() - INTERVAL '1 hour', 'RUNNING', 45.8, 0.12, 1.5, 920.0, 2000.0, 58.5, 99.2, 95.5, 'LOW'),
('EQ002', NOW() - INTERVAL '30 minutes', 'RUNNING', 46.5, 0.13, 1.6, 919.2, 1995.0, 61.2, 99.0, 95.0, 'LOW'),
('EQ003', NOW() - INTERVAL '1 hour', 'RUNNING', 72.3, 0.18, 3.2, 780.0, 1200.0, 68.5, 96.8, 78.5, 'MEDIUM'),
('EQ003', NOW() - INTERVAL '30 minutes', 'RUNNING', 73.1, 0.19, 3.4, 778.5, 1195.0, 70.2, 96.5, 77.8, 'MEDIUM');

-- 插入告警记录
INSERT INTO alert_records (equipment_id, customer_id, alert_type, alert_level, alert_title, alert_description, alert_time, alert_value, threshold_value, status) VALUES
('EQ003', 'CU003', 'EQUIPMENT_FAULT', 'WARNING', '设备温度偏高', '线路保护装置温度超过正常范围', NOW() - INTERVAL '45 minutes', 73.1, 70.0, 'OPEN'),
('EQ001', 'CU001', 'POWER_ANOMALY', 'INFO', '负载率较高', '主变压器负载率接近80%', NOW() - INTERVAL '25 minutes', 77.8, 80.0, 'ACKNOWLEDGED'),
('EQ002', 'CU002', 'VOLTAGE_ABNORMAL', 'WARNING', '电压波动', '开关柜电压存在轻微波动', NOW() - INTERVAL '15 minutes', 10070, 10000, 'OPEN');

-- ========================================
-- 2. 查询Source数据库验证数据插入
-- ========================================

SELECT 'equipment_info' as table_name, COUNT(*) as record_count FROM equipment_info
UNION ALL
SELECT 'customer_info' as table_name, COUNT(*) as record_count FROM customer_info
UNION ALL
SELECT 'power_consumption' as table_name, COUNT(*) as record_count FROM power_consumption
UNION ALL
SELECT 'equipment_status' as table_name, COUNT(*) as record_count FROM equipment_status
UNION ALL
SELECT 'alert_records' as table_name, COUNT(*) as record_count FROM alert_records;

-- ========================================
-- 3. 更新操作测试
-- ========================================

-- 更新设备状态
UPDATE equipment_info SET status = 'MAINTENANCE' WHERE equipment_id = 'EQ003';

-- 更新客户信息
UPDATE customer_info SET contract_capacity = 5500.0 WHERE customer_id = 'CU001';

-- 更新告警状态
UPDATE alert_records SET status = 'RESOLVED', resolved_by = '运维工程师', resolved_at = NOW(), resolution_notes = '已调整设备参数，温度恢复正常' WHERE alert_id = (SELECT alert_id FROM alert_records WHERE equipment_id = 'EQ003' AND status = 'OPEN' LIMIT 1);

-- ========================================
-- 4. 删除操作测试（谨慎使用）
-- ========================================

-- 删除过期的用电数据（保留最近1小时的数据）
-- DELETE FROM power_consumption WHERE record_time < NOW() - INTERVAL '2 hours';

-- 删除已关闭的告警记录（保留用于测试）
-- DELETE FROM alert_records WHERE status = 'CLOSED' AND created_at < NOW() - INTERVAL '7 days';

-- ========================================
-- 5. 验证最终数据状态
-- ========================================

-- 查看最新的设备信息
SELECT equipment_id, equipment_name, equipment_type, status, updated_at 
FROM equipment_info 
ORDER BY updated_at DESC;

-- 查看最新的用电数据
SELECT customer_id, equipment_id, record_time, active_power, power_factor 
FROM power_consumption 
ORDER BY record_time DESC 
LIMIT 10;

-- 查看告警状态分布
SELECT status, COUNT(*) as count 
FROM alert_records 
GROUP BY status;

-- 查看设备健康度分布
SELECT risk_level, COUNT(*) as count 
FROM equipment_status 
WHERE status_time > NOW() - INTERVAL '1 hour'
GROUP BY risk_level;

COMMIT;