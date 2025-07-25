-- 国网风控数仓分层架构 - PostgreSQL Source数据库初始化脚本
-- 创建5个核心业务表：设备信息、客户信息、用电数据、设备状态、告警记录

-- 1. 设备信息表 (equipment_info)
CREATE TABLE IF NOT EXISTS equipment_info (
    equipment_id VARCHAR(50) PRIMARY KEY,
    equipment_name VARCHAR(100) NOT NULL,
    equipment_type VARCHAR(50) NOT NULL, -- 变压器、开关、线路等
    location VARCHAR(200) NOT NULL,
    voltage_level INTEGER NOT NULL, -- 电压等级(kV)
    capacity DECIMAL(10,2), -- 容量(MVA)
    manufacturer VARCHAR(100),
    install_date DATE,
    status VARCHAR(20) DEFAULT 'NORMAL', -- NORMAL, WARNING, FAULT, MAINTENANCE
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 客户信息表 (customer_info)
CREATE TABLE IF NOT EXISTS customer_info (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_type VARCHAR(50) NOT NULL, -- 居民、工业、商业、农业
    contact_person VARCHAR(50),
    contact_phone VARCHAR(20),
    address VARCHAR(200),
    contract_capacity DECIMAL(10,2), -- 合同容量(kW)
    voltage_level INTEGER, -- 供电电压等级
    tariff_type VARCHAR(50), -- 电价类型
    status VARCHAR(20) DEFAULT 'ACTIVE', -- ACTIVE, SUSPENDED, TERMINATED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. 用电数据表 (power_consumption)
CREATE TABLE IF NOT EXISTS power_consumption (
    consumption_id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    equipment_id VARCHAR(50) NOT NULL,
    record_time TIMESTAMP NOT NULL,
    active_power DECIMAL(10,4), -- 有功功率(kW)
    reactive_power DECIMAL(10,4), -- 无功功率(kVar)
    voltage_a DECIMAL(8,2), -- A相电压(V)
    voltage_b DECIMAL(8,2), -- B相电压(V)
    voltage_c DECIMAL(8,2), -- C相电压(V)
    current_a DECIMAL(8,2), -- A相电流(A)
    current_b DECIMAL(8,2), -- B相电流(A)
    current_c DECIMAL(8,2), -- C相电流(A)
    power_factor DECIMAL(4,3), -- 功率因数
    frequency DECIMAL(5,2), -- 频率(Hz)
    energy_consumption DECIMAL(12,4), -- 累计电量(kWh)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customer_info(customer_id),
    FOREIGN KEY (equipment_id) REFERENCES equipment_info(equipment_id)
);

-- 4. 设备状态表 (equipment_status)
CREATE TABLE IF NOT EXISTS equipment_status (
    status_id BIGSERIAL PRIMARY KEY,
    equipment_id VARCHAR(50) NOT NULL,
    status_time TIMESTAMP NOT NULL,
    operating_status VARCHAR(20) NOT NULL, -- RUNNING, STOPPED, FAULT, MAINTENANCE
    temperature DECIMAL(5,2), -- 温度(℃)
    pressure DECIMAL(8,2), -- 压力(MPa)
    vibration DECIMAL(6,3), -- 振动(mm/s)
    oil_level DECIMAL(5,2), -- 油位(mm)
    insulation_resistance DECIMAL(10,2), -- 绝缘电阻(MΩ)
    load_rate DECIMAL(5,2), -- 负载率(%)
    efficiency DECIMAL(5,2), -- 效率(%)
    health_score DECIMAL(5,2), -- 健康度评分(0-100)
    risk_level VARCHAR(20) DEFAULT 'LOW', -- LOW, MEDIUM, HIGH, CRITICAL
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (equipment_id) REFERENCES equipment_info(equipment_id)
);

-- 5. 告警记录表 (alert_records)
CREATE TABLE IF NOT EXISTS alert_records (
    alert_id BIGSERIAL PRIMARY KEY,
    equipment_id VARCHAR(50),
    customer_id VARCHAR(50),
    alert_type VARCHAR(50) NOT NULL, -- EQUIPMENT_FAULT, POWER_ANOMALY, VOLTAGE_ABNORMAL, OVERLOAD
    alert_level VARCHAR(20) NOT NULL, -- INFO, WARNING, ERROR, CRITICAL
    alert_title VARCHAR(200) NOT NULL,
    alert_description TEXT,
    alert_time TIMESTAMP NOT NULL,
    alert_value DECIMAL(12,4), -- 告警数值
    threshold_value DECIMAL(12,4), -- 阈值
    status VARCHAR(20) DEFAULT 'OPEN', -- OPEN, ACKNOWLEDGED, RESOLVED, CLOSED
    acknowledged_by VARCHAR(50),
    acknowledged_at TIMESTAMP,
    resolved_by VARCHAR(50),
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (equipment_id) REFERENCES equipment_info(equipment_id),
    FOREIGN KEY (customer_id) REFERENCES customer_info(customer_id)
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_power_consumption_customer_time ON power_consumption(customer_id, record_time);
CREATE INDEX IF NOT EXISTS idx_power_consumption_equipment_time ON power_consumption(equipment_id, record_time);
CREATE INDEX IF NOT EXISTS idx_equipment_status_equipment_time ON equipment_status(equipment_id, status_time);
CREATE INDEX IF NOT EXISTS idx_alert_records_equipment_time ON alert_records(equipment_id, alert_time);
CREATE INDEX IF NOT EXISTS idx_alert_records_customer_time ON alert_records(customer_id, alert_time);
CREATE INDEX IF NOT EXISTS idx_alert_records_status ON alert_records(status);
CREATE INDEX IF NOT EXISTS idx_alert_records_level ON alert_records(alert_level);

-- 创建更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为需要的表创建更新时间触发器
CREATE TRIGGER update_equipment_info_updated_at BEFORE UPDATE ON equipment_info
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_customer_info_updated_at BEFORE UPDATE ON customer_info
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_alert_records_updated_at BEFORE UPDATE ON alert_records
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 启用CDC复制标识
ALTER TABLE equipment_info REPLICA IDENTITY FULL;
ALTER TABLE customer_info REPLICA IDENTITY FULL;
ALTER TABLE power_consumption REPLICA IDENTITY FULL;
ALTER TABLE equipment_status REPLICA IDENTITY FULL;
ALTER TABLE alert_records REPLICA IDENTITY FULL;

COMMIT;