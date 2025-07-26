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

-- 为表和字段添加中文注释
-- 设备信息表注释
COMMENT ON TABLE equipment_info IS '设备信息表 - 存储电力设备的基本信息和配置参数';
COMMENT ON COLUMN equipment_info.equipment_id IS '设备编号 - 唯一标识设备的主键';
COMMENT ON COLUMN equipment_info.equipment_name IS '设备名称 - 设备的中文名称';
COMMENT ON COLUMN equipment_info.equipment_type IS '设备类型 - 变压器、开关、线路等设备分类';
COMMENT ON COLUMN equipment_info.location IS '设备位置 - 设备安装的物理位置描述';
COMMENT ON COLUMN equipment_info.voltage_level IS '电压等级 - 设备额定电压等级(kV)';
COMMENT ON COLUMN equipment_info.capacity IS '设备容量 - 设备额定容量(MVA)';
COMMENT ON COLUMN equipment_info.manufacturer IS '制造厂商 - 设备生产厂家名称';
COMMENT ON COLUMN equipment_info.install_date IS '安装日期 - 设备投运安装日期';
COMMENT ON COLUMN equipment_info.status IS '设备状态 - NORMAL正常/WARNING警告/FAULT故障/MAINTENANCE维护';
COMMENT ON COLUMN equipment_info.created_at IS '创建时间 - 记录创建时间戳';
COMMENT ON COLUMN equipment_info.updated_at IS '更新时间 - 记录最后更新时间戳';

-- 客户信息表注释
COMMENT ON TABLE customer_info IS '客户信息表 - 存储用电客户的基本信息和合同参数';
COMMENT ON COLUMN customer_info.customer_id IS '客户编号 - 唯一标识客户的主键';
COMMENT ON COLUMN customer_info.customer_name IS '客户名称 - 客户的正式名称';
COMMENT ON COLUMN customer_info.customer_type IS '客户类型 - 居民、工业、商业、农业等用电性质分类';
COMMENT ON COLUMN customer_info.contact_person IS '联系人 - 客户方联系人姓名';
COMMENT ON COLUMN customer_info.contact_phone IS '联系电话 - 客户方联系电话号码';
COMMENT ON COLUMN customer_info.address IS '客户地址 - 客户用电地址详细信息';
COMMENT ON COLUMN customer_info.contract_capacity IS '合同容量 - 客户签约的用电容量(kW)';
COMMENT ON COLUMN customer_info.voltage_level IS '供电电压等级 - 客户接入的电压等级(kV)';
COMMENT ON COLUMN customer_info.tariff_type IS '电价类型 - 客户适用的电价政策分类';
COMMENT ON COLUMN customer_info.status IS '客户状态 - ACTIVE正常/SUSPENDED暂停/TERMINATED终止';
COMMENT ON COLUMN customer_info.created_at IS '创建时间 - 记录创建时间戳';
COMMENT ON COLUMN customer_info.updated_at IS '更新时间 - 记录最后更新时间戳';

-- 用电数据表注释
COMMENT ON TABLE power_consumption IS '用电数据表 - 存储客户实时用电量和电能质量数据';
COMMENT ON COLUMN power_consumption.consumption_id IS '用电记录ID - 用电数据记录的唯一标识';
COMMENT ON COLUMN power_consumption.customer_id IS '客户编号 - 关联客户信息表的外键';
COMMENT ON COLUMN power_consumption.equipment_id IS '设备编号 - 关联设备信息表的外键';
COMMENT ON COLUMN power_consumption.record_time IS '记录时间 - 用电数据采集时间戳';
COMMENT ON COLUMN power_consumption.active_power IS '有功功率 - 实际消耗的电功率(kW)';
COMMENT ON COLUMN power_consumption.reactive_power IS '无功功率 - 电感性或电容性功率(kVar)';
COMMENT ON COLUMN power_consumption.voltage_a IS 'A相电压 - A相线电压有效值(V)';
COMMENT ON COLUMN power_consumption.voltage_b IS 'B相电压 - B相线电压有效值(V)';
COMMENT ON COLUMN power_consumption.voltage_c IS 'C相电压 - C相线电压有效值(V)';
COMMENT ON COLUMN power_consumption.current_a IS 'A相电流 - A相线电流有效值(A)';
COMMENT ON COLUMN power_consumption.current_b IS 'B相电流 - B相线电流有效值(A)';
COMMENT ON COLUMN power_consumption.current_c IS 'C相电流 - C相线电流有效值(A)';
COMMENT ON COLUMN power_consumption.power_factor IS '功率因数 - 有功功率与视在功率的比值';
COMMENT ON COLUMN power_consumption.frequency IS '频率 - 交流电频率(Hz)';
COMMENT ON COLUMN power_consumption.energy_consumption IS '累计电量 - 累计用电量(kWh)';
COMMENT ON COLUMN power_consumption.created_at IS '创建时间 - 记录创建时间戳';

-- 设备状态表注释
COMMENT ON TABLE equipment_status IS '设备状态表 - 存储设备运行状态和健康监测数据';
COMMENT ON COLUMN equipment_status.status_id IS '状态记录ID - 设备状态记录的唯一标识';
COMMENT ON COLUMN equipment_status.equipment_id IS '设备编号 - 关联设备信息表的外键';
COMMENT ON COLUMN equipment_status.status_time IS '状态时间 - 设备状态数据采集时间戳';
COMMENT ON COLUMN equipment_status.operating_status IS '运行状态 - RUNNING运行/STOPPED停机/FAULT故障/MAINTENANCE维护';
COMMENT ON COLUMN equipment_status.temperature IS '设备温度 - 设备运行温度(℃)';
COMMENT ON COLUMN equipment_status.pressure IS '设备压力 - 设备内部压力(MPa)';
COMMENT ON COLUMN equipment_status.vibration IS '振动幅度 - 设备振动强度(mm/s)';
COMMENT ON COLUMN equipment_status.oil_level IS '油位高度 - 变压器油位高度(mm)';
COMMENT ON COLUMN equipment_status.insulation_resistance IS '绝缘电阻 - 设备绝缘电阻值(MΩ)';
COMMENT ON COLUMN equipment_status.load_rate IS '负载率 - 设备当前负载占额定容量的百分比(%)';
COMMENT ON COLUMN equipment_status.efficiency IS '运行效率 - 设备运行效率百分比(%)';
COMMENT ON COLUMN equipment_status.health_score IS '健康度评分 - 设备健康状况综合评分(0-100)';
COMMENT ON COLUMN equipment_status.risk_level IS '风险等级 - LOW低风险/MEDIUM中风险/HIGH高风险/CRITICAL严重';
COMMENT ON COLUMN equipment_status.created_at IS '创建时间 - 记录创建时间戳';

-- 告警记录表注释
COMMENT ON TABLE alert_records IS '告警记录表 - 存储设备和客户的告警信息及处理记录';
COMMENT ON COLUMN alert_records.alert_id IS '告警ID - 告警记录的唯一标识';
COMMENT ON COLUMN alert_records.equipment_id IS '设备编号 - 关联设备信息表的外键';
COMMENT ON COLUMN alert_records.customer_id IS '客户编号 - 关联客户信息表的外键';
COMMENT ON COLUMN alert_records.alert_type IS '告警类型 - EQUIPMENT_FAULT设备故障/POWER_ANOMALY电力异常/VOLTAGE_ABNORMAL电压异常/OVERLOAD过载';
COMMENT ON COLUMN alert_records.alert_level IS '告警级别 - INFO信息/WARNING警告/ERROR错误/CRITICAL严重';
COMMENT ON COLUMN alert_records.alert_title IS '告警标题 - 告警事件的简要描述';
COMMENT ON COLUMN alert_records.alert_description IS '告警描述 - 告警事件的详细描述信息';
COMMENT ON COLUMN alert_records.alert_time IS '告警时间 - 告警事件发生的时间戳';
COMMENT ON COLUMN alert_records.alert_value IS '告警数值 - 触发告警的实际测量值';
COMMENT ON COLUMN alert_records.threshold_value IS '阈值 - 触发告警的阈值设定';
COMMENT ON COLUMN alert_records.status IS '处理状态 - OPEN未处理/ACKNOWLEDGED已确认/RESOLVED已解决/CLOSED已关闭';
COMMENT ON COLUMN alert_records.acknowledged_by IS '确认人 - 确认告警的操作人员';
COMMENT ON COLUMN alert_records.acknowledged_at IS '确认时间 - 告警确认的时间戳';
COMMENT ON COLUMN alert_records.resolved_by IS '解决人 - 解决告警的操作人员';
COMMENT ON COLUMN alert_records.resolved_at IS '解决时间 - 告警解决的时间戳';
COMMENT ON COLUMN alert_records.resolution_notes IS '解决备注 - 告警解决的详细说明';
COMMENT ON COLUMN alert_records.created_at IS '创建时间 - 记录创建时间戳';
COMMENT ON COLUMN alert_records.updated_at IS '更新时间 - 记录最后更新时间戳';

COMMIT;