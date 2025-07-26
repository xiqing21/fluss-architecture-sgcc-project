# ADS层数据更新问题解决方案及CRUD功能说明

## 问题分析

### 为什么只有 `ads_energy_efficiency` 表数据持续增长？

经过分析发现，问题的根本原因在于**主键冲突**：

1. **ads_energy_efficiency 表**：使用 `HASH_CODE(CONCAT(scope_type, scope_id, CAST(CURRENT_TIMESTAMP AS STRING)))` 生成唯一ID，每次执行都会产生不同的主键，因此能持续插入新数据。

2. **其他ADS表的问题**：
   - `ads_realtime_dashboard`：使用固定的 `metric_id` (1, 2, 3)，导致主键冲突
   - `ads_equipment_health`：使用 `HASH_CODE(equipment_id)`，同一设备ID总是生成相同的hash值
   - `ads_customer_behavior`：使用 `HASH_CODE(customer_id)`，同一客户ID总是生成相同的hash值
   - `ads_alert_statistics`：使用固定的 `stat_id = 1`
   - `ads_power_quality`：已经使用了时间戳，应该能正常更新
   - `ads_risk_assessment`：已经使用了时间戳，应该能正常更新

## 解决方案

### 修改主键生成策略

将所有ADS表的主键生成方式修改为包含时间戳，确保每次执行都能生成唯一的主键：

```sql
-- 修改前（会导致主键冲突）
CAST(HASH_CODE(equipment_id) AS BIGINT) as analysis_id

-- 修改后（支持持续更新）
CAST(HASH_CODE(CONCAT(equipment_id, CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as analysis_id
```

### 具体修改内容

1. **ads_realtime_dashboard**：
   ```sql
   -- 修改前
   1 as metric_id, 2 as metric_id, 3 as metric_id
   
   -- 修改后
   CAST(HASH_CODE(CONCAT('total_active_power', CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as metric_id
   CAST(HASH_CODE(CONCAT('avg_equipment_health', CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as metric_id
   CAST(HASH_CODE(CONCAT('total_alerts_today', CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as metric_id
   ```

2. **ads_equipment_health**：
   ```sql
   -- 修改前
   CAST(HASH_CODE(e.equipment_id) AS BIGINT) as analysis_id
   
   -- 修改后
   CAST(HASH_CODE(CONCAT(e.equipment_id, CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as analysis_id
   ```

3. **ads_customer_behavior**：
   ```sql
   -- 修改前
   CAST(HASH_CODE(c.customer_id) AS BIGINT) as behavior_id
   
   -- 修改后
   CAST(HASH_CODE(CONCAT(c.customer_id, CAST(CURRENT_TIMESTAMP AS STRING))) AS BIGINT) as behavior_id
   ```

4. **ads_alert_statistics**：
   ```sql
   -- 修改前
   1 as stat_id
   
   -- 修改后
   CAST(HASH_CODE(CAST(CURRENT_TIMESTAMP AS STRING)) AS BIGINT) as stat_id
   ```

## CRUD功能说明

### 1. CREATE (插入) - 已实现
所有ADS表的INSERT语句已经优化，支持持续插入新数据。

### 2. READ (查询) - 示例

#### 基础查询
```sql
-- 查询最新的实时监控指标
SELECT metric_name, metric_value, metric_unit, update_time
FROM ads_realtime_dashboard
WHERE DATE(created_at) = CURRENT_DATE
ORDER BY update_time DESC;

-- 查询高风险设备
SELECT equipment_id, equipment_name, health_score, risk_level, recommendation
FROM ads_equipment_health
WHERE risk_level IN ('HIGH', 'CRITICAL')
AND DATE(analysis_time) = CURRENT_DATE
ORDER BY health_score ASC;
```

#### 复合查询
```sql
-- 综合设备状态查询
SELECT 
    h.equipment_id,
    h.equipment_name,
    h.health_score,
    h.risk_level,
    q.overall_quality_score,
    q.quality_grade,
    ef.efficiency_ratio,
    ef.efficiency_grade
FROM ads_equipment_health h
JOIN ads_power_quality q ON h.equipment_id = q.equipment_id
JOIN ads_energy_efficiency ef ON h.equipment_id = ef.scope_id
WHERE DATE(h.analysis_time) = CURRENT_DATE
AND DATE(q.analysis_time) = CURRENT_DATE
AND DATE(ef.analysis_time) = CURRENT_DATE
AND ef.analysis_scope = 'EQUIPMENT'
ORDER BY h.health_score DESC;
```

### 3. UPDATE (更新) - 示例

```sql
-- 更新设备健康度分析结果
UPDATE ads_equipment_health 
SET health_score = 95.0, risk_level = 'LOW', recommendation = '设备状态优秀'
WHERE equipment_id = 'EQ001' AND analysis_time >= CURRENT_DATE;

-- 更新客户用电行为分析
UPDATE ads_customer_behavior 
SET efficiency_rating = 'EXCELLENT', cost_estimation = total_consumption * 0.75
WHERE customer_id = 'CUST001' AND analysis_time >= CURRENT_DATE;

-- 更新实时监控大屏指标
UPDATE ads_realtime_dashboard 
SET metric_value = 1500.0, update_time = CURRENT_TIMESTAMP
WHERE metric_name = 'total_active_power' AND DATE(created_at) = CURRENT_DATE;
```

### 4. DELETE (删除) - 示例

```sql
-- 删除过期的实时监控数据（保留最近7天）
DELETE FROM ads_realtime_dashboard 
WHERE created_at < CURRENT_DATE - INTERVAL '7' DAY;

-- 删除过期的设备健康度分析（保留最近30天）
DELETE FROM ads_equipment_health 
WHERE analysis_time < CURRENT_DATE - INTERVAL '30' DAY;

-- 删除过期的客户行为分析（保留最近30天）
DELETE FROM ads_customer_behavior 
WHERE analysis_time < CURRENT_DATE - INTERVAL '30' DAY;
```

## 数据更新验证

### 验证方法
1. 执行修改后的SQL脚本
2. 查看各ADS表的数据量变化
3. 确认所有表都能持续接收新数据

### 预期结果
- 所有ADS表（ads_realtime_dashboard、ads_equipment_health、ads_customer_behavior、ads_