-- ========================================
-- DWS层数据检查脚本（包含DDL初始化）
-- 可在任何新的Flink SQL CLI会话中独立运行
-- ========================================

-- 1. 首先初始化环境
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

-- 2. 创建 Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- 3. 切换到Fluss catalog
USE CATALOG fluss_catalog;
USE sgcc_dw;

-- 4. 检查DWS层表数据
SELECT 'dws_customer_hour_power' as table_name, COUNT(*) as total_count FROM dws_customer_hour_power;
SELECT 'dws_customer_hour_power_recent' as table_name, COUNT(*) as recent_count FROM dws_customer_hour_power WHERE stat_date >= CURRENT_DATE - INTERVAL '1' DAY;
SELECT 'dws_equipment_hour_summary' as table_name, COUNT(*) as total_count FROM dws_equipment_hour_summary;
SELECT 'dws_alert_hour_stats' as table_name, COUNT(*) as total_count FROM dws_alert_hour_stats;

-- 5. 检查最新数据时间
SELECT 'dws_customer_hour_power_latest' as info, MAX(stat_date) as latest_date FROM dws_customer_hour_power;
SELECT 'dws_equipment_hour_summary_latest' as info, MAX(stat_date) as latest_date FROM dws_equipment_hour_summary;
SELECT 'dws_alert_hour_stats_latest' as info, MAX(stat_date) as latest_date FROM dws_alert_hour_stats;