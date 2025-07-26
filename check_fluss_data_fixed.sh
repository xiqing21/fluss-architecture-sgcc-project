#!/bin/bash

# ========================================
# Fluss数据检查脚本（修复版）
# 解决Flink SQL CLI会话消失问题的完整方案
# ========================================

echo "=== Fluss数据检查脚本（修复版） ==="
echo "正在创建临时SQL文件..."

# 在容器内创建临时SQL文件
docker exec sql-client-sgcc bash -c "cat > /tmp/check_fluss_data_fixed.sql << 'EOF'
-- ========================================
-- DWS层数据检查脚本（包含DDL初始化）
-- 可在任何新的Flink SQL CLI会话中独立运行
-- ========================================

-- 1. 设置结果模式
SET sql-client.execution.result-mode=TABLEAU;

-- 2. 初始化环境
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

-- 3. 创建 Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- 4. 显示catalog状态
SHOW CATALOGS;

-- 5. 切换到Fluss catalog
USE CATALOG fluss_catalog;
USE sgcc_dw;

-- 6. 显示表状态
SHOW TABLES;

-- 7. 检查DWS层表数据
SELECT 'dws_customer_hour_power' as table_name, COUNT(*) as total_count FROM dws_customer_hour_power;
SELECT 'dws_equipment_hour_summary' as table_name, COUNT(*) as total_count FROM dws_equipment_hour_summary;
SELECT 'dws_alert_hour_stats' as table_name, COUNT(*) as total_count FROM dws_alert_hour_stats;

-- 8. 检查最新数据时间
SELECT 'dws_customer_hour_power_latest' as info, MAX(stat_date) as latest_date FROM dws_customer_hour_power;
SELECT 'dws_equipment_hour_summary_latest' as info, MAX(stat_date) as latest_date FROM dws_equipment_hour_summary;
SELECT 'dws_alert_hour_stats_latest' as info, MAX(stat_date) as latest_date FROM dws_alert_hour_stats;
EOF"

echo "正在执行Flink SQL查询..."

# 执行SQL文件
docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/check_fluss_data_fixed.sql

echo "=== 检查完成 ==="