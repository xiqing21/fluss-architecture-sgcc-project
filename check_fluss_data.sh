#!/bin/bash

# ========================================
# Fluss数据检查脚本
# 解决Flink SQL CLI会话消失问题的完整方案
# ========================================

echo "=== Fluss数据检查脚本 ==="
echo "正在创建临时SQL文件..."

# 在容器内创建临时SQL文件
docker exec sql-client-sgcc bash -c "cat > /tmp/check_fluss_data.sql << 'EOF'
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

-- 3. 显示catalog状态
SHOW CATALOGS;

-- 4. 切换到Fluss catalog
USE CATALOG fluss_catalog;
USE sgcc_dw;

-- 5. 显示表状态
SHOW TABLES;

-- 6. 检查DWS层表数据
SELECT 'dws_customer_hour_power' as table_name, COUNT(*) as total_count FROM dws_customer_hour_power;
SELECT 'dws_equipment_hour_summary' as table_name, COUNT(*) as total_count FROM dws_equipment_hour_summary;
SELECT 'dws_alert_hour_stats' as table_name, COUNT(*) as total_count FROM dws_alert_hour_stats;
EOF"

echo "正在执行Flink SQL查询..."

# 执行SQL文件
docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/check_fluss_data.sql

echo "=== 检查完成 ==="