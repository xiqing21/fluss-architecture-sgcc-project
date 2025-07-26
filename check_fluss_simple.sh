#!/bin/bash

# ========================================
# Fluss数据检查脚本（简化版）
# 解决Flink SQL CLI会话消失问题
# ========================================

echo "=== Fluss数据检查脚本（简化版） ==="
echo "正在创建临时SQL文件..."

# 在容器内创建临时SQL文件
docker exec sql-client-sgcc bash -c "cat > /tmp/check_fluss_simple.sql << 'EOF'
-- 1. 创建 Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- 2. 显示catalog状态
SHOW CATALOGS;

-- 3. 切换到Fluss catalog
USE CATALOG fluss_catalog;
USE sgcc_dw;

-- 4. 显示表状态
SHOW TABLES;
EOF"

echo "正在执行Flink SQL查询..."

# 执行SQL文件
docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/check_fluss_simple.sql

echo "=== 第一步完成，现在检查数据 ==="

# 创建数据查询脚本
docker exec sql-client-sgcc bash -c "cat > /tmp/query_data.sql << 'EOF'
-- 重新初始化环境
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE sgcc_dw;

-- 使用简单的查询方式
SELECT COUNT(*) FROM dws_customer_hour_power;
EOF"

echo "正在查询dws_customer_hour_power数据..."
docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/query_data.sql

echo "=== 检查完成 ==="