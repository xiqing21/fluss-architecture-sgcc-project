#!/bin/bash

# ========================================
# ADS Power Quality 调试脚本
# 完整解决方案：检查数据流和修复问题
# ========================================

echo "=== ADS Power Quality 调试脚本 ==="
echo "正在创建调试SQL文件..."

# 在容器内创建调试SQL文件
docker exec sql-client-sgcc bash -c "cat > /tmp/debug_power_quality.sql << 'EOF'
-- 设置结果显示模式
SET 'sql-client.execution.result-mode' = 'tableau';

-- 创建 Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- 切换到Fluss catalog
USE CATALOG fluss_catalog;
USE sgcc_dw;

-- 显示所有表
SHOW TABLES;
EOF"

echo "第1步：检查Fluss环境和表结构"
docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/debug_power_quality.sql

echo ""
echo "第2步：检查DWS层数据量"

# 创建数据量检查脚本
docker exec sql-client-sgcc bash -c "cat > /tmp/check_data_count.sql << 'EOF'
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE sgcc_dw;

SELECT COUNT(*) as dws_customer_hour_power_count FROM dws_customer_hour_power;
EOF"

docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/check_data_count.sql

echo ""
echo "第3步：检查DWS层最新数据"

# 创建最新数据检查脚本
docker exec sql-client-sgcc bash -c "cat > /tmp/check_latest_data.sql << 'EOF'
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE sgcc_dw;

SELECT MAX(stat_date) as latest_stat_date FROM dws_customer_hour_power;
EOF"

docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/check_latest_data.sql

echo ""
echo "第4步：检查源数据DWD层"

# 检查DWD层数据
docker exec sql-client-sgcc bash -c "cat > /tmp/check_dwd_data.sql << 'EOF'
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE sgcc_dw;

SELECT COUNT(*) as dwd_fact_power_consumption_count FROM dwd_fact_power_consumption;
EOF"

docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/check_dwd_data.sql

echo ""
echo "=== 调试完成 ==="
echo "如果DWS层没有数据，问题可能是："
echo "1. DWD层数据不足"
echo "2. DWS层聚合任务没有正确运行"
echo "3. 时间窗口设置问题"
echo "请检查Flink任务状态：docker exec jobmanager-sgcc flink list"