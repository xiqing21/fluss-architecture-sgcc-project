#!/bin/bash

# ========================================
# 详细检查源数据脚本
# 检查所有相关表的数据情况
# ========================================

echo "=== 详细检查源数据 ==="
echo "正在创建数据检查SQL文件..."

# 在容器内创建数据检查SQL文件
docker exec sql-client-sgcc bash -c "cat > /tmp/check_detailed.sql << 'EOF'
-- ========================================
-- 详细数据检查脚本
-- ========================================

-- 1. 设置执行环境
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

-- 2. 创建 Fluss Catalog
CREATE CATALOG IF NOT EXISTS