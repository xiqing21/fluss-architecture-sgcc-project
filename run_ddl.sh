#!/bin/bash

# ========================================
# 执行DDL脚本 - 创建所有表结构
# ========================================

echo "=== 执行DDL脚本 - 创建表结构 ==="
echo "正在执行ddl_only.sql..."

# 执行DDL脚本
docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /opt/flink/ddl_only.sql

echo ""
echo "=== DDL脚本执行完成 ==="
echo "所有表结构已创建，可以重复执行此脚本"
echo "下一步请执行: ./run_insert.sh"