#!/bin/bash

# ========================================
# 执行INSERT脚本 - 处理数据流
# 前提：已执行run_ddl.sh创建表结构
# ========================================

echo "=== 执行INSERT脚本 - 数据处理 ==="
echo "前提：请确保已执行run_ddl.sh创建表结构"
echo "正在执行insert_only.sql..."

# 执行INSERT脚本
docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /opt/flink/insert_only.sql

echo ""
echo "=== INSERT脚本执行完成 ==="
echo "数据处理任务已启动"
echo "检查结果请执行: ./check_ads_result.sh"