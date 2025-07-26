#!/bin/bash

# ========================================
# 检查ADS层数据结果
# ========================================

echo "=== 检查ADS层数据结果 ==="

echo "第1步：检查ads_power_quality表记录数"
docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "SELECT 'ads_power_quality' as table_name, COUNT(*) as record_count FROM ads_power_quality;"

echo ""
echo "第2步：查看最新5条电能质量数据"
docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "SELECT equipment_id, equipment_name, overall_quality, quality_grade, improvement_suggestions, created_at FROM ads_power_quality ORDER BY created_at DESC LIMIT 5;"

echo ""
echo "第3步：检查质量等级分布"
docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "SELECT quality_grade, COUNT(*) as count FROM ads_power_quality GROUP BY quality_grade ORDER BY count DESC;"

echo ""
echo "第4步：检查Fluss catalog中的表状态"
docker exec sql-client-sgcc bash -c "cat > /tmp/check_fluss_status.sql << 'EOF'
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;
USE sgcc_dw;
SHOW TABLES;
SELECT COUNT(*) as dwd_equipment_count FROM dwd_dim_equipment;
EOF" && docker exec sql-client-sgcc /opt/flink/bin/sql-client.sh -f /tmp/check_fluss_status.sql

echo ""
echo "=== 检查完成 ==="
echo "如果ads_power_quality有数据，说明修复成功"
echo "如果没有数据，请检查源数据和Flink任务状态"