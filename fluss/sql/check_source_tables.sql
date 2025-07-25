-- 检查源表数据情况
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT 'dwd_dim_equipment' as table_name, COUNT(*) as record_count FROM dwd_dim_equipment;
SELECT 'dwd_dim_customer' as table_name, COUNT(*) as record_count FROM dwd_dim_customer;
SELECT 'dws_equipment_hour_summary' as table_name, COUNT(*) as record_count FROM dws_equipment_hour_summary;
SELECT 'dws_customer_hour_power' as table_name, COUNT(*) as record_count FROM dws_customer_hour_power;
SELECT 'dws_alert_hour_stats' as table_name, COUNT(*) as record_count FROM dws_alert_hour_stats;