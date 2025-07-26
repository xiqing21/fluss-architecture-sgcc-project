-- ========================================
-- Fluss DDL 初始化脚本
-- 用于在新的Flink SQL CLI会话中重新创建所有DDL定义
-- ========================================

-- 设置检查点间隔
SET 'execution.checkpointing.interval' = '30s';

-- 设置状态TTL，解决UPDATE和DELETE操作的状态管理问题
SET 'table.exec.state.ttl' = '1h';

-- 设置空闲状态保留时间
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '1000';

-- 优化CDC处理性能
SET 'table.optimizer.join-reorder-enabled' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'auto';

-- 创建 Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- 显示所有catalog
SHOW CATALOGS;

-- 切换到Fluss catalog
USE CATALOG fluss_catalog;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS sgcc_dw;
USE sgcc_dw;

-- 显示所有数据库
SHOW DATABASES;

-- 显示当前数据库中的所有表
SHOW TABLES;