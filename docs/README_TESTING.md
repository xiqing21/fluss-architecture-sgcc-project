# 国网风控数仓 - CRUD测试指南

本文档介绍如何使用提供的测试脚本来验证从PostgreSQL Source到Sink的完整数据流。

## 📋 测试概述

本测试套件包含以下功能：
1. **基础CRUD测试** - 验证基本的增删改查操作
2. **数据流验证** - 检查Source和Sink端的数据一致性
3. **大数据量测试** - 生成大量数据测试系统性能
4. **延迟监控** - 实时监控数据传输延迟
5. **性能分析** - 计算整体传输和计算延迟

## 🛠️ 环境准备

### 1. 确保Docker环境运行
```bash
# 启动所有服务
docker-compose up -d

# 检查容器状态
docker ps
```

### 2. 安装Python依赖
```bash
pip3 install psycopg2-binary
```

### 3. 验证数据库连接
```bash
# 测试Source数据库
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d source_db -c "SELECT 1;"

# 测试Sink数据库
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d sink_db -c "SELECT 1;"
```

## 🚀 快速开始

### 一键完整测试
```bash
./run_complete_test.sh
```

这个脚本会自动执行所有测试步骤，包括：
- 环境检查
- 基础CRUD测试
- 数据验证
- 大数据量测试（后台运行）
- 延迟监控（后台运行）
- 生成测试报告

## 📊 分步测试

### 1. 基础CRUD测试
```bash
# 执行基础CRUD操作
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d source_db -f test_crud_operations.sql

# 等待30秒让数据处理
sleep 30

# 检查结果
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d source_db -c "SELECT COUNT(*) FROM equipment_info;"
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d sink_db -c "SELECT COUNT(*) FROM ads_equipment_health;"
```

### 2. 数据流验证
```bash
# Source端验证
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d source_db -f verify_data_flow.sql > source_verification.log

# Sink端验证
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d sink_db -f verify_data_flow.sql > sink_verification.log

# 查看结果
cat source_verification.log
cat sink_verification.log
```

### 3. 大数据量测试
```bash
# 运行大数据量测试（可能需要几分钟）
python3 generate_test_data.py
```

**测试参数说明：**
- `equipment_count`: 设备数量（默认2000）
- `customer_count`: 客户数量（默认1000）
- `power_records`: 电力消耗记录（默认20000）
- `status_records`: 设备状态记录（默认10000）
- `alert_records`: 告警记录（默认2000）

### 4. 延迟监控
```bash
# 启动实时延迟监控
python3 latency_monitor.py
```

监控功能：
- 实时显示延迟统计
- 分表延迟分析
- 系统性能指标
- 自动生成延迟报告

按 `Ctrl+C` 停止监控并生成最终报告。

## 📈 结果分析

### 测试输出文件

| 文件名 | 描述 |
|--------|------|
| `source_verification.log` | Source数据库验证结果 |
| `sink_verification.log` | Sink数据库验证结果 |
| `load_test.log` | 大数据量测试日志 |
| `latency_monitor.log` | 延迟监控日志 |
| `latency_report_*.json` | 详细延迟报告（JSON格式） |
| `test_report_*.md` | 综合测试报告 |

### 关键指标

#### 1. 数据一致性
- Source端记录数 vs Sink端记录数
- 数据类型匹配度
- 时间戳准确性

#### 2. 性能指标
- **吞吐量**: 记录/秒
- **延迟**: 数据从Source到Sink的时间
- **P95/P99延迟**: 95%/99%的请求延迟

#### 3. 系统资源
- 数据库连接数
- 表大小增长
- 内存使用情况

## 🔧 故障排除

### 常见问题

#### 1. 数据库连接失败
```bash
# 检查容器状态
docker ps

# 重启容器
docker-compose restart postgres-source postgres-sink

# 检查端口占用
lsof -i :5432
lsof -i :5433
```

#### 2. Flink任务失败
```bash
# 检查Flink任务状态
docker exec sql-client-sgcc flink list

# 查看任务日志
docker logs sql-client-sgcc

# 重启Flink任务
docker exec sql-client-sgcc flink run -d /tmp/fluss_all_chain.sql
```

#### 3. 数据延迟过高
- 检查网络连接
- 验证Flink集群资源
- 查看数据库性能指标
- 检查CDC配置

#### 4. Python脚本错误
```bash
# 检查Python依赖
pip3 list | grep psycopg2

# 重新安装依赖
pip3 install --upgrade psycopg2-binary

# 检查数据库配置
python3 -c "import psycopg2; print('psycopg2 OK')"
```

## 📝 自定义测试

### 修改测试参数

编辑 `generate_test_data.py` 中的 `main()` 函数：

```python
generator.run_load_test(
    equipment_count=5000,    # 增加设备数量
    customer_count=2000,     # 增加客户数量
    power_records=50000,     # 增加电力记录
    status_records=25000,    # 增加状态记录
    alert_records=5000,      # 增加告警记录
    update_count=500         # 增加更新操作
)
```

### 修改数据库配置

编辑脚本中的数据库配置：

```python
source_config = {
    'host': 'your_host',
    'port': 5432,
    'database': 'your_source_db',
    'user': 'your_user',
    'password': 'your_password'
}
```

### 添加自定义监控指标

在 `latency_monitor.py` 中添加新的监控表：

```python
table_mappings.append({
    'source_table': 'your_source_table',
    'sink_table': 'your_sink_table',
    'source_time_col': 'created_at',
    'sink_time_col': 'updated_at'
})
```

## 🎯 性能基准

### 预期性能指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 数据延迟 | < 30秒 | 从Source到Sink的端到端延迟 |
| 吞吐量 | > 1000条/秒 | 数据处理速度 |
| 数据一致性 | 100% | Source和Sink数据匹配度 |
| 系统可用性 | > 99% | 服务正常运行时间 |

### 优化建议

1. **数据库优化**
   - 调整PostgreSQL配置参数
   - 优化索引策略
   - 增加连接池大小

2. **Flink优化**
   - 调整并行度
   - 优化检查点配置
   - 增加内存分配

3. **网络优化**
   - 使用更快的网络连接
   - 减少网络跳数
   - 启用数据压缩

## 📞 支持

如果遇到问题，请检查：
1. 所有Docker容器是否正常运行
2. 数据库连接是否正常
3. Flink任务是否成功启动
4. 网络连接是否稳定

更多详细信息请查看各个日志文件和生成的测试报告。