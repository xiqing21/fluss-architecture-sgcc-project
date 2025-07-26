# Grafana 大屏修复指南

## 问题分析

### 1. 登录失败问题
- **原因**: Grafana 默认用户名是 `admin`，但之前配置中没有明确设置用户名
- **解决**: 在 `docker-compose-grafana.yml` 中添加了 `GF_SECURITY_ADMIN_USER=admin`

### 2. PostgreSQL Sink 重复问题
- **原因**: `docker-compose-grafana.yml` 和 `docker-compose.yml` 都定义了 PostgreSQL sink 服务
- **解决**: 移除了 `docker-compose-grafana.yml` 中的重复 PostgreSQL 服务，统一使用主 compose 文件中的 `postgres-sgcc-sink`

### 3. 网络连接问题
- **原因**: Grafana 使用了独立的网络，无法连接到主 compose 文件中的 PostgreSQL
- **解决**: 修改 Grafana 使用 `fluss-sgcc-network` 外部网络

### 4. 数据源配置错误
- **原因**: Grafana 数据源配置指向错误的容器名 `sgcc-postgres-sink`
- **解决**: 修正为正确的容器名 `postgres-sgcc-sink`

## 修复内容

### 1. 修改 `docker-compose-grafana.yml`
- 添加明确的管理员用户名配置
- 移除重复的 PostgreSQL 服务
- 修改网络配置为外部网络 `fluss-sgcc-network`

### 2. 修改 `grafana/provisioning/datasources/postgres.yml`
- 修正数据库连接 URL 为 `postgres-sgcc-sink:5432`

## 正确的启动步骤

### 1. 启动主服务（包含 PostgreSQL sink）
```bash
docker-compose up -d
```

### 2. 等待主服务完全启动后，启动 Grafana
```bash
docker-compose -f docker-compose-grafana.yml up -d
```

### 3. 访问 Grafana
- URL: http://localhost:3000
- 用户名: `admin`
- 密码: `admin123`

## 数据流说明

```
PostgreSQL Source (5442) 
    ↓ (Flink CDC)
Fluss 流处理
    ↓ (Flink Sink)
PostgreSQL Sink (5443) ← Grafana 读取这里的数据
```

- **Source 数据库**: `postgres-sgcc-source:5442` - 原始业务数据
- **Sink 数据库**: `postgres-sgcc-sink:5443` - Flink 处理后的 ADS 层数据
- **Grafana 读取**: 连接到 Sink 数据库，展示实时处理后的数据

## 验证步骤

1. 检查所有容器状态:
```bash
docker ps
```

2. 检查 Grafana 日志:
```bash
docker logs sgcc-grafana
```

3. 检查 PostgreSQL Sink 数据:
```bash
docker exec -it postgres-sgcc-sink-container psql -U sgcc_user -d sgcc_dw_db -c "SELECT COUNT(*) FROM ads_power_quality;"
```

4. 访问 Grafana 并检查数据源连接状态

## 注意事项

- Grafana 现在读取的是 Flink 处理后的实际数据（ADS 层）
- 不再有 PostgreSQL 服务重复的问题
- 所有服务都在同一个网络中，可以正常通信
- 如果数据为空，请检查 Flink 作业是否正常运行