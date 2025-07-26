# Docker配置目录

本目录包含Docker相关的配置文件：

## 文件说明

- `docker-compose.yml` - 主要的Docker Compose配置文件
  - PostgreSQL Source数据库 (端口: 5442)
  - PostgreSQL Sink数据库 (端口: 5443)
  - Grafana服务 (端口: 3000)
  - 网络配置

## 使用方法

```bash
# 启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 停止所有服务
docker-compose down

# 查看日志
docker-compose logs -f
```

## 服务访问

- Grafana: http://localhost:3000 (admin/admin123)
- PostgreSQL Source: localhost:5442
- PostgreSQL Sink: localhost:5443