# Fluss 实时数据处理项目

基于Apache Fluss的实时数据处理和监控系统，集成PostgreSQL和Grafana。

## 项目结构

```
.
├── docker/                 # Docker配置文件
│   └── docker-compose.yml  # 主要服务配置
├── scripts/                # 脚本文件
│   ├── interactive_data_manager.py    # 交互式数据管理
│   ├── test_data_manager.py          # 数据管理测试
│   ├── test_grafana_dashboard.py     # Grafana测试
│   └── generate_rich_test_data.py    # 测试数据生成
├── grafana/                # Grafana配置
│   ├── dashboards/         # 仪表板配置
│   └── provisioning/       # 数据源配置
├── fluss/                  # Fluss相关文件
│   ├── conf/              # 配置文件
│   └── lib/               # JAR包
├── postgres_sink/          # PostgreSQL Sink配置
├── tests/                  # 测试文件
└── docs/                   # 文档文件
```

## 快速开始

### 1. 启动服务

```bash
# 启动所有Docker服务
cd docker
docker-compose up -d
```

### 2. 生成测试数据

```bash
# 运行数据管理脚本
python scripts/interactive_data_manager.py

# 或者直接生成测试数据
python scripts/generate_rich_test_data.py
```

### 3. 访问Grafana

- URL: http://localhost:3000
- 用户名: admin
- 密码: admin123

### 4. 运行测试

```bash
# 测试数据管理功能
python scripts/test_data_manager.py

# 测试Grafana仪表板
python scripts/test_grafana_dashboard.py
```

## 数据库连接信息

### PostgreSQL Source (端口: 5442)
- 数据库: sgcc_source_db
- 用户名: sgcc_user
- 密码: sgcc_pass_2024

### PostgreSQL Sink (端口: 5443)
- 数据库: sgcc_dw_db
- 用户名: sgcc_user
- 密码: sgcc_pass_2024

## 主要功能

1. **实时数据处理**: 基于Fluss的流式数据处理
2. **数据监控**: Grafana实时监控大屏
3. **数据管理**: 交互式数据CRUD操作
4. **自动化测试**: 完整的测试套件
5. **数据生成**: 自动化测试数据生成

## 技术栈

- **流处理**: Apache Fluss
- **数据库**: PostgreSQL
- **监控**: Grafana
- **容器化**: Docker & Docker Compose
- **编程语言**: Python

## 贡献指南

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request

## 许可证

MIT License