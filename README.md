# 国网电力监控系统 - Fluss实时数据流架构

> 🚀 **致敬开源精神** - 这是一个极其复杂的实时数据流项目，历经无数个不眠之夜，踩过无数个坑，从Flink到Fluss，从CDC到数据湖，每一行代码都凝聚着对技术的执着追求。如果这个项目对您有帮助，请在GitHub上给我们一个⭐，您的支持是我们继续前行的动力！

基于Apache Fluss构建的电力监控实时数据处理系统，实现从PostgreSQL源数据库到数据仓库的完整ETL流程。

## 🔧 核心文件说明

### 环境配置文件 - docker-compose.yml

项目的核心环境配置文件位于 `docker-compose.yml`，定义了完整的微服务架构：

- **PostgreSQL Source** (端口5442): 源数据库，存储原始业务数据
- **PostgreSQL Sink** (端口5443): 目标数据仓库，存储分析结果
- **Fluss集群**: Coordinator Server + Tablet Server，提供流表存储
- **Flink集群**: JobManager + TaskManager，负责流处理计算
- **Grafana**: 数据可视化大屏 (端口3000)
- **ZooKeeper**: 分布式协调服务

**重要**: 该配置文件自动将 `fluss_all_chain.sql` 挂载到Flink SQL Client容器的 `/opt/sql/` 目录下。

### 核心SQL脚本 - fluss_all_chain.sql

这是项目的**灵魂文件**，包含完整的四层数仓架构SQL定义：

1. **ODS层**: PostgreSQL CDC源表定义 (在default catalog)
2. **DWD层**: 数据清洗和标准化表 (在fluss_catalog)
3. **DWS层**: 数据汇总和聚合表 (在fluss_catalog)
4. **ADS层**: 应用数据服务表，JDBC sink到PostgreSQL (在default catalog)

**执行方式**:
```bash
# 一次性执行完整数据流链路
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh -f /opt/sql/fluss_all_chain.sql
```

执行后可在Flink Web UI (http://localhost:8091) 查看运行的流处理任务。

**⚠️ 重要提醒 - Catalog持久化差异**:
- **Fluss Catalog**: 在Fluss Catalog中创建的库和表具有**持久化特性**，重启flinksqlcli客户端后只需重新声明Catalog即可恢复访问
- **Default Catalog**: Flink默认Catalog中创建的任何对象都会随着flinksqlcli客户端会话关闭而**消失**
- **实践建议**: 新开CLI时如遇到default catalog下DDL缺失问题，请先创建fluss_catalog并切换使用

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
直接在根目录执行

docker-compose up -d
```

### 2. 执行 sql 脚本

```bash
# 同样在根目录执行完整数据流链路
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh -f /opt/sql/fluss_all_chain.sql
```

### 3. 运行生成测试数据脚本来验证 crud 和数据生成

```bash
# 运行数据管理脚本
python scripts/interactive_data_manager.py



### 4. 访问Grafana

- URL: http://localhost:3000
- 用户名: admin
- 密码: admin123


### 数据库配置信息

#### PostgreSQL Source Database (源数据库)
- **连接地址**: localhost:5442
- **数据库名**: sgcc_source_db
- **用户名**: sgcc_user
- **密码**: sgcc_pass_2024
- **用途**: 存储客户信息、设备信息、电力消耗、设备状态、告警记录等业务数据

#### PostgreSQL Sink Database (目标数据仓库)
- **连接地址**: localhost:5443
- **数据库名**: sgcc_dw_db
- **用户名**: sgcc_user
- **密码**: sgcc_pass_2024
- **用途**: 存储经过ETL处理的分析结果，供Grafana大屏展示

### 测试脚本 - interactive_data_manager.py

位于 `scripts/interactive_data_manager.py` 的交互式数据管理脚本，提供完整的CRUD测试功能：

**主要功能**:
- **Source端数据操作**: 增删改查客户、设备、电力数据、告警记录
- **自动数据生成**: 批量生成测试数据，支持历史数据和实时数据模拟
- **Sink端监控**: 监控数据流转情况和延迟分析
- **大屏数据生成**: 专门为Grafana大屏生成测试数据
- **数据流诊断**: 端到端数据流验证

**使用方式**:
```bash
cd scripts
python3 interactive_data_manager.py
```

该脚本是验证从Source到Sink完整数据流的重要工具。

## ⚠️ 已知问题与后续改进

- **Default Catalog持久化问题**: Flink默认Catalog的DDL对象无法持久化，需要每次重新创建
- **Grafana大屏数据显示**: 部分趋势图可能存在数据展示问题，需要进一步调优
- **性能优化**: 大数据量场景下的性能调优有待完善

这些问题我们会在后续版本中逐步解决，也欢迎社区贡献力量！

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