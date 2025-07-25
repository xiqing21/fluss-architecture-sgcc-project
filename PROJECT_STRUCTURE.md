# 国网风控数仓项目结构说明

## 📁 项目目录结构

```
trae-fluss-base/
├── docs/                           # 📚 项目文档
│   ├── 国网风控数仓业务详细文档.md      # 详细业务文档
│   ├── 国网风控数仓技术架构图.drawio    # DrawIO架构图
│   ├── 注意事项和坑点总结.md           # 问题总结和解决方案
│   ├── README_TESTING.md             # 测试指南
│   ├── 使用说明.md                   # 使用说明
│   └── 数据观察指南.md               # 数据观察指南
├── tests/                          # 🧪 测试文件
│   ├── test_crud_operations.sql      # CRUD操作测试
│   ├── verify_data_flow.sql          # 数据流验证
│   ├── generate_test_data.py         # 大数据量测试脚本
│   └── latency_monitor.py            # 延迟监控脚本
├── scripts/                        # 📜 脚本文件
│   └── run_complete_test.sh          # 完整测试脚本
├── monitoring/                     # 📊 监控配置(预留)
├── postgres_source/                # 🗄️ PostgreSQL源端配置
│   └── init/
│       ├── 01_init_sgcc_tables.sql   # SGCC业务表初始化
│       └── 02_insert_sgcc_test_data.sql # 测试数据插入
├── postgres_sink/                  # 🗄️ PostgreSQL目标端配置
│   └── init/
│       └── 01_init_sink_tables.sql   # ADS层表初始化
├── fluss/                          # ⚡ Flink作业配置
├── jars/                           # 📦 JAR包依赖
├── .trae/                          # 🔧 Trae配置
├── .vercel/                        # 🚀 Vercel部署配置
├── docker-compose.yml              # 🐳 Docker编排文件
├── fluss_all_chain.sql             # 🔗 Flink SQL作业定义
└── PROJECT_STRUCTURE.md            # 📋 本文件
```

## 📖 目录说明

### 📚 docs/ - 项目文档
存放所有项目相关的文档，包括：
- **业务文档**：详细的业务需求和架构说明
- **技术文档**：架构图、API文档、部署指南
- **运维文档**：故障排除、性能调优、监控指南
- **用户文档**：使用说明、操作手册

### 🧪 tests/ - 测试文件
包含各种测试脚本和验证工具：
- **SQL测试**：数据库CRUD操作测试
- **数据流测试**：端到端数据流验证
- **性能测试**：大数据量和延迟测试
- **Python脚本**：自动化测试工具

### 📜 scripts/ - 脚本文件
存放各种自动化脚本：
- **部署脚本**：环境部署和配置
- **测试脚本**：自动化测试执行
- **运维脚本**：日常运维操作
- **数据脚本**：数据迁移和处理

### 📊 monitoring/ - 监控配置
预留目录，用于存放监控相关配置：
- **Prometheus配置**：指标采集配置
- **Grafana仪表板**：可视化配置
- **告警规则**：告警策略配置
- **日志配置**：日志采集和分析

### 🗄️ postgres_source/ - 源端数据库
PostgreSQL源端数据库的配置和初始化脚本：
- **表结构定义**：ODS层业务表
- **初始化数据**：测试和演示数据
- **索引和约束**：性能优化配置
- **CDC配置**：变更数据捕获设置

### 🗄️ postgres_sink/ - 目标端数据库
PostgreSQL目标端数据库的配置和初始化脚本：
- **ADS层表结构**：应用数据服务层
- **视图定义**：业务查询视图
- **索引优化**：查询性能优化
- **分区策略**：大数据量处理

### ⚡ fluss/ - Flink作业
Apache Flink流处理作业的配置和代码：
- **作业配置**：Flink作业参数
- **连接器配置**：CDC和Sink连接器
- **状态管理**：检查点和状态后端
- **资源配置**：内存和并行度设置

### 📦 jars/ - JAR包依赖
存放Flink作业所需的JAR包：
- **连接器JAR**：PostgreSQL CDC连接器
- **依赖库JAR**：第三方依赖包
- **自定义JAR**：项目特定的代码包

## 🚀 快速开始

### 1. 环境准备
```bash
# 启动Docker环境
docker-compose up -d

# 等待服务就绪
sleep 30
```

### 2. 运行测试
```bash
# 执行完整测试
./scripts/run_complete_test.sh

# 或分步执行
psql -h localhost -p 5433 -U postgres -d source_db -f tests/test_crud_operations.sql
psql -h localhost -p 5434 -U postgres -d sink_db -f tests/verify_data_flow.sql
```

### 3. 查看文档
```bash
# 查看业务文档
open docs/国网风控数仓业务详细文档.md

# 查看架构图
open docs/国网风控数仓技术架构图.drawio

# 查看测试指南
open docs/README_TESTING.md
```

## 📋 文件命名规范

### 文档文件
- 中文文档：使用中文名称，如 `国网风控数仓业务详细文档.md`
- 英文文档：使用英文名称，如 `README_TESTING.md`
- 配置文件：使用英文名称，如 `docker-compose.yml`

### 代码文件
- SQL文件：使用数字前缀排序，如 `01_init_tables.sql`
- Python文件：使用下划线分隔，如 `generate_test_data.py`
- Shell脚本：使用下划线分隔，如 `run_complete_test.sh`

### 目录命名
- 使用英文小写字母
- 多个单词用下划线分隔
- 避免使用特殊字符和空格

## 🔧 维护指南

### 添加新文档
1. 确定文档类型和目标目录
2. 使用规范的文件命名
3. 更新本文件的目录结构
4. 在相关文档中添加交叉引用

### 添加新测试
1. 在 `tests/` 目录下创建测试文件
2. 更新 `scripts/run_complete_test.sh`
3. 在 `docs/README_TESTING.md` 中添加说明
4. 确保测试可以独立运行

### 添加新脚本
1. 在 `scripts/` 目录下创建脚本文件
2. 添加执行权限：`chmod +x scripts/new_script.sh`
3. 在脚本开头添加说明注释
4. 更新相关文档

## 📞 联系方式

- **项目维护**：国网数字化团队
- **技术支持**：请查看 `docs/注意事项和坑点总结.md`
- **问题反馈**：请参考故障排除指南

---

**最后更新**：2024年12月  
**文档版本**：v1.0