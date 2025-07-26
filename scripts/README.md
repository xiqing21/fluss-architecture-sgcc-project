# 脚本文件目录

本目录包含项目的各种脚本文件：

## 主要脚本

- `interactive_data_manager.py` - 交互式数据管理脚本
  - Source端CRUD操作
  - 自动生成测试数据
  - Sink端数据监控
  - 数据流延迟分析

- `test_data_manager.py` - 数据管理脚本自动化测试
  - 数据库连接测试
  - 表结构验证
  - 功能完整性检查

- `test_grafana_dashboard.py` - Grafana仪表板测试脚本
  - 数据源连接测试
  - 面板查询验证
  - 数据显示检查

- `generate_rich_test_data.py` - 测试数据生成脚本
  - 为ADS表生成丰富的测试数据
  - 支持时间范围配置
  - 数据质量保证

## 使用方法

```bash
# 运行交互式数据管理
python scripts/interactive_data_manager.py

# 测试数据管理功能
python scripts/test_data_manager.py

# 测试Grafana仪表板
python scripts/test_grafana_dashboard.py

# 生成测试数据
python scripts/generate_rich_test_data.py
```