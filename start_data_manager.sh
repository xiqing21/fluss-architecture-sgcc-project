#!/bin/bash

# 实时数据流管理工具启动脚本

echo "🚀 启动实时数据流管理工具..."
echo "================================================"

# 检查虚拟环境是否存在
if [ ! -d "venv" ]; then
    echo "📦 创建Python虚拟环境..."
    python3 -m venv venv
    
    echo "📥 安装依赖包..."
    source venv/bin/activate
    pip install psycopg2-binary
else
    echo "✅ 虚拟环境已存在"
fi

# 检查脚本是否存在
if [ ! -f "interactive_data_manager.py" ]; then
    echo "❌ 找不到 interactive_data_manager.py 文件"
    exit 1
fi

echo "🔧 启动数据管理工具..."
echo "================================================"
echo ""
echo "💡 使用提示："
echo "   - 选择选项 2 生成测试数据"
echo "   - 选择选项 3 监控sink端数据"
echo "   - 选择选项 4 分析数据流延迟"
echo "   - 按 Ctrl+C 可以随时退出"
echo ""
echo "📖 详细说明请查看: README_data_manager.md"
echo "================================================"
echo ""

# 激活虚拟环境并运行脚本
source venv/bin/activate
python3 interactive_data_manager.py