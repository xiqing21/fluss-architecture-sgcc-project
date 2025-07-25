#!/bin/bash

# 国网风控数仓 - 完整CRUD测试执行脚本
# 从source端到sink端的完整数据流测试

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖项..."
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装或不在PATH中"
        exit 1
    fi
    
    # 检查Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python3未安装或不在PATH中"
        exit 1
    fi
    
    # 检查psycopg2
    if ! python3 -c "import psycopg2" 2>/dev/null; then
        log_warning "psycopg2未安装，正在安装..."
        pip3 install psycopg2-binary
    fi
    
    log_success "依赖检查完成"
}

# 检查Docker容器状态
check_containers() {
    log_info "检查Docker容器状态..."
    
    # 检查必要的容器
    containers=("postgres-sgcc-source" "postgres-sgcc-sink" "sql-client-sgcc")
    
    for container in "${containers[@]}"; do
        if ! docker ps --format "table {{.Names}}" | grep -q "$container"; then
            log_error "容器 $container 未运行"
            log_info "请先启动Docker Compose: docker-compose up -d"
            exit 1
        fi
    done
    
    log_success "所有必要容器正在运行"
}

# 等待数据库就绪
wait_for_database() {
    local host=$1
    local port=$2
    local database=$3
    local user=$4
    local max_attempts=30
    local attempt=1
    
    log_info "等待数据库 $host:$port/$database 就绪..."
    
    while [ $attempt -le $max_attempts ]; do
        if [ "$port" = "5442" ]; then
            # Source数据库通过容器连接
            if docker exec -i postgres-sgcc-source psql -U $user -d $database -c "SELECT 1" &>/dev/null; then
                log_success "数据库 $host:$port/$database 已就绪"
                return 0
            fi
        elif [ "$port" = "5443" ]; then
            # Sink数据库通过容器连接
            if docker exec -i postgres-sgcc-sink psql -U $user -d $database -c "SELECT 1" &>/dev/null; then
                log_success "数据库 $host:$port/$database 已就绪"
                return 0
            fi
        fi
        
        log_info "等待数据库就绪... ($attempt/$max_attempts)"
        sleep 2
        ((attempt++))
    done
    
    log_error "数据库 $host:$port/$database 连接超时"
    return 1
}

# 执行SQL文件
execute_sql_file() {
    local host=$1
    local port=$2
    local database=$3
    local user=$4
    local sql_file=$5
    local description=$6
    
    log_info "$description"
    
    if [ ! -f "$sql_file" ]; then
        log_error "SQL文件不存在: $sql_file"
        return 1
    fi
    
    if [ "$port" = "5442" ]; then
        # Source数据库通过容器连接
        if docker exec -i postgres-sgcc-source psql -U $user -d $database < "$sql_file" &>/dev/null; then
            log_success "$description 完成"
            return 0
        else
            log_error "$description 失败"
            return 1
        fi
    elif [ "$port" = "5443" ]; then
        # Sink数据库通过容器连接
        if docker exec -i postgres-sgcc-sink psql -U $user -d $database < "$sql_file" &>/dev/null; then
            log_success "$description 完成"
            return 0
        else
            log_error "$description 失败"
            return 1
        fi
    fi
}

# 检查表数据
check_table_data() {
    local host=$1
    local port=$2
    local database=$3
    local user=$4
    local table=$5
    local description=$6
    
    log_info "检查 $description"
    
    local count
    if [ "$port" = "5442" ]; then
        # Source数据库通过容器连接
        count=$(docker exec -i postgres-sgcc-source psql -U $user -d $database -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ')
    elif [ "$port" = "5443" ]; then
        # Sink数据库通过容器连接
        count=$(docker exec -i postgres-sgcc-sink psql -U $user -d $database -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ')
    fi
    
    if [ -n "$count" ] && [ "$count" -gt 0 ]; then
        log_success "$description: $count 条记录"
        return 0
    else
        log_warning "$description: 无数据或查询失败"
        return 1
    fi
}

# 执行基础CRUD测试
run_basic_crud_test() {
    log_info "=== 执行基础CRUD测试 ==="
    
    # 等待数据库就绪
    wait_for_database "localhost" "5442" "sgcc_source_db" "sgcc_user"
    wait_for_database "localhost" "5443" "sgcc_dw_db" "sgcc_user"
    
    # 执行基础CRUD操作
    if [ -f "tests/test_crud_operations.sql" ]; then
        execute_sql_file "localhost" "5442" "sgcc_source_db" "sgcc_user" "tests/test_crud_operations.sql" "执行基础CRUD操作"
    else
        log_warning "基础CRUD测试文件不存在，跳过"
    fi
    
    # 等待数据处理
    log_info "等待数据处理 (30秒)..."
    sleep 30
    
    # 检查source端数据
    log_info "检查Source端数据..."
    check_table_data "localhost" "5442" "sgcc_source_db" "sgcc_user" "equipment_info" "设备信息表"
    check_table_data "localhost" "5442" "sgcc_source_db" "sgcc_user" "customer_info" "客户信息表"
    check_table_data "localhost" "5442" "sgcc_source_db" "sgcc_user" "power_consumption" "电力消耗表"
    check_table_data "localhost" "5442" "sgcc_source_db" "sgcc_user" "equipment_status" "设备状态表"
    check_table_data "localhost" "5442" "sgcc_source_db" "sgcc_user" "alert_records" "告警记录表"
    
    # 检查sink端数据
    log_info "检查Sink端数据..."
    check_table_data "localhost" "5443" "sgcc_dw_db" "sgcc_user" "ads_realtime_dashboard" "实时仪表板"
    check_table_data "localhost" "5443" "sgcc_dw_db" "sgcc_user" "ads_equipment_health" "设备健康分析"
    check_table_data "localhost" "5443" "sgcc_dw_db" "sgcc_user" "ads_customer_behavior" "客户行为分析"
    check_table_data "localhost" "5443" "sgcc_dw_db" "sgcc_user" "ads_alert_statistics" "告警统计"
    check_table_data "localhost" "5443" "sgcc_dw_db" "sgcc_user" "ads_power_quality" "电能质量分析"
    
    log_success "基础CRUD测试完成"
}

# 执行数据验证
run_data_verification() {
    log_info "=== 执行数据验证 ==="
    
    if [ -f "tests/verify_data_flow.sql" ]; then
        log_info "执行Source端数据验证..."
        docker exec -i postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db < tests/verify_data_flow.sql > source_verification.log 2>&1
        
        log_info "执行Sink端数据验证..."
        docker exec -i postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db < tests/verify_data_flow.sql > sink_verification.log 2>&1
        
        log_success "数据验证完成，结果保存到 source_verification.log 和 sink_verification.log"
    else
        log_warning "数据验证脚本不存在，跳过"
    fi
}

# 执行大数据量测试
run_load_test() {
    log_info "=== 执行大数据量测试 ==="
    
    if [ -f "tests/generate_test_data.py" ]; then
        log_info "开始生成大数据量测试数据..."
        
        # 修改Python脚本中的数据库配置（如果需要）
        python3 tests/generate_test_data.py > load_test.log 2>&1 &
        local load_test_pid=$!
        
        log_info "大数据量测试正在后台运行 (PID: $load_test_pid)"
        log_info "可以查看 load_test.log 了解进度"
        
        # 等待一段时间让测试开始
        sleep 10
        
        return $load_test_pid
    else
        log_warning "大数据量测试脚本不存在，跳过"
        return 0
    fi
}

# 启动延迟监控
start_latency_monitoring() {
    log_info "=== 启动延迟监控 ==="
    
    if [ -f "tests/latency_monitor.py" ]; then
        log_info "启动实时延迟监控..."
        
        python3 tests/latency_monitor.py > latency_monitor.log 2>&1 &
        local monitor_pid=$!
        
        log_info "延迟监控已启动 (PID: $monitor_pid)"
        log_info "按 Ctrl+C 停止监控"
        
        return $monitor_pid
    else
        log_warning "延迟监控脚本不存在，跳过"
        return 0
    fi
}

# 生成测试报告
generate_test_report() {
    log_info "=== 生成测试报告 ==="
    
    local report_file="test_report_$(date +%Y%m%d_%H%M%S).md"
    
    cat > "$report_file" << EOF
# 国网风控数仓 - CRUD测试报告

## 测试概述
- 测试时间: $(date '+%Y-%m-%d %H:%M:%S')
- 测试类型: 完整CRUD测试 (Source -> Fluss -> Sink)
- 测试环境: Docker Compose

## 测试结果

### 1. 基础CRUD测试
$(if [ -f "source_verification.log" ]; then echo "✅ Source端验证完成"; else echo "❌ Source端验证失败"; fi)
$(if [ -f "sink_verification.log" ]; then echo "✅ Sink端验证完成"; else echo "❌ Sink端验证失败"; fi)

### 2. 数据验证
$(if [ -f "source_verification.log" ]; then echo "详见 source_verification.log"; fi)
$(if [ -f "sink_verification.log" ]; then echo "详见 sink_verification.log"; fi)

### 3. 大数据量测试
$(if [ -f "load_test.log" ]; then echo "详见 load_test.log"; fi)

### 4. 延迟监控
$(if [ -f "latency_monitor.log" ]; then echo "详见 latency_monitor.log"; fi)
$(if ls latency_report_*.json 1> /dev/null 2>&1; then echo "延迟报告: $(ls -t latency_report_*.json | head -1)"; fi)

## 文件说明
- source_verification.log: Source数据库验证结果
- sink_verification.log: Sink数据库验证结果
- load_test.log: 大数据量测试日志
- latency_monitor.log: 延迟监控日志
- latency_report_*.json: 详细延迟报告

## 建议
1. 检查各个日志文件了解详细结果
2. 如果发现延迟过高，检查Flink任务状态
3. 如果数据不一致，检查CDC配置和网络连接

EOF

    log_success "测试报告已生成: $report_file"
}

# 清理函数
cleanup() {
    log_info "清理测试环境..."
    
    # 停止后台进程
    if [ -n "$LOAD_TEST_PID" ] && kill -0 $LOAD_TEST_PID 2>/dev/null; then
        log_info "停止大数据量测试..."
        kill $LOAD_TEST_PID
    fi
    
    if [ -n "$MONITOR_PID" ] && kill -0 $MONITOR_PID 2>/dev/null; then
        log_info "停止延迟监控..."
        kill $MONITOR_PID
    fi
    
    log_success "清理完成"
}

# 信号处理
trap cleanup EXIT INT TERM

# 主函数
main() {
    echo "==========================================="
    echo "     国网风控数仓 - 完整CRUD测试"
    echo "==========================================="
    echo
    
    # 检查依赖和环境
    check_dependencies
    check_containers
    
    # 执行测试步骤
    run_basic_crud_test
    run_data_verification
    
    # 启动大数据量测试（后台）
    run_load_test
    LOAD_TEST_PID=$?
    
    # 启动延迟监控（后台）
    start_latency_monitoring
    MONITOR_PID=$?
    
    # 等待用户输入或测试完成
    log_info "测试正在进行中..."
    log_info "按 Enter 键生成报告并退出，或等待测试自动完成"
    
    # 等待用户输入或一定时间后自动结束
    read -t 300 -p "" || true  # 等待5分钟或用户输入
    
    # 生成报告
    generate_test_report
    
    log_success "测试完成！"
    echo
    echo "查看测试结果:"
    echo "- 基础测试: source_verification.log, sink_verification.log"
    echo "- 负载测试: load_test.log"
    echo "- 延迟监控: latency_monitor.log"
    echo "- 详细报告: test_report_*.md"
    echo
}

# 执行主函数
main "$@"