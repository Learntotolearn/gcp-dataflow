#!/bin/bash

# 智能增量同步脚本运行器 - 性能优化版
# 使用方法：
#   ./run_optimized_sync.sh          # 增量同步
#   ./run_optimized_sync.sh --full   # 强制全量同步

echo "🚀 启动智能增量同步工具 - 性能优化版"
echo "=================================="

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 未安装"
    exit 1
fi

# 检查配置文件
if [ ! -f "params.json" ]; then
    echo "❌ 配置文件 params.json 不存在"
    echo "💡 请复制 params-incremental-example.json 并修改配置"
    exit 1
fi

# 检查依赖
echo "🔍 检查依赖..."
python3 -c "import mysql.connector, mysql.connector.pooling, google.cloud.bigquery" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ 依赖缺失，正在安装..."
    pip3 install mysql-connector-python google-cloud-bigquery
fi

# 设置日志目录
mkdir -p logs
LOG_FILE="logs/sync_optimized_$(date +%Y%m%d_%H%M%S).log"

# 显示性能优化信息
echo "⚡ 性能优化特性："
echo "  💾 表结构缓存 - 减少重复查询"
echo "  🔗 连接池复用 - 减少连接开销"
echo "  📦 批量数据处理 - 提升处理效率"
echo "  🎯 智能写入策略 - 根据数据量选择最优方式"

# 执行同步
echo ""
echo "⚡ 开始同步..."
if [ "$1" = "--full" ]; then
    echo "🔄 强制全量同步模式"
    python3 smart_sync_incremental_optimized.py --full 2>&1 | tee "$LOG_FILE"
    SYNC_RESULT=${PIPESTATUS[0]}
else
    echo "⚡ 智能增量同步模式"
    python3 smart_sync_incremental_optimized.py 2>&1 | tee "$LOG_FILE"
    SYNC_RESULT=${PIPESTATUS[0]}
fi

# 显示结果
echo ""
echo "=================================="
if [ $SYNC_RESULT -eq 0 ]; then
    echo "✅ 同步成功完成！"
    echo "📋 日志文件: $LOG_FILE"
    echo ""
    echo "📊 性能统计："
    grep -E "(处理速度|总耗时|表结构缓存)" "$LOG_FILE" | tail -5
else
    echo "❌ 同步过程中出现错误 (退出码: $SYNC_RESULT)"
    echo "📋 错误日志: $LOG_FILE"
    echo "💡 请检查日志文件获取详细错误信息"
    echo ""
    echo "🔍 最后几行错误信息："
    tail -10 "$LOG_FILE" | grep -E "(ERROR|Exception|Traceback|Error)" || tail -5 "$LOG_FILE"
fi

echo ""
echo "🔍 查看同步状态："
echo "   mysql> SELECT * FROM sync_status ORDER BY updated_at DESC LIMIT 10;"

exit $SYNC_RESULT