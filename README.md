# 🚀 智能数据同步工具

高性能的 MySQL 到 BigQuery 数据同步解决方案，支持全量和增量同步模式。

## ✨ 核心特性

- **🧠 智能同步**: 自动检测表结构，选择最佳同步策略
- **⚡ 高性能**: 表结构缓存、连接池复用、批量处理、并行同步
- **🔄 双模式**: 支持全量同步和增量同步
- **🛡️ 数据一致性**: 安全时间窗口、MERGE操作、哈希去重
- **📊 状态管理**: 本地JSON文件存储，按数据库分组
- **🔍 完整监控**: 详细日志、统计报告、状态追踪

## 🚀 快速开始

### 1. 环境准备
```bash
# 安装依赖
pip install -r requirements.txt

# 配置 Google Cloud 认证
gcloud auth application-default login

# 复制配置文件
cp params-incremental-example.json params.json
```

### 2. 配置参数
编辑 `params.json`:
```json
{
  "db_host": "your-mysql-host",
  "db_port": "3306",
  "db_user": "readonly_user", 
  "db_pass": "your_password",
  "db_list": "shop1,shop2,shop3",
  "table_list": "users,orders,products",
  "bq_project": "your-gcp-project",
  "bq_dataset": "your_dataset"
}
```

### 3. 运行同步
```bash
# 智能增量同步 (推荐)
python3 smart_sync_incremental_optimized.py

# 强制全量同步
python3 smart_sync_incremental_optimized.py --full

# 使用脚本运行
./run_optimized_sync.sh
```

### 4. 状态管理
```bash
# 查看状态概览
python3 test_status_manager.py --overview

# 迁移旧状态文件
python3 migrate_status_files.py
```

## 🔄 同步模式

### 增量同步 (默认)
- **触发条件**: 存在时间戳字段 + 有同步历史
- **同步逻辑**: 基于时间戳字段，只同步变更数据
- **写入策略**: MERGE (有主键) / APPEND (无主键)
- **适用场景**: 日常数据同步，高效率

### 全量同步
- **触发条件**: 首次同步 / 无时间戳字段 / 手动强制
- **同步逻辑**: 完整同步所有数据
- **写入策略**: TRUNCATE 模式，完全替换
- **适用场景**: 数据初始化，一致性校验

## ⚡ 性能优化

| 优化项 | 性能提升 | 说明 |
|--------|----------|------|
| 表结构缓存 | 50-70% | 减少重复查询 |
| 连接池复用 | 20-30% | 减少连接开销 |
| 批量处理 | 30-40% | 提升处理效率 |
| 并行同步 | 50-65% | 表级并行处理 |
| 状态优化 | 67% | 减少文件数量 |

## 📁 项目结构

```
dataflow/
├── 🎯 核心生产文件
│   ├── smart_sync_incremental_optimized.py  # 主同步脚本
│   ├── run_optimized_sync.sh               # 运行脚本
│   ├── params-incremental-example.json      # 配置模板
│   ├── params.json                         # 实际配置文件
│   └── requirements.txt                    # 依赖包
├── 🔧 管理工具
│   ├── migrate_status_files.py             # 状态迁移工具
│   └── test_status_manager.py              # 状态管理工具
├── 📖 文档
│   ├── README.md                          # 项目概览 (本文档)
│   ├── DATA_SYNC_GUIDE.md                 # 完整使用指南
│   ├── INCREMENTAL_SYNC_SQL_GUIDE.md      # SQL查询详解
│   ├── INCREMENTAL_SYNC_GUIDE.md          # 详细技术文档
│   └── PROJECT_STRUCTURE.md               # 项目结构说明
├── 💾 运行时目录
│   ├── sync_status/                       # 状态文件目录
│   │   ├── {database}.json               # 数据库状态文件
│   │   └── backup_single_table_files/    # 旧状态文件备份
│   ├── logs/                              # 日志目录
│   └── lib/                               # 库文件目录
└── 🗄️ 备份目录
    └── backup/                            # 非生产文件存储
        ├── debug_tools/                   # 调试工具
        ├── test_scripts/                  # 测试脚本
        ├── deprecated_scripts/            # 废弃脚本
        └── ...                           # 其他备份文件
```

> 📋 详细的项目结构说明请查看 [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

## 📖 文档指南

- **🚀 [DATA_SYNC_GUIDE.md](DATA_SYNC_GUIDE.md)** - **完整使用指南** (推荐阅读)
  - 全量和增量同步逻辑详解
  - 系统架构和数据流程
  - 配置说明和最佳实践
  - 性能优化和故障排除

- **🔍 [INCREMENTAL_SYNC_SQL_GUIDE.md](INCREMENTAL_SYNC_SQL_GUIDE.md)** - **增量同步SQL详解** (重要)
  - 时间戳字段检测逻辑
  - 增量查询SQL示例
  - 安全时间窗口计算
  - 性能优化和调试

- **📋 [INCREMENTAL_SYNC_GUIDE.md](INCREMENTAL_SYNC_GUIDE.md)** - 详细技术文档
  - 具体操作步骤
  - 高级配置选项
  - 监控和维护指南

- **📁 [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** - 项目结构说明
  - 文件用途和重要性
  - 维护建议和最佳实践
  - 备份文件管理

## 🔍 状态管理

### 状态文件结构
```json
{
  "database_info": {
    "tenant_id": "shop1",
    "last_updated": "2025-09-29T11:00:00",
    "total_tables": 3
  },
  "tables": {
    "users": {
      "last_sync_time": "2025-09-29T10:30:00",
      "sync_status": "SUCCESS",
      "sync_mode": "INCREMENTAL",
      "records_synced": 150
    }
  }
}
```

### 管理工具
```bash
# 查看状态概览
python3 test_status_manager.py --overview

# 迁移旧状态文件
python3 migrate_status_files.py --preview
python3 migrate_status_files.py
```

## 📊 性能基准

| 表大小 | 全量同步 | 增量同步 | 内存使用 |
|--------|----------|----------|----------|
| < 1万行 | < 30秒 | < 10秒 | ~50MB |
| 1-10万行 | < 5分钟 | < 1分钟 | ~100MB |
| > 10万行 | < 30分钟 | < 5分钟 | ~200MB |

## 🛡️ 系统要求

- **Python**: 3.7+
- **权限**: MySQL SELECT 权限 + BigQuery 读写权限
- **网络**: 访问 MySQL 数据库和 BigQuery API
- **存储**: 读写 `sync_status/` 目录权限

## 🆘 快速故障排除

```bash
# 检查连接
mysql -h your-host -u user -p
gcloud auth application-default login

# 查看日志
tail -f sync_incremental.log
grep -i error sync_incremental.log

# 重置状态 (将执行全量同步)
rm sync_status/problematic_db.json
```

---

## 📞 获取帮助

### 📖 文档优先级
1. **[README.md](README.md)** - 项目概览 (本文档)
2. **[DATA_SYNC_GUIDE.md](DATA_SYNC_GUIDE.md)** - 完整使用指南
3. **[INCREMENTAL_SYNC_SQL_GUIDE.md](INCREMENTAL_SYNC_SQL_GUIDE.md)** - SQL查询详解
4. **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** - 项目结构说明

### 🔧 工具和调试
- **状态管理**: `python3 test_status_manager.py --overview`
- **调试工具**: 查看 `backup/debug_tools/` 目录
- **测试脚本**: 查看 `backup/test_scripts/` 目录

### 🚨 问题反馈
提供以下信息以便快速解决问题：
- 错误日志 (`logs/sync_incremental.log`)
- 配置文件 (`params.json`)
- 状态文件 (`sync_status/*.json`)

---

**版本**: v4.1-clean (项目整理版)  
**更新时间**: 2025-09-29  
**状态**: 生产就绪 ✅

> 💡 **推荐**: 首次使用请按顺序阅读文档，从 README.md 开始！