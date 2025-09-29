# 📁 DataFlow 项目结构说明

## 🚀 核心生产文件

### 主要脚本
```
smart_sync_incremental_optimized.py    # 🎯 主同步脚本 (生产版本)
run_optimized_sync.sh                  # 🔧 运行脚本
```

### 配置文件
```
params-incremental-example.json        # 📋 配置模板
params.json                           # ⚙️ 实际配置文件 (用户自定义)
requirements.txt                      # 📦 Python依赖包
```

### 管理工具
```
migrate_status_files.py               # 🔄 状态文件迁移工具
test_status_manager.py                # 📊 状态管理和查看工具
```

### 文档
```
README.md                            # 📖 项目概览和快速开始
DATA_SYNC_GUIDE.md                   # 🚀 完整使用指南 (推荐阅读)
INCREMENTAL_SYNC_SQL_GUIDE.md        # 🔍 增量同步SQL详解
INCREMENTAL_SYNC_GUIDE.md            # 📋 详细技术文档
PROJECT_STRUCTURE.md                 # 📁 项目结构说明 (本文档)
```

### 运行时目录
```
sync_status/                         # 💾 状态文件存储目录
├── {database1}.json                # 数据库1的同步状态
├── {database2}.json                # 数据库2的同步状态
└── backup_single_table_files/       # 旧状态文件备份

logs/                               # 📝 日志目录 (运行时生成)
├── sync_incremental.log            # 同步日志
└── error.log                       # 错误日志

lib/                                # 📚 库文件目录 (如果需要)
```

---

## 🗄️ 备份目录结构

### backup/ - 非生产文件存储
```
backup/
├── debug_tools/                    # 🔧 调试工具
│   └── fix_bigquery_table_conflict.py
├── test_scripts/                   # 🧪 测试脚本
│   ├── test_multitenant_sync.py
│   └── verify_multitenant_fix.py
├── deprecated_scripts/             # 🗑️ 废弃脚本
├── traditional_scripts/            # 📜 传统脚本
├── old_configs/                    # ⚙️ 旧配置文件
├── old_scripts/                    # 📝 旧脚本
├── analysis_docs/                  # 📊 分析文档
├── sync_incremental.log            # 📝 历史日志文件
├── __pycache__/                    # 🗂️ Python缓存
└── README.md                       # 📖 备份目录说明
```

---

## 🎯 文件用途说明

### 🚀 生产环境必需文件
| 文件 | 用途 | 重要性 |
|------|------|--------|
| `smart_sync_incremental_optimized.py` | 主同步脚本 | ⭐⭐⭐⭐⭐ |
| `params.json` | 配置文件 | ⭐⭐⭐⭐⭐ |
| `requirements.txt` | 依赖包 | ⭐⭐⭐⭐⭐ |
| `run_optimized_sync.sh` | 运行脚本 | ⭐⭐⭐⭐ |

### 📖 文档文件
| 文件 | 用途 | 推荐阅读顺序 |
|------|------|-------------|
| `README.md` | 项目概览 | 1️⃣ 首先阅读 |
| `DATA_SYNC_GUIDE.md` | 完整指南 | 2️⃣ 重点阅读 |
| `INCREMENTAL_SYNC_SQL_GUIDE.md` | SQL详解 | 3️⃣ 深入理解 |
| `INCREMENTAL_SYNC_GUIDE.md` | 技术文档 | 4️⃣ 参考资料 |

### 🔧 管理工具
| 文件 | 用途 | 使用场景 |
|------|------|----------|
| `migrate_status_files.py` | 状态迁移 | 版本升级时 |
| `test_status_manager.py` | 状态查看 | 日常监控 |

### 🗄️ 备份文件
| 目录/文件 | 用途 | 说明 |
|-----------|------|------|
| `backup/debug_tools/` | 调试工具 | 问题排查时使用 |
| `backup/test_scripts/` | 测试脚本 | 功能验证时使用 |
| `backup/deprecated_scripts/` | 废弃脚本 | 历史版本保留 |

---

## 🚀 快速开始流程

### 1. 首次部署
```bash
# 1. 复制配置文件
cp params-incremental-example.json params.json

# 2. 编辑配置
vim params.json

# 3. 安装依赖
pip install -r requirements.txt

# 4. 运行同步
python3 smart_sync_incremental_optimized.py
```

### 2. 日常使用
```bash
# 增量同步 (推荐)
./run_optimized_sync.sh

# 全量同步
./run_optimized_sync.sh --full

# 查看状态
python3 test_status_manager.py --overview
```

### 3. 问题排查
```bash
# 查看日志
tail -f logs/sync_incremental.log

# 使用调试工具
python3 backup/debug_tools/fix_bigquery_table_conflict.py

# 运行测试脚本
python3 backup/test_scripts/test_multitenant_sync.py
```

---

## 📋 维护建议

### 🔄 定期维护
- **每周**: 检查同步状态和日志
- **每月**: 清理旧日志文件
- **每季度**: 检查备份文件，清理不需要的文件

### 🗂️ 文件管理
- **保留**: 所有生产文件和文档
- **定期清理**: `logs/` 目录中的旧日志
- **谨慎删除**: `backup/` 目录中的文件

### 📊 监控要点
- 同步成功率
- 数据量变化
- 错误日志
- 磁盘空间使用

---

## 🎯 版本管理

### 当前版本
- **版本**: v4.1 (状态管理优化版)
- **更新时间**: 2025-09-29
- **状态**: 生产就绪 ✅

### 版本历史
- **v4.1**: 本地状态存储，按数据库分组
- **v4.0**: 性能优化版，缓存和并行处理
- **v3.x**: 传统版本 (已迁移到 backup/)

---

## 📞 获取帮助

### 📖 文档优先级
1. **README.md** - 快速了解项目
2. **DATA_SYNC_GUIDE.md** - 完整使用指南
3. **INCREMENTAL_SYNC_SQL_GUIDE.md** - SQL查询详解
4. **INCREMENTAL_SYNC_GUIDE.md** - 详细技术文档

### 🔧 工具使用
- **状态查看**: `python3 test_status_manager.py --overview`
- **状态迁移**: `python3 migrate_status_files.py`
- **问题调试**: 查看 `backup/debug_tools/` 中的工具

### 🚨 紧急情况
- **同步失败**: 查看日志，使用调试工具
- **数据异常**: 运行测试脚本验证
- **配置问题**: 参考配置模板重新设置

---

**项目整理完成时间**: 2025-09-29  
**整理版本**: v4.1-clean  
**维护状态**: 生产就绪 ✅