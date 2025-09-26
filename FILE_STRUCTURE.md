# 📁 项目文件结构详解

## 🏗️ 完整目录结构

```
dataflow/
├── 📚 文档系统 (Documentation)
│   ├── README.md                    # 主文档 - 完整使用指南
│   ├── PROJECT_OVERVIEW.md          # 项目总览 - 架构和特性介绍
│   ├── QUICK_START.md               # 快速开始 - 5分钟上手指南
│   ├── DEDUP_GUIDE.md               # 去重指南 - 解决重复数据问题
│   ├── WRITE_MODES.md               # 写入模式 - TRUNCATE/APPEND/EMPTY详解
│   ├── TROUBLESHOOTING.md           # 故障排除 - 常见问题解决方案
│   └── FILE_STRUCTURE.md            # 文件结构 - 本文档
│
├── 🎯 核心同步工具 (Core Sync Tools)
│   ├── simple_sync_fixed_multitenant.py    # ⭐⭐⭐⭐⭐ 多租户修复版本
│   ├── simple_sync_append_safe.py          # ⭐⭐⭐⭐⭐ 安全追加版本
│   ├── simple_sync_incremental.py          # ⭐⭐⭐⭐ 增量同步版本
│   ├── simple_sync_dedup.py                # ⭐⭐⭐⭐ MERGE去重版本
│   ├── simple_sync_fixed.py                # ⭐⭐⭐ 单租户标准版本
│   ├── simple_sync_incremental_compatible.py # 兼容版增量同步
│   └── simple_sync.py                      # 原始版本（已废弃）
│
├── ⚙️ 配置文件 (Configuration Files)
│   ├── params.json                         # 主配置文件
│   ├── params-example.json                 # 基础配置示例
│   ├── params-dedup-example.json           # 去重配置示例
│   └── params copy.json                    # 配置备份
│
├── 🔧 工具脚本 (Utility Scripts)
│   ├── test_connection.py                  # 连接测试工具
│   ├── install_deps.sh                     # 依赖安装脚本
│   ├── run_dataflow_fixed.sh               # 云端Dataflow运行脚本
│   └── run_local.py                        # 本地Apache Beam运行器
│
├── 📦 依赖管理 (Dependencies)
│   └── requirements.txt                    # Python依赖列表
│
├── 🗂️ 项目管理 (Project Management)
│   ├── .gitignore                          # Git忽略文件
│   ├── .git/                               # Git版本控制
│   └── __pycache__/                        # Python缓存目录
│
├── 📊 资源文件 (Resources)
│   ├── image.png                           # 项目相关图片
│   └── lib/                                # 库文件目录
│
└── 🧪 测试和开发 (Testing & Development)
    └── (测试文件将在此目录)
```

---

## 📚 文档系统详解

### 🎯 文档层次结构
```
文档导航路径：
PROJECT_OVERVIEW.md (项目总览)
    ↓
QUICK_START.md (快速开始)
    ↓
README.md (详细文档)
    ↓
DEDUP_GUIDE.md (专题指南)
WRITE_MODES.md (配置指南)
TROUBLESHOOTING.md (问题解决)
```

### 📖 各文档用途

| 文档 | 目标用户 | 阅读时间 | 主要内容 |
|------|----------|----------|----------|
| **PROJECT_OVERVIEW.md** | 项目了解者 | 3分钟 | 项目架构、特性、案例 |
| **QUICK_START.md** | 新手用户 | 5分钟 | 快速上手、基础配置 |
| **README.md** | 所有用户 | 15分钟 | 完整功能、详细说明 |
| **DEDUP_GUIDE.md** | 高级用户 | 10分钟 | 去重策略、性能优化 |
| **WRITE_MODES.md** | 配置用户 | 5分钟 | 写入模式、参数配置 |
| **TROUBLESHOOTING.md** | 问题解决者 | 按需 | 故障诊断、解决方案 |

---

## 🎯 核心工具详解

### 🏆 推荐使用优先级

#### ⭐⭐⭐⭐⭐ 强烈推荐
```python
# 1. simple_sync_fixed_multitenant.py
# 用途：多租户首次同步，解决数据覆盖问题
# 特点：按表分组同步，确保所有租户数据完整保存
# 适用：多租户场景的全量同步

# 2. simple_sync_append_safe.py  
# 用途：多租户日常同步，智能去重防重复
# 特点：基于数据哈希的智能去重机制
# 适用：多租户场景的增量同步
```

#### ⭐⭐⭐⭐ 高度推荐
```python
# 3. simple_sync_incremental.py
# 用途：基于时间戳的真正增量同步
# 特点：自动检测时间戳字段，只同步变更数据
# 适用：有时间戳字段的表，性能最佳

# 4. simple_sync_dedup.py
# 用途：MERGE去重，支持UPDATE操作
# 特点：使用BigQuery MERGE语句进行upsert
# 适用：有主键的表，需要更新现有数据
```

#### ⭐⭐⭐ 标准推荐
```python
# 5. simple_sync_fixed.py
# 用途：单租户标准同步
# 特点：支持多种写入模式，功能完整
# 适用：单租户场景，简单可靠
```

### 🔧 工具选择决策树
```
开始
├── 多租户场景？
│   ├── 是
│   │   ├── 首次同步？
│   │   │   ├── 是 → simple_sync_fixed_multitenant.py
│   │   │   └── 否 → simple_sync_append_safe.py
│   │   └── 有时间戳字段？
│   │       ├── 是 → simple_sync_incremental.py
│   │       └── 否 → simple_sync_append_safe.py
│   └── 否（单租户）
│       ├── 有时间戳字段？
│       │   ├── 是 → simple_sync_incremental.py
│       │   └── 否 → simple_sync_fixed.py
│       └── 需要MERGE？
│           ├── 是 → simple_sync_dedup.py
│           └── 否 → simple_sync_fixed.py
```

### 📊 工具功能对比

| 工具 | 多租户 | 去重 | 增量 | MERGE | 复杂度 | 性能 |
|------|--------|------|------|-------|--------|------|
| **multitenant** | ✅ | ❌ | ❌ | ❌ | 中 | 中 |
| **append_safe** | ✅ | ✅ | ✅ | ❌ | 中 | 高 |
| **incremental** | ✅ | ✅ | ✅ | ✅ | 高 | 最高 |
| **dedup** | ✅ | ✅ | ❌ | ✅ | 高 | 中 |
| **fixed** | ❌ | ❌ | ❌ | ❌ | 低 | 中 |

---

## ⚙️ 配置文件详解

### 📋 配置文件层次
```
params.json (主配置)
├── params-example.json (基础示例)
├── params-dedup-example.json (去重示例)
└── params copy.json (备份配置)
```

### 🔧 配置文件用途

#### `params.json` - 主配置文件
```json
{
  "db_host": "MySQL主机地址",
  "db_port": "MySQL端口",
  "db_user": "MySQL用户名",
  "db_pass": "MySQL密码",
  "db_list": "数据库列表，逗号分隔",
  "table_list": "表列表，逗号分隔",
  "bq_project": "BigQuery项目ID",
  "bq_dataset": "BigQuery数据集",
  "write_mode": "写入模式：TRUNCATE/APPEND/EMPTY"
}
```

#### `params-example.json` - 基础配置示例
- 标准配置模板
- 适用于大多数场景
- 包含必要参数说明

#### `params-dedup-example.json` - 去重配置示例
- 专用于去重场景
- 包含去重相关参数
- 增量同步配置示例

---

## 🔧 工具脚本详解

### 🧪 测试和验证工具

#### `test_connection.py` - 连接测试工具
```python
# 功能：
# 1. 测试MySQL连接
# 2. 测试BigQuery连接
# 3. 验证配置文件
# 4. 检查表结构

# 使用：
python test_connection.py
```

#### `install_deps.sh` - 依赖安装脚本
```bash
# 功能：
# 1. 自动安装Python依赖
# 2. 检查Java环境
# 3. 验证安装结果
# 4. 处理常见安装问题

# 使用：
./install_deps.sh
```

### ☁️ 云端运行工具

#### `run_dataflow_fixed.sh` - Dataflow运行脚本
```bash
# 功能：
# 1. 配置Dataflow参数
# 2. 提交云端作业
# 3. 监控作业状态
# 4. 处理云端错误

# 使用：
./run_dataflow_fixed.sh
```

#### `run_local.py` - 本地Apache Beam运行器
```python
# 功能：
# 1. 本地运行Apache Beam管道
# 2. 调试和测试
# 3. 小规模数据处理
# 4. 开发环境验证

# 使用：
python run_local.py
```

---

## 📦 依赖管理

### `requirements.txt` - Python依赖列表
```
apache-beam[gcp]==2.54.0    # Apache Beam核心框架
mysql-connector-python      # MySQL连接器
google-cloud-bigquery       # BigQuery客户端
google-cloud-storage        # Google Cloud Storage
google-auth                 # Google认证
```

### 依赖版本策略
- **固定主版本**：确保兼容性
- **定期更新**：获取安全补丁
- **测试验证**：更新前充分测试

---

## 🗂️ 项目管理文件

### `.gitignore` - Git忽略规则
```
__pycache__/          # Python缓存
*.pyc                 # 编译文件
.env                  # 环境变量
params.json           # 敏感配置（可选）
*.log                 # 日志文件
.DS_Store            # macOS系统文件
```

### 版本控制策略
- **包含**：代码、文档、配置示例
- **排除**：敏感信息、缓存文件、日志
- **分支管理**：main（稳定版）、dev（开发版）

---

## 📊 文件大小和复杂度

### 📈 代码统计
| 文件类型 | 文件数量 | 总行数 | 平均复杂度 |
|----------|----------|--------|------------|
| **核心工具** | 7个 | ~2000行 | 中等 |
| **文档** | 6个 | ~3000行 | 简单 |
| **配置** | 4个 | ~100行 | 简单 |
| **工具脚本** | 4个 | ~500行 | 简单 |

### 🎯 维护建议
1. **定期更新**：每月检查依赖更新
2. **文档同步**：代码变更时同步更新文档
3. **测试覆盖**：新功能添加对应测试
4. **性能监控**：定期检查工具性能

---

## 🔮 未来规划

### 📁 计划新增目录
```
dataflow/
├── tests/                   # 单元测试
├── examples/               # 使用示例
├── scripts/                # 辅助脚本
├── configs/                # 多环境配置
└── monitoring/             # 监控工具
```

### 🚀 功能扩展计划
1. **测试框架**：完整的单元测试和集成测试
2. **监控系统**：数据质量监控和告警
3. **配置管理**：多环境配置管理
4. **性能优化**：并发处理和缓存机制

---

**💡 提示**：这个文件结构经过精心设计，既保证了功能的完整性，又确保了使用的便利性。每个文件都有明确的职责和用途。

*建议按照推荐优先级选择合适的工具，并参考相应的文档进行配置和使用。*