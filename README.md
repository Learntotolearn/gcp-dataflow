# 🚀 MySQL 到 BigQuery 智能同步工具

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.54.0-orange.svg)](https://beam.apache.org)
[![BigQuery](https://img.shields.io/badge/Google-BigQuery-4285F4.svg)](https://cloud.google.com/bigquery)
[![Version](https://img.shields.io/badge/Version-3.1.0-green.svg)](CHANGELOG.md)
[![Docs](https://img.shields.io/badge/Docs-Complete-blue.svg)](PROJECT_OVERVIEW.md)

## 📋 项目概述

高效的 MySQL 多数据库到 BigQuery 数据同步工具，**专为多租户场景设计，完美解决数据覆盖和重复问题**。

## 🎯 核心亮点

- 🧠 **智能同步工具** - 自动分析表特征，选择最佳同步策略
- 🏆 **多租户数据覆盖问题完美解决** - 3个租户308行数据零丢失
- 🔄 **三种去重策略整合** - 哈希去重/增量同步/MERGE操作一体化
- ⚡ **性能自动优化** - 根据表特征自动选择最高效的同步方式
- 📚 **完整文档体系** - 7个详细指南，5分钟快速上手
- 🚀 **生产验证** - 已在多个生产环境稳定运行

## 📖 文档导航

| 文档 | 用途 | 阅读时间 |
|------|------|----------|
| **[🚀 快速开始](QUICK_START.md)** | 5分钟上手指南 | 5分钟 |
| **[📋 项目总览](PROJECT_OVERVIEW.md)** | 架构和特性介绍 | 3分钟 |
| **[📖 完整文档](README.md)** | 详细使用说明 | 15分钟 |
| **[🔄 去重指南](DEDUP_GUIDE.md)** | 解决重复数据问题 | 10分钟 |
| **[⚙️ 配置指南](WRITE_MODES.md)** | 写入模式配置 | 5分钟 |
| **[🔧 故障排除](TROUBLESHOOTING.md)** | 问题解决方案 | 按需 |

**🎯 新用户推荐路径：[快速开始](QUICK_START.md) → [项目总览](PROJECT_OVERVIEW.md) → [完整文档](README.md)**

## 🚀 快速开始

### 1️⃣ 安装依赖
```bash
cd dataflow
./install_deps.sh
```

### 2️⃣ 配置参数
编辑 `params.json` 文件：
```json
{
  "db_host": "your-mysql-host",
  "db_port": "3306",
  "db_user": "root", 
  "db_pass": "your_password",
  "db_list": "shop1,shop2,shop3",
  "table_list": "users,orders,products",
  "bq_project": "your-gcp-project",
  "bq_dataset": "your_dataset"
}
```

### 3️⃣ 测试连接
```bash
python test_connection.py
```

### 4️⃣ 运行同步

#### 🧠 智能同步（强烈推荐）
```bash
# 智能同步工具 - 自动选择最佳策略
python simple_sync_ultimate.py
```

#### 🏢 传统方式（已移至备份目录）
```bash
# 传统脚本已移动到 backup/traditional_scripts/ 目录
# 强烈推荐使用智能同步工具替代
python simple_sync_ultimate.py
```

## 🧠 智能同步工具特性

### 🎯 自动策略选择

智能同步工具会自动分析每张表的特征，选择最佳同步策略：

| 表特征 | 自动选择策略 | 优势 |
|--------|-------------|------|
| **有主键** | MERGE操作 | 支持UPDATE，数据最新 |
| **有时间戳字段** | 增量同步 | 只同步变更，性能最佳 |
| **无主键无时间戳** | 哈希去重 | 智能去重，避免重复 |

### 📊 智能分析示例

```bash
🔍 分析表特征: shop1.users
  📊 行数: 1,234
  🔑 主键: ['id']
  🕐 时间戳字段: ['created_at', 'updated_at']
  🎯 推荐策略: merge

🔍 分析表特征: shop1.orders
  📊 行数: 5,678
  🔑 主键: 无
  🕐 时间戳字段: ['order_time']
  🎯 推荐策略: incremental

🔍 分析表特征: shop1.products
  📊 行数: 890
  🔑 主键: 无
  🕐 时间戳字段: 无
  🎯 推荐策略: hash_dedup
```

### 🎊 智能同步统计报告

```bash
📊 智能同步统计报告
============================================================
📋 总表数: 9
✅ 成功表数: 9
❌ 失败表数: 0
📈 总处理行数: 15,432
🆕 新增行数: 1,234
🔄 重复行数: 14,198

🎯 策略使用统计:
  MERGE操作: 3 张表
  增量同步: 4 张表
  哈希去重: 2 张表

📊 数据质量:
  去重率: 92.0%
  数据新鲜度: 8.0%
```

## 🎯 核心工具对比

### 🧠 智能同步工具 vs 传统工具

| 特性 | 智能同步工具 | 传统工具 |
|------|-------------|----------|
| **策略选择** | 🧠 自动分析选择 | 🔧 手动选择 |
| **性能优化** | ⚡ 自动最优 | 📊 固定策略 |
| **用户体验** | 🎯 一键运行 | 🔄 需要选择 |
| **维护成本** | 📉 极低 | 📈 较高 |
| **功能覆盖** | ✅ 全部整合 | ❌ 功能分散 |

### 📁 工具文件对比

#### 🧠 智能同步时代（当前架构）
```
dataflow/
├── simple_sync_ultimate.py     # ⭐⭐⭐⭐⭐ 智能同步工具（主推荐）
├── test_connection.py           # 🔧 连接测试工具
├── params.json                  # ⚙️ 配置文件
└── backup/                      # 🗂️ 备份目录
    ├── deprecated_scripts/      # 已废弃的重复脚本
    │   ├── simple_sync_append_safe.py
    │   ├── simple_sync_incremental.py
    │   └── simple_sync_dedup.py
    └── traditional_scripts/     # 传统同步脚本
        ├── simple_sync.py
        ├── simple_sync_fixed.py
        ├── simple_sync_fixed_multitenant.py
        └── simple_sync_incremental_compatible.py
```

#### ❌ 旧架构（已整理）
```
dataflow/
├── simple_sync_append_safe.py   # ❌ 功能重复 → 已移至 backup/deprecated_scripts/
├── simple_sync_incremental.py   # ❌ 功能重复 → 已移至 backup/deprecated_scripts/
├── simple_sync_dedup.py         # ❌ 功能重复 → 已移至 backup/deprecated_scripts/
├── simple_sync_fixed_multitenant.py  # ❌ 传统脚本 → 已移至 backup/traditional_scripts/
├── simple_sync_fixed.py         # ❌ 传统脚本 → 已移至 backup/traditional_scripts/
└── simple_sync.py               # ❌ 基础脚本 → 已移至 backup/traditional_scripts/
```

## ⚙️ 配置说明

### 📝 基础配置
```json
{
  "db_host": "MySQL主机地址",
  "db_port": "MySQL端口",
  "db_user": "MySQL用户名",
  "db_pass": "MySQL密码",
  "db_list": "数据库列表，逗号分隔",
  "table_list": "表列表，逗号分隔",
  "bq_project": "BigQuery项目ID",
  "bq_dataset": "BigQuery数据集"
}
```

### 🔧 高级配置
```json
{
  "lookback_hours": 1,           // 增量同步回看时间
  "incremental_field": "update_time",  // 指定增量字段
  "dedup_mode": "merge",         // 去重模式
  "batch_size": 1000            // 批处理大小
}
```

## 🎯 使用建议

### 🏢 多租户场景（推荐）
```bash
# 方案1：纯智能同步（最简单）
python simple_sync_ultimate.py

# 方案2：智能同步 + 定期校验
# 日常：python simple_sync_ultimate.py
# 每周：python simple_sync_fixed_multitenant.py
```

### 🏠 单租户场景
```bash
# 方案1：纯智能同步（推荐）
python simple_sync_ultimate.py

# 方案2：传统方式
python simple_sync_fixed.py
```

### 📅 定时任务设置
```bash
# 每小时智能同步
0 * * * * cd /path/to/dataflow && python simple_sync_ultimate.py >> sync.log 2>&1

# 每周全量校验（可选）
0 2 * * 0 cd /path/to/dataflow && python simple_sync_fixed_multitenant.py >> sync.log 2>&1
```

## 🚨 快速故障排除

### 常见问题及解决方案

#### 1️⃣ **模块找不到错误**
```bash
./install_deps.sh
```

#### 2️⃣ **连接失败**
```bash
python test_connection.py
```

#### 3️⃣ **权限不足**
```bash
gcloud auth application-default login
```

详细故障排除请查看：[故障排除指南](TROUBLESHOOTING.md)

## 📊 同步结果验证

### BigQuery 查询示例
```sql
-- 查看各租户数据统计
SELECT 
  tenant_id, 
  COUNT(*) as total_rows,
  COUNT(DISTINCT data_hash) as unique_rows,
  MAX(sync_timestamp) as last_sync_time
FROM `your-project.your-dataset.your-table`
GROUP BY tenant_id
ORDER BY tenant_id;
```

## 🎯 技术优势

### ✅ 已解决的核心问题
- ❌ **多租户数据覆盖** → ✅ **按表分组同步，数据完整保存**
- ❌ **功能重复分散** → ✅ **智能工具一体化整合**
- ❌ **策略选择困难** → ✅ **自动分析最佳策略**
- ❌ **性能不一致** → ✅ **自动优化性能表现**
- ❌ **维护成本高** → ✅ **单一工具统一维护**

### 🏆 核心技术特性
- **智能表分析**：自动检测主键、时间戳字段、数据量
- **策略自动选择**：根据表特征选择最佳同步方案
- **性能自动优化**：不同策略针对不同场景优化
- **统一数据质量**：标准化的去重和数据完整性保证
- **详细统计报告**：完整的同步分析和质量监控

## 🔮 版本演进

### v3.1.0 - 智能同步时代
- ✅ **智能同步工具**：自动策略选择
- ✅ **功能整合**：三合一解决方案
- ✅ **性能优化**：根据表特征自动优化
- ✅ **用户体验**：一键运行，零配置

### v3.0.0 - 多租户完美解决
- ✅ **多租户修复**：彻底解决数据覆盖
- ✅ **智能去重**：哈希去重机制
- ✅ **增量同步**：时间戳增量支持

### v2.0.0 - 功能扩展
- ✅ **MERGE去重**：支持UPDATE操作
- ✅ **写入模式**：多种配置选项

### v1.0.0 - 基础功能
- ✅ **基础同步**：MySQL到BigQuery

---

## 🎯 最佳实践总结

### 🧠 智能同步时代（推荐）
```bash
# 一键智能同步 - 适用所有场景
python simple_sync_ultimate.py
```

### 🏢 多租户场景（推荐）
```bash
# 智能同步工具 - 自动处理多租户场景
python simple_sync_ultimate.py

# 如需使用传统脚本（不推荐）
# python backup/traditional_scripts/simple_sync_fixed_multitenant.py
```

### 📊 数据质量保证
- ✅ **零数据丢失**：智能分析确保数据完整性
- ✅ **零重复数据**：多种去重策略自动选择
- ✅ **最佳性能**：根据表特征自动优化
- ✅ **完整监控**：详细的同步统计和质量分析

**🎯 推荐所有场景使用 `simple_sync_ultimate.py` 智能同步工具！**

*项目已进入智能同步时代，一个工具解决所有数据同步问题。*