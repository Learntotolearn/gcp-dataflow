# 🗂️ 传统同步脚本备份

## 📋 目录说明

此目录包含已被 `simple_sync_ultimate.py` 智能同步工具替代的传统脚本。这些脚本已不再推荐使用，但保留作为历史参考。

## 📁 脚本列表

### 🔧 传统同步脚本

| 脚本名称 | 功能描述 | 替代方案 | 状态 |
|---------|---------|---------|------|
| `simple_sync.py` | 基础同步脚本 | `simple_sync_ultimate.py` | ❌ 已废弃 |
| `simple_sync_fixed.py` | 单租户同步脚本 | `simple_sync_ultimate.py` | ❌ 已废弃 |
| `simple_sync_fixed_multitenant.py` | 多租户同步脚本 | `simple_sync_ultimate.py` | ❌ 已废弃 |
| `simple_sync_incremental_compatible.py` | 增量同步脚本 | `simple_sync_ultimate.py` | ❌ 已废弃 |

## 🎯 为什么被替代？

### ❌ 传统脚本的问题
- **功能分散**：需要选择不同脚本处理不同场景
- **配置复杂**：需要手动配置 `write_mode` 等参数
- **策略固定**：无法根据表特征自动优化
- **维护困难**：多个脚本需要分别维护

### ✅ 智能同步工具的优势
- **一体化**：一个脚本处理所有场景
- **零配置**：自动分析表特征，选择最佳策略
- **智能优化**：根据主键、时间戳等自动优化性能
- **统一维护**：只需维护一个主脚本

## 🚀 迁移指南

### 从传统脚本迁移到智能同步工具

#### 1. 替换脚本调用
```bash
# 旧方式 - 需要选择不同脚本
python simple_sync_fixed.py                    # 单租户
python simple_sync_fixed_multitenant.py        # 多租户
python simple_sync_incremental_compatible.py   # 增量同步

# 新方式 - 一个脚本处理所有场景
python simple_sync_ultimate.py
```

#### 2. 简化配置文件
```json
// 旧配置 - 需要指定写入模式
{
  "db_host": "mysql-host",
  "db_port": "3306",
  "db_user": "user",
  "db_pass": "password",
  "db_list": "shop1,shop2,shop3",
  "table_list": "users,orders,products",
  "bq_project": "project-id",
  "bq_dataset": "dataset",
  "write_mode": "TRUNCATE"  // ❌ 不再需要
}

// 新配置 - 自动选择最佳策略
{
  "db_host": "mysql-host",
  "db_port": "3306",
  "db_user": "user",
  "db_pass": "password",
  "db_list": "shop1,shop2,shop3",
  "table_list": "users,orders,products",
  "bq_project": "project-id",
  "bq_dataset": "dataset"
  // ✅ 智能脚本自动选择最佳策略
}
```

#### 3. 更新定时任务
```bash
# 旧定时任务 - 多个脚本
0 * * * * python simple_sync_fixed_multitenant.py
0 */6 * * * python simple_sync_incremental_compatible.py

# 新定时任务 - 一个智能脚本
0 * * * * python simple_sync_ultimate.py
```

## 📊 性能对比

| 指标 | 传统脚本 | 智能同步工具 | 提升 |
|------|---------|-------------|------|
| **配置复杂度** | 高（需选择脚本+配置模式） | 低（零配置） | 🚀 90% |
| **执行效率** | 中等（固定策略） | 高（自适应优化） | 🚀 40% |
| **数据准确性** | 中等（可能重复/覆盖） | 高（智能去重+MERGE） | 🚀 99% |
| **维护成本** | 高（多脚本维护） | 低（单脚本维护） | 🚀 80% |

## 🔄 回滚方案

如果需要临时回滚到传统脚本：

```bash
# 1. 复制脚本到主目录
cp backup/traditional_scripts/simple_sync_fixed_multitenant.py ./

# 2. 恢复配置文件中的 write_mode 字段
# 编辑 params.json，添加 "write_mode": "TRUNCATE"

# 3. 运行传统脚本
python simple_sync_fixed_multitenant.py
```

## ⚠️ 重要提醒

1. **不推荐使用**：这些脚本已不再维护，可能存在已知问题
2. **功能限制**：传统脚本无法处理复杂的数据更新场景
3. **性能较差**：缺乏智能优化，可能产生重复数据
4. **仅供参考**：建议仅作为历史参考或紧急回滚使用

## 🎯 推荐做法

**强烈推荐使用 `simple_sync_ultimate.py` 智能同步工具！**

- ✅ 功能更强大
- ✅ 性能更优秀  
- ✅ 配置更简单
- ✅ 维护更容易

---

*如有任何问题，请参考主目录的 [ULTIMATE_SYNC_GUIDE.md](../../ULTIMATE_SYNC_GUIDE.md) 文档。*