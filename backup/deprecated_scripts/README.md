# 🗂️ 已废弃脚本备份

## 📋 备份说明

这个目录包含了已被合并到 `simple_sync_ultimate.py` 的原始脚本备份。

## 📁 备份文件

### 🔄 已废弃的重复脚本
- **simple_sync_append_safe.py** - 安全追加版本（哈希去重）
- **simple_sync_incremental.py** - 增量同步版本（时间戳过滤）
- **simple_sync_dedup.py** - MERGE去重版本（主键upsert）

## 🎯 废弃原因

这三个脚本存在功能重复：
1. **核心逻辑相似**：都是MySQL到BigQuery同步
2. **去重策略重叠**：都解决APPEND模式重复问题
3. **维护成本高**：三个脚本需要同时维护
4. **用户困扰**：用户难以选择合适的工具

## 🚀 替代方案

所有功能已合并到 **`simple_sync_ultimate.py`** 智能同步工具：

### 🧠 智能策略选择
```python
# 自动检测表特征，选择最佳同步策略
if has_primary_key(table):
    use_merge_strategy()      # 替代 dedup.py
elif has_timestamp_field(table):
    use_incremental_strategy() # 替代 incremental.py
else:
    use_hash_dedup_strategy()  # 替代 append_safe.py
```

### ✅ 功能完全覆盖
- ✅ 哈希去重（原 append_safe 功能）
- ✅ 增量同步（原 incremental 功能）
- ✅ MERGE操作（原 dedup 功能）
- ✅ 多租户支持（所有原功能）
- ✅ 智能策略选择（新增功能）

## 📅 备份时间

备份时间：2024-01-15
合并版本：v3.1.0
新工具：simple_sync_ultimate.py

## 🔄 恢复说明

如果需要恢复原始脚本：
```bash
# 恢复单个脚本
cp backup/deprecated_scripts/simple_sync_append_safe.py ./

# 恢复所有脚本
cp backup/deprecated_scripts/*.py ./
```

## ⚠️ 注意事项

1. **不建议恢复**：新的智能工具功能更强大
2. **配置兼容**：原有配置文件完全兼容
3. **性能更优**：智能选择策略性能更好
4. **维护停止**：这些脚本不再维护更新

---

**💡 建议**：使用新的 `simple_sync_ultimate.py` 获得更好的体验！