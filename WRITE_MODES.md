# 📊 BigQuery 写入模式配置指南

## 🎯 概述

项目现在支持在 `params.json` 中配置 BigQuery 的写入模式，你可以根据不同的业务需求选择合适的写入策略。

## ⚙️ 配置方法

在 `params.json` 文件中添加 `write_mode` 参数：

```json
{
  "db_host": "103.63.139.155",
  "db_port": "58888",
  "db_user": "root",
  "db_pass": "cdb1bd6f5f08acaf",
  "db_list": "shop4282489245696000,shop5703188090880000,shop8693575884800000",
  "table_list": "ttpos_member,ttpos_product_package,ttpos_sale_order_product",
  "bq_project": "diyl-407103",
  "bq_dataset": "TTPOS_TEST_DATE",
  "write_mode": "TRUNCATE"
}
```

## 🔧 支持的写入模式

### 1️⃣ **TRUNCATE** (覆盖模式) - 默认推荐 ⭐

```json
"write_mode": "TRUNCATE"
```

**行为**：每次运行前清空表，然后写入新数据
- ✅ **优点**：数据始终保持最新状态，无重复数据
- ❌ **缺点**：历史数据会丢失
- 🎯 **适用场景**：
  - 全量数据同步
  - 数据一致性要求高的场景
  - 定期完整更新的报表数据

**示例输出**：
```
📝 写入模式: 覆盖 (TRUNCATE)
✅ 数据同步完成: 273 行
```

### 2️⃣ **APPEND** (追加模式)

```json
"write_mode": "APPEND"
```

**行为**：在现有数据后追加新数据，不删除原有数据
- ✅ **优点**：保留所有历史数据
- ❌ **缺点**：可能产生重复数据
- 🎯 **适用场景**：
  - 增量数据同步
  - 需要保留历史记录的场景
  - 日志类数据收集

**示例输出**：
```
📝 写入模式: 追加 (APPEND)
✅ 数据同步完成: 273 行
```

### 3️⃣ **EMPTY** (仅空表模式)

```json
"write_mode": "EMPTY"
```

**行为**：只在表为空时写入数据，如果表已有数据则失败
- ✅ **优点**：最安全，防止意外覆盖数据
- ❌ **缺点**：不支持数据更新
- 🎯 **适用场景**：
  - 初始化数据导入
  - 一次性数据迁移
  - 防止意外覆盖的安全场景

**示例输出**：
```
📝 写入模式: 仅空表 (EMPTY)
✅ 数据同步完成: 273 行
```

## 📋 使用场景对比

| 场景 | 推荐模式 | 原因 |
|------|----------|------|
| **日常数据同步** | `TRUNCATE` | 保持数据最新，避免重复 |
| **历史数据保留** | `APPEND` | 累积所有变更记录 |
| **初次数据导入** | `EMPTY` | 安全防护，避免误操作 |
| **定时全量更新** | `TRUNCATE` | 确保数据完整性 |
| **增量数据收集** | `APPEND` | 保留时间序列数据 |

## 🔄 重复执行行为详解

### TRUNCATE 模式重复执行
```bash
# 第一次运行
python simple_sync_fixed.py
# 结果：BigQuery 表有 273 行数据

# 第二次运行（数据无变化）
python simple_sync_fixed.py  
# 结果：BigQuery 表仍然有 273 行数据（先清空，再写入相同数据）

# 第三次运行（MySQL 新增了 10 行）
python simple_sync_fixed.py
# 结果：BigQuery 表有 283 行数据（清空后写入最新的全部数据）
```

### APPEND 模式重复执行
```bash
# 第一次运行
python simple_sync_fixed.py
# 结果：BigQuery 表有 273 行数据

# 第二次运行（数据无变化）
python simple_sync_fixed.py
# 结果：BigQuery 表有 546 行数据（273 + 273，数据重复了）

# 第三次运行（MySQL 新增了 10 行）
python simple_sync_fixed.py
# 结果：BigQuery 表有 829 行数据（546 + 283）
```

## 🚨 重要注意事项

### ⚠️ APPEND 模式的重复数据问题
如果使用 `APPEND` 模式，重复运行会导致数据重复。解决方案：

1. **添加时间戳字段**：
```sql
-- 查询最新数据
SELECT * FROM table_name 
WHERE sync_timestamp = (SELECT MAX(sync_timestamp) FROM table_name)
```

2. **使用 MERGE 语句去重**：
```sql
-- 创建去重视图
CREATE OR REPLACE VIEW table_name_latest AS
SELECT DISTINCT * FROM table_name
```

3. **定期清理重复数据**：
```sql
-- 删除重复数据，保留最新的
DELETE FROM table_name 
WHERE sync_timestamp < (SELECT MAX(sync_timestamp) FROM table_name)
```

## 🎯 推荐配置

### 生产环境推荐
```json
{
  "write_mode": "TRUNCATE",
  "comment": "生产环境使用覆盖模式，确保数据一致性"
}
```

### 开发测试推荐
```json
{
  "write_mode": "APPEND", 
  "comment": "开发环境使用追加模式，便于调试和数据对比"
}
```

### 数据迁移推荐
```json
{
  "write_mode": "EMPTY",
  "comment": "初次迁移使用空表模式，防止意外覆盖"
}
```

## 🔍 验证写入结果

### 检查数据行数
```sql
-- 查看表的总行数
SELECT COUNT(*) as total_rows 
FROM `diyl-407103.TTPOS_TEST_DATE.ttpos_member`;

-- 按租户查看行数
SELECT tenant_id, COUNT(*) as rows_per_tenant
FROM `diyl-407103.TTPOS_TEST_DATE.ttpos_member`
GROUP BY tenant_id;
```

### 检查数据时间戳（如果有）
```sql
-- 查看最新同步时间
SELECT MAX(_PARTITIONTIME) as last_sync_time
FROM `diyl-407103.TTPOS_TEST_DATE.ttpos_member`;
```

## 🛠️ 故障排除

### 问题：EMPTY 模式失败
```
错误：Table contains data. Cannot write to non-empty table
```
**解决**：表已有数据，改用 `TRUNCATE` 或 `APPEND` 模式

### 问题：APPEND 模式数据重复
```
现象：每次运行后数据量翻倍
```
**解决**：改用 `TRUNCATE` 模式或实现增量同步逻辑

### 问题：权限不足
```
错误：Access Denied: BigQuery BigQuery: Permission denied
```
**解决**：确保有 BigQuery Data Editor 权限

---

**💡 提示**：如果不确定使用哪种模式，建议先用 `TRUNCATE` 模式，这是最安全和常用的选择！