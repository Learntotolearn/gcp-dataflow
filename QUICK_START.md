# ⚡ 5分钟快速开始指南

## 🌟 智能同步工具（强烈推荐）

### 一步到位的解决方案
```bash
# 🎯 推荐：使用智能同步工具
python simple_sync_ultimate.py

# ✨ 特点：
# ✅ 自动检测主键，智能选择策略
# ✅ 有主键：MERGE策略（完美处理数据更新）
# ✅ 无主键：哈希去重策略（避免重复数据）
# ✅ 多租户安全，时间戳记录同步时间
# ✅ 适用于所有场景，无需手动选择
```

## 🎯 传统场景选择（兼容）

### 🏢 多租户场景
如果你有多个数据库需要同步到BigQuery（如多个店铺、多个客户）

### 🏠 单租户场景
如果你只有一个数据库需要同步

---

## 🚀 多租户快速开始

### 1️⃣ 安装依赖（30秒）
```bash
cd dataflow
./install_deps.sh
```

### 2️⃣ 配置参数（2分钟）
编辑 `params.json`：
```json
{
  "db_host": "你的MySQL主机",
  "db_port": "3306",
  "db_user": "root",
  "db_pass": "你的密码",
  "db_list": "shop1,shop2,shop3",
  "table_list": "users,orders,products",
  "bq_project": "你的GCP项目",
  "bq_dataset": "你的数据集",
  "write_mode": "TRUNCATE"
}
```

### 3️⃣ 测试连接（30秒）
```bash
python test_connection.py
```

### 4️⃣ 首次同步（2分钟）
```bash
# 解决多租户数据覆盖问题
python simple_sync_fixed_multitenant.py
```

### 5️⃣ 日常同步（设置定时任务）
```bash
# 智能去重，避免重复数据
python simple_sync_append_safe.py
```

**🎉 完成！你的多租户数据已经完美同步到BigQuery！**

---

## 🏠 单租户快速开始

### 1️⃣ 安装依赖
```bash
cd dataflow
./install_deps.sh
```

### 2️⃣ 配置参数
编辑 `params.json`：
```json
{
  "db_host": "你的MySQL主机",
  "db_port": "3306", 
  "db_user": "root",
  "db_pass": "你的密码",
  "db_list": "your_database",
  "table_list": "users,orders,products",
  "bq_project": "你的GCP项目",
  "bq_dataset": "你的数据集",
  "write_mode": "TRUNCATE"
}
```

### 3️⃣ 测试连接
```bash
python test_connection.py
```

### 4️⃣ 运行同步
```bash
python simple_sync_fixed.py
```

**🎉 完成！你的数据已经同步到BigQuery！**

---

## 🔧 进阶配置

### 如果你的表有时间戳字段
```bash
# 使用增量同步，性能最佳
python simple_sync_incremental.py
```

### 如果你需要MERGE去重
```bash
# 支持UPDATE操作的去重
python simple_sync_dedup.py
```

### 如果遇到重复数据问题
```bash
# 智能去重，解决APPEND模式重复问题
python simple_sync_append_safe.py
```

---

## 📊 验证结果

### 在BigQuery中查看数据
```sql
-- 查看同步的数据
SELECT * FROM `你的项目.你的数据集.你的表名` LIMIT 10;

-- 多租户场景：查看各租户数据统计
SELECT tenant_id, COUNT(*) as row_count
FROM `你的项目.你的数据集.你的表名`
GROUP BY tenant_id;
```

---

## 🚨 常见问题快速解决

### ❌ 连接失败
```bash
# 检查网络连通性
telnet 你的MySQL主机 端口

# 检查配置文件
cat params.json
```

### ❌ 权限不足
```bash
# 登录Google Cloud
gcloud auth application-default login
```

### ❌ 模块找不到
```bash
# 重新安装依赖
pip install 'apache-beam[gcp]==2.54.0' mysql-connector-python google-cloud-bigquery
```

### ❌ 多租户数据覆盖
```bash
# 使用多租户修复版本
python simple_sync_fixed_multitenant.py
```

### ❌ 数据重复
```bash
# 使用安全追加版本
python simple_sync_append_safe.py
```

---

## 📅 设置定时同步

### 多租户推荐定时策略
```bash
# 编辑定时任务
crontab -e

# 每小时智能去重同步
0 * * * * cd /path/to/dataflow && python simple_sync_append_safe.py >> sync.log 2>&1

# 每周日全量校验
0 2 * * 0 cd /path/to/dataflow && python simple_sync_fixed_multitenant.py >> sync.log 2>&1
```

### 单租户推荐定时策略
```bash
# 每小时全量同步
0 * * * * cd /path/to/dataflow && python simple_sync_fixed.py >> sync.log 2>&1

# 或者每15分钟增量同步（如果有时间戳字段）
*/15 * * * * cd /path/to/dataflow && python simple_sync_incremental.py >> sync.log 2>&1
```

---

## 🎯 工具选择速查表

| 场景 | 推荐工具 | 命令 |
|------|----------|------|
| **🌟 智能同步（推荐）** | 智能策略选择 | `python simple_sync_ultimate.py` |
| **多租户首次** | 多租户修复版 | `python simple_sync_fixed_multitenant.py` |
| **有时间戳增量** | 增量同步兼容版 | `python simple_sync_incremental_compatible.py` |
| **基础同步** | 标准版本 | `python simple_sync_fixed.py` |
| **测试连接** | 连接测试 | `python test_connection.py` |

---

## 📚 下一步

- 📖 查看 [完整文档](README.md) 了解所有功能
- 🔄 阅读 [去重指南](DEDUP_GUIDE.md) 解决重复数据问题
- ⚙️ 学习 [写入模式配置](WRITE_MODES.md) 优化同步策略
- 🔧 参考 [故障排除指南](TROUBLESHOOTING.md) 解决问题

---

**🎊 恭喜！你已经成功完成了MySQL到BigQuery的数据同步！**

*如果遇到任何问题，请查看详细文档或提交Issue。*