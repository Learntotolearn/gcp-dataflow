# 🔧 故障排除指南

## 🎯 快速诊断

### 🚨 紧急问题快速解决

| 问题症状 | 快速解决 |
|----------|----------|
| **连接失败** | `python test_connection.py` |
| **权限不足** | `gcloud auth application-default login` |
| **模块找不到** | `./install_deps.sh` |
| **数据覆盖** | 使用 `simple_sync_fixed_multitenant.py` |
| **数据重复** | 使用 `simple_sync_append_safe.py` |

---

## 📋 常见问题分类

### 1️⃣ 连接问题

#### ❌ MySQL连接失败
```
错误：Access denied for user 'root'@'xxx.xxx.xxx.xxx'
错误：Can't connect to MySQL server on 'host'
```

**诊断步骤**：
```bash
# 1. 测试连接
python test_connection.py

# 2. 检查网络连通性
telnet 103.6.1.155 58888

# 3. 验证配置文件
cat params.json | grep -E "(db_host|db_port|db_user|db_pass)"
```

**解决方案**：
```bash
# 检查配置文件中的连接参数
{
  "db_host": "正确的主机地址",
  "db_port": "正确的端口号",
  "db_user": "正确的用户名", 
  "db_pass": "正确的密码"
}

# 确保MySQL用户有远程连接权限
# 在MySQL中执行：
# GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'password';
# FLUSH PRIVILEGES;
```

#### ❌ BigQuery权限不足
```
错误：403 Forbidden: Access Denied
错误：BigQuery BigQuery: Permission denied
```

**解决方案**：
```bash
# 1. 重新登录Google Cloud
gcloud auth application-default login

# 2. 检查项目权限
gcloud projects get-iam-policy 你的项目ID

# 3. 确保有以下权限：
# - BigQuery Data Editor
# - BigQuery Job User
# - BigQuery User

# 4. 设置默认项目
gcloud config set project 你的项目ID
```

### 2️⃣ 依赖问题

#### ❌ 模块找不到错误
```
错误：ModuleNotFoundError: No module named 'apache_beam'
错误：ModuleNotFoundError: No module named 'mysql.connector'
```

**解决方案**：
```bash
# 方法1：使用安装脚本（推荐）
./install_deps.sh

# 方法2：手动安装
pip install 'apache-beam[gcp]==2.54.0' mysql-connector-python google-cloud-bigquery

# 方法3：使用国内镜像（如果安装慢）
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple 'apache-beam[gcp]==2.54.0'

# 方法4：检查虚拟环境
which python
pip list | grep -E "(apache-beam|mysql|google-cloud)"
```

#### ❌ Java环境问题
```
错误：java.lang.NoClassDefFoundError
错误：JAVA_HOME is not set
```

**解决方案**：
```bash
# macOS
brew install openjdk@11
export JAVA_HOME=/usr/local/opt/openjdk@11

# Ubuntu/Debian
sudo apt-get update
sudo apt-get install default-jre default-jdk

# CentOS/RHEL
sudo yum install java-11-openjdk java-11-openjdk-devel

# 验证Java安装
java -version
echo $JAVA_HOME
```

### 3️⃣ 数据同步问题

#### ❌ 多租户数据覆盖
```
现象：只有最后一个数据库的数据被保存
原因：TRUNCATE模式按数据库顺序执行，后面的清空了前面的数据
```

**解决方案**：
```bash
# 使用多租户修复版本
python simple_sync_fixed_multitenant.py

# 原理：按表分组同步，避免数据覆盖
# 错误逻辑：for db in dbs: for table in tables: sync(db, table)
# 正确逻辑：for table in tables: for db in dbs: collect_data(db, table) -> sync_all(table)
```

#### ❌ APPEND模式数据重复
```
现象：每次运行后数据量翻倍
第1次：273行 → 第2次：546行 → 第3次：819行
```

**解决方案**：
```bash
# 方法1：使用安全追加版本（推荐）
python simple_sync_append_safe.py

# 方法2：使用TRUNCATE模式
# 修改 params.json: "write_mode": "TRUNCATE"

# 方法3：使用增量同步
python simple_sync_incremental.py

# 方法4：手动清理重复数据
# 在BigQuery中执行去重SQL
```

#### ❌ 增量同步遗漏数据
```
现象：某些更新的数据没有同步
原因：时间戳字段选择不当或回看时间不够
```

**解决方案**：
```bash
# 1. 检查时间戳字段
# 确保选择的字段在数据更新时会自动更新

# 2. 增加回看时间
# 修改 params.json: "lookback_hours": 2

# 3. 手动指定时间戳字段
# 修改 params.json: "incremental_field": "update_time"

# 4. 验证时间戳字段
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'your_table' 
AND COLUMN_NAME LIKE '%time%';
```

### 4️⃣ 性能问题

#### ❌ 同步速度慢
```
现象：大表同步时间过长
```

**优化方案**：
```bash
# 1. 使用增量同步
python simple_sync_incremental.py

# 2. 减少同步表数量
# 修改 params.json，分批同步

# 3. 使用云端Dataflow
./run_dataflow_fixed.sh

# 4. 优化MySQL查询
# 在时间戳字段上创建索引
CREATE INDEX idx_update_time ON your_table(update_time);
```

#### ❌ 内存不足
```
错误：MemoryError
错误：Out of memory
```

**解决方案**：
```bash
# 1. 分批处理
# 修改代码中的batch_size参数

# 2. 使用云端Dataflow
./run_dataflow_fixed.sh

# 3. 增加系统内存
# 或使用更大内存的机器

# 4. 优化查询
# 添加LIMIT限制或WHERE条件过滤
```

### 5️⃣ BigQuery问题

#### ❌ 表结构不匹配
```
错误：Schema mismatch
错误：Field xxx not found in schema
```

**解决方案**：
```bash
# 1. 删除BigQuery表，让程序重新创建
# 在BigQuery控制台删除表

# 2. 手动创建表结构
# 参考MySQL表结构创建对应的BigQuery表

# 3. 使用自动Schema检测
# 确保代码中启用了autodetect=True

# 4. 检查字段类型映射
# MySQL DECIMAL → BigQuery NUMERIC
# MySQL DATETIME → BigQuery TIMESTAMP
```

#### ❌ 配额超限
```
错误：Quota exceeded
错误：Rate limit exceeded
```

**解决方案**：
```bash
# 1. 检查BigQuery配额
# 在Google Cloud Console查看配额使用情况

# 2. 增加延迟
# 在代码中添加time.sleep()

# 3. 分批处理
# 减少单次处理的数据量

# 4. 申请配额增加
# 在Google Cloud Console申请配额增加
```

---

## 🔍 调试技巧

### 📊 日志分析
```bash
# 1. 查看详细日志
python simple_sync_fixed.py 2>&1 | tee debug.log

# 2. 过滤错误信息
grep -i error debug.log

# 3. 查看同步统计
grep -i "同步完成\|rows" debug.log

# 4. 实时监控日志
tail -f sync.log
```

### 🧪 测试方法
```bash
# 1. 测试单个表
# 修改 params.json 只包含一个表
{
  "db_list": "shop1",
  "table_list": "users"
}

# 2. 测试小数据量
# 在MySQL中创建测试表
CREATE TABLE test_table AS SELECT * FROM original_table LIMIT 100;

# 3. 干运行模式
# 在代码中添加 --dry-run 参数支持

# 4. 分步调试
# 在关键位置添加 print() 或 logging
```

### 📈 性能监控
```bash
# 1. 监控系统资源
top -p $(pgrep -f python)
htop

# 2. 监控网络连接
netstat -an | grep :3306
netstat -an | grep :443

# 3. 监控MySQL连接
SHOW PROCESSLIST;  # 在MySQL中执行

# 4. 监控BigQuery作业
# 在BigQuery控制台查看作业历史
```

---

## 📋 问题报告模板

如果以上方法都无法解决问题，请按以下模板提供信息：

### 🔍 环境信息
```bash
# Python版本
python --version

# 依赖版本
pip list | grep -E "(apache-beam|mysql|google-cloud)"

# 操作系统
uname -a  # Linux/macOS
systeminfo  # Windows
```

### 📝 问题描述
```
1. 问题现象：（详细描述遇到的问题）
2. 错误信息：（完整的错误堆栈）
3. 复现步骤：（如何重现这个问题）
4. 预期结果：（期望的正确行为）
5. 实际结果：（实际发生的情况）
```

### ⚙️ 配置信息
```json
// 脱敏后的 params.json 内容
{
  "db_host": "xxx.xxx.xxx.xxx",
  "db_port": "3306",
  "db_user": "root",
  "db_pass": "***",
  "db_list": "db1,db2",
  "table_list": "table1,table2",
  "bq_project": "project-id",
  "bq_dataset": "dataset_name",
  "write_mode": "TRUNCATE"
}
```

### 📊 数据信息
```
1. MySQL表结构：DESCRIBE table_name;
2. 数据量：SELECT COUNT(*) FROM table_name;
3. BigQuery表信息：在控制台查看表详情
4. 同步历史：之前是否成功过？
```

---

## 🎯 预防措施

### ✅ 最佳实践
1. **定期备份**：重要操作前备份BigQuery表
2. **测试环境**：先在测试环境验证
3. **监控告警**：设置数据量异常告警
4. **版本控制**：配置文件纳入版本管理
5. **文档记录**：记录每次重要变更

### 🔄 定期维护
```bash
# 每周检查
1. 数据一致性检查
2. 同步日志分析
3. 性能指标监控
4. 错误率统计

# 每月维护
1. 清理临时文件
2. 更新依赖版本
3. 优化配置参数
4. 备份重要数据
```

---

**💡 提示**：90%的问题都可以通过 `python test_connection.py` 和查看详细日志来快速定位！

*如果问题仍未解决，欢迎提交Issue或查看项目文档获取更多帮助。*