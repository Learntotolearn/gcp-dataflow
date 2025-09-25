# 🚀 MySQL 到 BigQuery 数据同步工具

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.54.0-orange.svg)](https://beam.apache.org)
[![BigQuery](https://img.shields.io/badge/Google-BigQuery-4285F4.svg)](https://cloud.google.com/bigquery)

## 📋 项目概述

高效的 MySQL 多数据库到 BigQuery 数据同步工具，**无需容器，直接运行 Python 脚本**。

### ✨ 核心特性
- 🎯 **多租户支持**：自动为每个数据库添加 `tenant_id` 字段
- 🔄 **智能类型映射**：MySQL 字段完美映射到 BigQuery
- 🚀 **零容器依赖**：直接运行 Python 脚本，无需 Docker
- 📊 **可配置写入模式**：支持覆盖、追加、仅空表三种模式
- 🛡️ **错误处理**：优雅处理空表和异常情况
- 📈 **增量同步支持**：适合有时间戳字段的表（如订单表）

## 🚀 快速开始

### 1️⃣ 安装依赖
```bash
cd dataflow

# 方法1：使用安装脚本（推荐）
./install_deps.sh

# 方法2：手动安装
pip install 'apache-beam[gcp]==2.54.0' mysql-connector-python google-cloud-bigquery
```

### 2️⃣ 配置参数
编辑 `params.json` 文件：
```json
{
  "db_host": "103.63.139.155",
  "db_port": "58888",
  "db_user": "root", 
  "db_pass": "your_password",
  "db_list": "shop1,shop2,shop3",
  "table_list": "table1,table2,table3",
  "bq_project": "your-gcp-project",
  "bq_dataset": "your_dataset",
  "write_mode": "TRUNCATE"
}
```

### 3️⃣ 测试连接
```bash
python test_connection.py
```

### 4️⃣ 运行同步

#### 🏠 本地运行（推荐）
```bash
# 方法1：使用增强版本（推荐）
python simple_sync_fixed.py

# 方法2：使用简化版本
python simple_sync.py

# 方法3：使用 Apache Beam 版本
python run_local.py
```

#### ☁️ Dataflow 云端运行
```bash
./run_dataflow_fixed.sh
```

## ⚙️ 写入模式配置

### 📝 支持的写入模式

| 模式 | 配置值 | 行为 | 适用场景 |
|------|--------|------|----------|
| **覆盖模式** | `TRUNCATE` | 每次清空表后写入最新数据 | 日常同步（推荐） |
| **追加模式** | `APPEND` | 在现有数据后追加新数据 | 历史数据保留 |
| **仅空表模式** | `EMPTY` | 只在表为空时写入数据 | 初次导入 |

### 🔄 重复执行行为

#### TRUNCATE 模式（推荐）
```bash
# 第1次运行：BigQuery 有 273 行
# 第2次运行：BigQuery 仍有 273 行（先清空，再写入相同数据）
# 第3次运行（MySQL新增10行）：BigQuery 有 283 行
```

#### APPEND 模式
```bash
# 第1次运行：BigQuery 有 273 行
# 第2次运行：BigQuery 有 546 行（数据重复）
# 适合增量同步场景
```

详细配置说明请查看：[写入模式配置指南](WRITE_MODES.md)

## 📁 项目文件结构

### **🎯 核心运行文件**
| 文件 | 说明 | 推荐度 |
|------|------|--------|
| `simple_sync_fixed.py` | ⭐ 主推荐，支持可配置写入模式 | ⭐⭐⭐⭐⭐ |
| `simple_sync.py` | 简化版本，固定覆盖模式 | ⭐⭐⭐⭐ |
| `run_local.py` | Apache Beam 版本，功能完整 | ⭐⭐⭐ |
| `test_connection.py` | 连接测试工具 | ⭐⭐⭐⭐⭐ |

### **⚙️ 配置和工具**
| 文件 | 说明 | 必要性 |
|------|------|--------|
| `params.json` | 核心配置文件 | 必须 |
| `requirements.txt` | Python 依赖列表 | 必须 |
| `install_deps.sh` | 依赖安装脚本 | 推荐 |
| `run_dataflow_fixed.sh` | 云端运行脚本 | 可选 |

### **📚 文档**
| 文件 | 说明 |
|------|------|
| `README.md` | 主文档（本文件） |
| `WRITE_MODES.md` | 写入模式详细说明 |

## 🔧 运行模式对比

| 特性 | 本地运行 | Dataflow 运行 |
|------|----------|---------------|
| **适用场景** | 开发测试、小规模数据 | 生产环境、大规模数据 |
| **数据量** | < 10GB | 无限制 |
| **并发性** | 单机限制 | 自动扩展 |
| **成本** | 免费 | 按使用付费 |
| **监控** | 基础日志 | 完整监控面板 |
| **部署复杂度** | 简单 | 中等 |

## 📊 同步结果示例

### 最近一次同步统计
- **数据库数量**：3 个商店数据库
- **表数量**：3 张表/数据库  
- **总同步数据**：308 行记录
- **同步时间**：< 30 秒

| 数据库 | ttpos_member | ttpos_product_package | ttpos_sale_order_product |
|--------|--------------|----------------------|-------------------------|
| shop4282489245696000 | 2 行 | 4 行 | 21 行 |
| shop5703188090880000 | 0 行 | 6 行 | 23 行 |
| shop8693575884800000 | 6 行 | 17 行 | 229 行 |

## 🚨 快速故障排除

### 常见问题及解决方案

#### 1️⃣ **模块找不到错误**
```bash
# 错误：ModuleNotFoundError: No module named 'apache_beam'
# 解决：
./install_deps.sh

# 或手动安装：
pip install 'apache-beam[gcp]==2.54.0' mysql-connector-python google-cloud-bigquery

# 如果安装超时，使用国内镜像：
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple 'apache-beam[gcp]==2.54.0'
```

#### 2️⃣ **MySQL 连接失败**
```bash
# 错误：Access denied 或 Connection timeout
# 解决：
python test_connection.py  # 先测试连接

# 检查网络连通性：
telnet 103.63.139.155 58888

# 检查 params.json 中的连接参数
```

#### 3️⃣ **BigQuery 权限不足**
```bash
# 错误：403 Forbidden 或 Access Denied
# 解决：
gcloud auth application-default login

# 确保有以下权限：
# - BigQuery Data Editor
# - BigQuery Job User
```

#### 4️⃣ **Java 环境问题**
```bash
# 错误：java.lang.NoClassDefFoundError
# 解决：
# macOS: brew install openjdk@11
# Ubuntu: sudo apt-get install default-jre

# 检查 Java 版本：
java -version
```

### 🔍 调试技巧
```bash
# 1. 测试连接
python test_connection.py

# 2. 查看详细日志
python simple_sync_fixed.py  # 直接运行查看输出

# 3. 测试单个表（修改 params.json）
{
  "db_list": "shop4282489245696000",
  "table_list": "ttpos_member"
}
```

## 🔄 定时同步设置

### 使用 cron 设置定时同步
```bash
# 编辑 crontab
crontab -e

# 每天凌晨 2 点运行同步
0 2 * * * cd /path/to/dataflow && python simple_sync_fixed.py >> sync.log 2>&1

# 每小时运行一次
0 * * * * cd /path/to/dataflow && python simple_sync_fixed.py >> sync.log 2>&1

# 每15分钟运行一次（适合增量同步）
*/15 * * * * cd /path/to/dataflow && python simple_sync_fixed.py >> sync.log 2>&1
```

### 使用 systemd 服务（Linux）
```bash
# 创建服务文件
sudo nano /etc/systemd/system/mysql-bq-sync.service

# 服务内容：
[Unit]
Description=MySQL to BigQuery Sync
After=network.target

[Service]
Type=oneshot
User=your-user
WorkingDirectory=/path/to/dataflow
ExecStart=/usr/bin/python3 simple_sync_fixed.py
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target

# 创建定时器
sudo nano /etc/systemd/system/mysql-bq-sync.timer

# 定时器内容：
[Unit]
Description=Run MySQL to BigQuery Sync every hour
Requires=mysql-bq-sync.service

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target

# 启用服务
sudo systemctl enable mysql-bq-sync.timer
sudo systemctl start mysql-bq-sync.timer
```

## 🎊 查看同步结果

### BigQuery 控制台
- **项目**：`diyl-407103`
- **数据集**：`TTPOS_TEST_DATE`
- **表**：自动创建，包含 `tenant_id` 字段

### SQL 查询示例
```sql
-- 查看各商店会员统计
SELECT tenant_id, COUNT(*) as member_count 
FROM `diyl-407103.TTPOS_TEST_DATE.ttpos_member` 
GROUP BY tenant_id;

-- 查看销售数据汇总
SELECT 
  tenant_id, 
  COUNT(*) as order_count,
  SUM(CAST(total_price AS NUMERIC)) as total_sales
FROM `diyl-407103.TTPOS_TEST_DATE.ttpos_sale_order_product` 
GROUP BY tenant_id;

-- 查看最新同步时间（如果表有时间戳字段）
SELECT 
  tenant_id,
  MAX(FROM_UNIXTIME(update_time)) as last_update_time
FROM `diyl-407103.TTPOS_TEST_DATE.ttpos_sale_order_product` 
GROUP BY tenant_id;

-- 查看数据增长趋势
SELECT 
  DATE(FROM_UNIXTIME(create_time)) as date,
  COUNT(*) as daily_orders
FROM `diyl-407103.TTPOS_TEST_DATE.ttpos_sale_order_product` 
WHERE create_time > 0
GROUP BY DATE(FROM_UNIXTIME(create_time))
ORDER BY date DESC;
```

## 🎯 技术优势

### ✅ 已解决的问题
- ❌ **容器依赖** → ✅ **直接运行 Python**
- ❌ **Python 3.13 兼容性** → ✅ **完美兼容 3.9+**
- ❌ **复杂部署** → ✅ **一键运行**
- ❌ **调试困难** → ✅ **本地直接调试**
- ❌ **固定写入模式** → ✅ **可配置写入模式**

### 🏆 核心技术特性
- **自动 Schema 检测**：动态获取 MySQL 表结构
- **类型安全转换**：正确处理 Decimal、DateTime 等特殊类型
- **多租户架构**：自动添加 `tenant_id` 字段区分不同商店
- **可配置写入模式**：支持覆盖、追加、仅空表三种模式
- **增量同步友好**：支持基于时间戳的增量同步
- **优雅错误处理**：空表、网络异常等情况的智能处理

## 📈 增量同步建议

### 适合增量同步的表
对于有完整时间戳字段的表（如 `ttpos_sale_order_product`），建议：

1. **使用 APPEND 模式**：
```json
{
  "write_mode": "APPEND"
}
```

2. **实现增量查询逻辑**：
```sql
-- 基于 update_time 的增量查询
SELECT * FROM ttpos_sale_order_product 
WHERE update_time > {last_sync_timestamp}
   OR (delete_time > {last_sync_timestamp} AND delete_time > 0)
```

3. **定期全量校验**：
```bash
# 每周进行一次全量同步校验
0 2 * * 0 cd /path/to/dataflow && python simple_sync_fixed.py
```

## 🔮 下一步优化建议

### 短期优化
1. **增量同步实现**：基于时间戳字段的增量同步逻辑
2. **数据验证**：同步后数据一致性检查
3. **错误重试**：网络异常自动重试机制

### 中期优化
4. **监控告警**：集成 Prometheus/Grafana 监控
5. **配置管理**：支持环境变量和多环境配置
6. **性能优化**：并发同步和批量处理

### 长期优化
7. **实时同步**：基于 MySQL binlog 的实时数据同步
8. **数据血缘**：数据流向和依赖关系追踪
9. **自动化运维**：故障自愈和智能调度

---

**🎯 推荐使用 `simple_sync_fixed.py` 进行日常数据同步！**

*项目已完全摆脱容器依赖，支持 Python 3.9+ 环境直接运行。*