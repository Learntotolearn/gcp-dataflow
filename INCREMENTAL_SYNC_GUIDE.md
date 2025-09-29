# 🚀 智能增量同步工具使用指南

## 📋 概述

智能增量同步工具 (`smart_sync_incremental_optimized.py`) 是一个支持全量和增量同步的高级数据同步工具，集成了本地状态管理、性能优化、异常处理、数据一致性保证等功能。

**最新版本**: v4.1 (状态管理优化版)

## ✨ 核心特性

### 🧠 智能同步策略
- **自动检测**：自动检测表的时间戳字段和主键
- **智能选择**：根据表特征自动选择最佳同步策略
- **状态管理**：记录每张表的同步状态和历史

### 🔄 双模式支持
- **增量同步**：基于时间戳字段，只同步变更数据
- **全量同步**：完整同步所有数据，支持强制执行

### 🛡️ 数据一致性保证
- **安全时间窗口**：回退10分钟避免时钟偏差
- **MERGE操作**：有主键的表支持UPDATE操作
- **哈希去重**：智能去重避免重复数据

### 📊 完整监控
- **本地状态文件**：JSON文件维护同步状态，按数据库分组
- **详细日志**：完整的同步过程记录
- **统计报告**：同步完成后的详细统计
- **状态管理工具**：提供迁移和查看工具

## 🚀 快速开始

### 1️⃣ 配置参数
复制示例配置文件：
```bash
cp params-incremental-example.json params.json
```

编辑 `params.json`：
```json
{
  "db_host": "your-mysql-host",
  "db_port": "3306",
  "db_user": "root",
  "db_pass": "your_password",
  "db_list": "shop1,shop2,shop3",
  "table_list": "users,orders,products",
  "bq_project": "your-gcp-project",
  "bq_dataset": "your_dataset",
  "lookback_minutes": 10,
  "batch_size": 1000
}
```

### 2️⃣ 运行同步

#### 使用脚本运行（推荐）
```bash
# 增量同步
./run_incremental_sync.sh

# 强制全量同步
./run_incremental_sync.sh --full
```

#### 直接运行Python
```bash
# 增量同步
python3 smart_sync_incremental_optimized.py

# 强制全量同步
python3 smart_sync_incremental_optimized.py --full
```

#### 状态管理工具
```bash
# 查看状态概览
python3 test_status_manager.py --overview

# 迁移旧状态文件
python3 migrate_status_files.py

# 预览迁移效果
python3 migrate_status_files.py --preview
```

## 📊 同步策略详解

### 🔄 增量同步条件
满足以下条件时自动使用增量同步：
1. 表中存在时间戳字段（updated_at, created_at等）
2. 该表之前已成功同步过
3. 未指定强制全量同步

### 🏗️ 全量同步条件
以下情况会使用全量同步：
1. 首次同步该表
2. 表中无时间戳字段
3. 手动指定 `--full` 参数
4. 上次同步失败

### 🎯 写入策略
- **有主键表**：使用MERGE操作，支持INSERT和UPDATE
- **无主键表**：使用APPEND模式，依赖哈希去重
- **全量同步**：使用TRUNCATE模式，完全替换数据

## 🔧 配置参数详解

### 基础配置
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

### 高级配置
```json
{
  "lookback_minutes": 10,        // 安全回退时间（分钟）
  "batch_size": 1000,           // 批处理大小
  "max_retries": 3,             // 最大重试次数
  "retry_delay": 5,             // 重试延迟（秒）
  "pool_size": 5,               // 数据库连接池大小
  "status_dir": "sync_status"   // 状态文件目录（本地存储）
}
```

### ⚠️ 重要变更说明
- **v4.1版本移除了MySQL状态存储**，改为本地JSON文件存储
- **数据库用户只需要SELECT权限**，无需写权限
- **状态文件按数据库分组**，减少文件数量，提高管理效率

## 📈 监控和状态管理

### 本地状态文件结构
工具使用本地JSON文件存储同步状态，按数据库分组：

#### 文件位置
```
sync_status/
├── shop1.json              # 数据库shop1的状态
├── shop2.json              # 数据库shop2的状态
└── backup_single_table_files/  # 旧文件备份
```

#### 状态文件格式
```json
{
  "database_info": {
    "tenant_id": "shop1",
    "last_updated": "2025-09-28T16:00:00",
    "total_tables": 3
  },
  "tables": {
    "users": {
      "table_name": "users",
      "last_sync_time": "2025-09-28T15:30:00",
      "sync_status": "SUCCESS",
      "sync_mode": "INCREMENTAL",
      "records_synced": 150,
      "error_message": null,
      "updated_at": "2025-09-28T15:30:05"
    },
    "orders": {
      "table_name": "orders",
      "last_sync_time": "2025-09-28T15:30:00",
      "sync_status": "SUCCESS", 
      "sync_mode": "FULL",
      "records_synced": 1200,
      "error_message": null,
      "updated_at": "2025-09-28T15:30:10"
    }
  }
}
```

### 查看同步状态
```bash
# 查看所有数据库状态概览
python3 test_status_manager.py --overview

# 测试状态管理器功能
python3 test_status_manager.py

# 查看特定数据库状态文件
cat sync_status/shop1.json | jq '.'
```

### 状态文件迁移
如果你有旧版本的单表状态文件，可以使用迁移工具：
```bash
# 预览迁移效果
python3 migrate_status_files.py --preview

# 执行迁移
python3 migrate_status_files.py
```

## 📊 BigQuery表结构

### 系统字段
每个同步的表都会自动添加以下系统字段：
- `tenant_id`: 租户标识（数据库名）
- `sync_timestamp`: 同步时间戳
- `sync_mode`: 同步模式（FULL/INCREMENTAL）

### 表优化
- **分区**：按 `sync_timestamp` 进行日期分区
- **聚簇**：按 `tenant_id` 进行聚簇
- **索引**：自动为主键字段创建索引

## 🚨 故障排除

### 常见问题

#### 1️⃣ 连接失败
```bash
# 检查网络连接
ping your-mysql-host

# 测试MySQL连接
mysql -h your-mysql-host -P 3306 -u root -p

# 测试BigQuery权限
gcloud auth application-default login
```

#### 2️⃣ 权限不足
```bash
# MySQL权限（只需要SELECT权限）
GRANT SELECT ON *.* TO 'readonly_user'@'%';

# BigQuery权限
# 需要 BigQuery Data Editor 和 BigQuery Job User 角色
```

#### 3️⃣ 时间戳字段检测失败
脚本会自动检测常见的时间戳字段名：
- `updated_at`, `update_time`, `last_updated`
- `created_at`, `create_time`, `insert_time`
- `timestamp`, `sync_time`

如果检测失败，请检查表结构中是否存在这些字段。

#### 4️⃣ 数据类型不兼容
检查MySQL和BigQuery类型映射，必要时修改 `MYSQL_TO_BQ_TYPE` 字典。

### 日志分析
```bash
# 查看最新日志
tail -f sync_incremental.log

# 搜索错误信息
grep -i error sync_incremental.log

# 查看同步统计
grep -i "统计报告" sync_incremental.log

# 查看状态文件
ls -la sync_status/
cat sync_status/shop1.json | jq '.database_info'
```

## 📅 定时任务设置

### Crontab示例
```bash
# 每小时增量同步
0 * * * * cd /path/to/dataflow && python3 smart_sync_incremental_optimized.py >> cron.log 2>&1

# 每天凌晨2点全量同步（可选）
0 2 * * * cd /path/to/dataflow && python3 smart_sync_incremental_optimized.py --full >> cron.log 2>&1

# 每周日凌晨进行完整校验
0 1 * * 0 cd /path/to/dataflow && python3 smart_sync_incremental_optimized.py --full >> weekly.log 2>&1
```

### Systemd服务（推荐）
创建服务文件 `/etc/systemd/system/dataflow-sync.service`：
```ini
[Unit]
Description=DataFlow Incremental Sync
After=network.target

[Service]
Type=oneshot
User=dataflow
WorkingDirectory=/path/to/dataflow
ExecStart=/usr/bin/python3 /path/to/dataflow/smart_sync_incremental_optimized.py
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

创建定时器 `/etc/systemd/system/dataflow-sync.timer`：
```ini
[Unit]
Description=Run DataFlow Sync every hour
Requires=dataflow-sync.service

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

启用服务：
```bash
sudo systemctl enable dataflow-sync.timer
sudo systemctl start dataflow-sync.timer
```

## 🎯 最佳实践

### 1️⃣ 同步频率建议
- **高频变更表**：每15-30分钟同步一次
- **中频变更表**：每1-2小时同步一次
- **低频变更表**：每天同步一次
- **静态表**：每周同步一次

### 2️⃣ 性能优化
- **批量大小**：根据网络和内存情况调整 `batch_size`
- **并发控制**：避免同时运行多个同步任务
- **资源监控**：监控CPU、内存、网络使用情况

### 3️⃣ 数据质量保证
- **定期校验**：每周进行一次全量同步校验
- **数据对比**：定期对比源表和目标表数据一致性
- **异常监控**：设置同步失败告警

### 4️⃣ 安全建议
- **权限最小化**：数据库用户只需要SELECT权限
- **密码管理**：使用环境变量或密钥管理系统
- **网络安全**：使用SSL连接和VPN
- **状态文件安全**：确保sync_status目录的读写权限

## 📊 性能指标

### 同步性能参考
- **小表（<1万行）**：全量同步 < 30秒，增量同步 < 10秒
- **中表（1-10万行）**：全量同步 < 5分钟，增量同步 < 1分钟  
- **大表（>10万行）**：全量同步 < 30分钟，增量同步 < 5分钟

### 资源使用参考
- **内存使用**：约 100MB + (batch_size × 行大小)
- **网络带宽**：取决于数据量和压缩比
- **CPU使用**：主要用于数据转换和哈希计算

## 🔮 未来规划

### 即将支持的功能
- **实时同步**：基于MySQL binlog的实时同步
- **数据校验**：自动数据一致性校验
- **性能监控**：详细的性能指标和监控面板
- **多目标支持**：支持同步到多个BigQuery项目
- **Web界面**：状态管理和监控的Web界面

### 版本路线图
- **v4.2**：增加实时同步支持
- **v4.3**：添加Web管理界面
- **v4.4**：支持更多数据源和目标

### 最新更新 (v4.1)
- ✅ **本地状态存储**：移除MySQL依赖，使用本地JSON文件
- ✅ **按数据库分组**：优化状态文件结构，减少文件数量
- ✅ **只读权限支持**：数据库用户只需SELECT权限
- ✅ **迁移工具**：提供状态文件迁移和管理工具
- ✅ **性能优化**：连接池、缓存、批量处理、并行同步

---

## 🎯 总结

智能增量同步工具提供了完整的数据同步解决方案，具有以下优势：

✅ **智能化**：自动分析表结构，选择最佳同步策略  
✅ **高性能**：增量同步大幅提升同步效率  
✅ **高可靠**：完整的状态管理和异常处理  
✅ **易监控**：详细的日志和统计报告  
✅ **易维护**：清晰的配置和简单的操作  

**推荐在生产环境中使用增量同步作为主要同步方式，定期使用全量同步进行数据校验！**