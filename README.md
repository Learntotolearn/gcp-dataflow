# 🚀 MySQL 到 BigQuery 数据同步工具

## 📋 项目概述
这是一个用于将 MySQL 多个数据库的表同步到 BigQuery 的工具，支持本地运行和 Dataflow 云端运行两种模式。

## 🛠️ 快速开始

### 1️⃣ 环境设置
```bash
# 安装依赖和下载 JDBC 驱动
./setup.sh
```

### 2️⃣ 配置参数
编辑 `params.json` 文件：
```json
{
  "db_host": "你的MySQL主机",
  "db_port": "3306", 
  "db_user": "用户名",
  "db_pass": "密码",
  "db_list": "数据库1,数据库2,数据库3",
  "table_list": "表1,表2,表3",
  "bq_project": "你的GCP项目ID",
  "bq_dataset": "BigQuery数据集名"
}
```

### 3️⃣ 测试连接
```bash
python test_connection.py
```

### 4️⃣ 运行同步

#### 🏠 本地运行（推荐用于测试）
```bash
./run_local.sh
```

#### ☁️ Dataflow 云端运行（推荐用于生产）
```bash
bsh
```

## 📁 文件说明

| 文件 | 说明 |
|------|------|
| `main.py` | 原始的 Flex Template 代码 |
| `run_local.py` | 改进的本地运行版本 |
| `params.json` | 配置参数文件 |
| `requirements.txt` | Python 依赖 |
| `setup.sh` | 环境设置脚本 |
| `run_local.sh` | 本地运行脚本 |
| `run_dataflow.sh` | Dataflow 运行脚本 |
| `test_connection.py` | 连接测试脚本 |

## 🔧 运行模式对比

| 特性 | 本地运行 | Dataflow 运行 |
|------|----------|---------------|
| **适用场景** | 开发测试 | 生产环境 |
| **数据量** | 小到中等 | 大规模 |
| **并发性** | 单机限制 | 自动扩展 |
| **成本** | 免费 | 按使用付费 |
| **监控** | 基础日志 | 完整监控 |

## 🚨 常见问题

### Q: JDBC 驱动找不到？
```bash
# 重新下载驱动
mkdir -p lib
wget -O lib/mysql-connector-java.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
```

### Q: BigQuery 权限不足？
```bash
# 设置认证
gcloud auth application-default login
```

### Q: MySQL 连接超时？
检查防火墙设置和网络连通性：
```bash
telnet 你的MySQL主机 端口
```

## 🎯 优势对比

### ✅ 直接运行 Python 脚本的优势：
- **简单直接**：无需构建容器镜像
- **快速调试**：可以直接修改代码测试
- **本地开发**：支持本地 IDE 调试
- **灵活配置**：参数可以轻松修改
- **错误排查**：日志直接输出到控制台

### ❌ 容器方式的问题：
- 构建镜像耗时
- 调试困难
- 版本管理复杂
- 依赖环境配置

## 📊 数据流程

```
MySQL 数据库 → Apache Beam Pipeline → BigQuery
     ↓              ↓                    ↓
  多个数据库      添加 tenant_id        自动建表
  多张表         类型转换              数据写入
```

## 🔄 下一步优化建议

1. **增量同步**：添加时间戳字段支持
2. **错误重试**：网络异常自动重试
3. **数据验证**：同步后数据一致性检查
4. **监控告警**：集成 Prometheus/Grafana
5. **配置管理**：支持环境变量和配置文件