#!/bin/bash
# 依赖安装脚本

echo "🔧 安装 MySQL 到 BigQuery 数据同步工具依赖..."

# 检查 Python 版本
python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
major_version=$(echo $python_version | cut -d'.' -f1)
minor_version=$(echo $python_version | cut -d'.' -f2)
echo "🐍 Python 版本: $python_version"

# 检查是否为 Python 3.8+
if [[ $major_version -lt 3 ]] || [[ $major_version -eq 3 && $minor_version -lt 8 ]]; then
    echo "❌ 需要 Python 3.8 或更高版本，当前版本: $python_version"
    exit 1
else
    echo "✅ Python 版本检查通过: $python_version"
fi

# 升级 pip
echo "📦 升级 pip..."
python3 -m pip install --upgrade pip

# 安装基础依赖
echo "📥 安装基础依赖..."
python3 -m pip install wheel setuptools

# 分步安装依赖（避免超时）
echo "🚀 安装 Apache Beam..."
python3 -m pip install apache-beam[gcp]==2.54.0 --timeout 300

echo "🔌 安装 MySQL 连接器..."
python3 -m pip install mysql-connector-python==8.0.33

echo "📊 安装 BigQuery 客户端..."
python3 -m pip install google-cloud-bigquery==3.11.4

# 下载 JDBC 驱动
echo "📥 下载 MySQL JDBC 驱动..."
mkdir -p lib
if [ ! -f "lib/mysql-connector-java.jar" ]; then
    wget -O lib/mysql-connector-java.jar \
        https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
    echo "✅ JDBC 驱动下载完成"
else
    echo "✅ JDBC 驱动已存在"
fi

# 验证安装
echo "🧪 验证安装..."
python3 -c "import apache_beam; print('✅ Apache Beam 安装成功')" || echo "❌ Apache Beam 安装失败"
python3 -c "import mysql.connector; print('✅ MySQL 连接器安装成功')" || echo "❌ MySQL 连接器安装失败"
python3 -c "from google.cloud import bigquery; print('✅ BigQuery 客户端安装成功')" || echo "❌ BigQuery 客户端安装失败"

echo ""
echo "🎉 依赖安装完成！"
echo "📋 下一步："
echo "  1. 测试连接: python3 test_connection.py"
echo "  2. 本地运行: ./run_local.sh"