#!/bin/bash
# 基于成功的 simple_sync_fixed.py 的 Dataflow 云端运行脚本

echo "☁️ Dataflow 云端运行 - MySQL 到 BigQuery 同步"
echo "================================================"

# 检查必要文件
if [ ! -f "simple_sync_fixed.py" ]; then
    echo "❌ 错误: simple_sync_fixed.py 文件不存在"
    exit 1
fi

if [ ! -f "params.json" ]; then
    echo "❌ 错误: params.json 配置文件不存在"
    exit 1
fi

# 读取配置
PROJECT_ID=$(python3 -c "import json; print(json.load(open('params.json'))['bq_project'])")
REGION="us-central1"
JOB_NAME="mysql-to-bq-sync-$(date +%Y%m%d-%H%M%S)"
TEMP_LOCATION="gs://ttpos-dataflow-templates/tmp/"
STAGING_LOCATION="gs://ttpos-dataflow-templates/staging/"

echo "🎯 项目ID: $PROJECT_ID"
echo "🌍 区域: $REGION"
echo "📝 作业名称: $JOB_NAME"
echo "📁 临时位置: $TEMP_LOCATION"
echo "📦 暂存位置: $STAGING_LOCATION"

# 创建 Dataflow 兼容版本
cat > dataflow_runner.py << 'EOF'
#!/usr/bin/env python3
"""
Dataflow 云端运行版本 - MySQL 到 BigQuery 数据同步
基于 simple_sync_fixed.py 的成功实现
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import mysql.connector
from google.cloud import bigquery
import json
import sys
from datetime import datetime
from decimal import Decimal

# MySQL -> BigQuery 类型映射
MYSQL_TO_BQ_TYPE = {
    "int": "INT64", "bigint": "INT64", "tinyint": "INT64", 
    "smallint": "INT64", "mediumint": "INT64",
    "decimal": "NUMERIC", "numeric": "NUMERIC",
    "float": "FLOAT64", "double": "FLOAT64",
    "varchar": "STRING", "char": "STRING", "text": "STRING",
    "mediumtext": "STRING", "longtext": "STRING",
    "date": "DATE", "datetime": "TIMESTAMP", "timestamp": "TIMESTAMP",
    "time": "STRING", "json": "STRING", "blob": "BYTES",
    "binary": "BYTES", "varbinary": "BYTES",
    "enum": "STRING", "set": "STRING"
}

class MySQLReader(beam.DoFn):
    def __init__(self, db_config, db_name, table_name):
        self.db_config = db_config
        self.db_name = db_name
        self.table_name = table_name
    
    def process(self, element):
        conn = mysql.connector.connect(
            host=self.db_config['db_host'],
            port=int(self.db_config['db_port']),
            user=self.db_config['db_user'],
            password=self.db_config['db_pass'],
            database=self.db_name
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {self.table_name}")
        
        for row in cursor.fetchall():
            # 添加 tenant_id
            row['tenant_id'] = self.db_name
            
            # 处理特殊数据类型
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    row[key] = float(value)
                elif value is None:
                    row[key] = None
            
            yield row
        
        cursor.close()
        conn.close()

def get_table_schema(db_config, db_name, table_name):
    """获取表结构"""
    conn = mysql.connector.connect(
        host=db_config['db_host'],
        port=int(db_config['db_port']),
        user=db_config['db_user'],
        password=db_config['db_pass'],
        database=db_name
    )
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    
    schema = []
    for field, ftype, *_ in cursor.fetchall():
        base_type = ftype.split("(")[0].lower()
        bq_type = MYSQL_TO_BQ_TYPE.get(base_type, "STRING")
        schema.append({"name": field, "type": bq_type, "mode": "NULLABLE"})
    
    # 添加 tenant_id 字段
    schema.append({"name": "tenant_id", "type": "STRING", "mode": "NULLABLE"})
    
    cursor.close()
    conn.close()
    return schema

def run_pipeline():
    # 读取配置
    with open('params.json', 'r') as f:
        params = json.load(f)
    
    # 解析数据库和表列表
    db_names = [db.strip() for db in params['db_list'].split(",")]
    table_names = [t.strip() for t in params['table_list'].split(",")]
    
    # Pipeline 选项
    pipeline_options = PipelineOptions([
        f'--project={params["bq_project"]}',
        f'--region={sys.argv[1] if len(sys.argv) > 1 else "us-central1"}',
        f'--job_name={sys.argv[2] if len(sys.argv) > 2 else "mysql-to-bq-sync"}',
        f'--temp_location={sys.argv[3] if len(sys.argv) > 3 else "gs://ttpos-dataflow-templates/tmp/"}',
        f'--staging_location={sys.argv[4] if len(sys.argv) > 4 else "gs://ttpos-dataflow-templates/staging/"}',
        '--runner=DataflowRunner',
        '--save_main_session=True'
    ])
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        for db_name in db_names:
            for table_name in table_names:
                # 获取表结构
                schema = get_table_schema(params, db_name, table_name)
                
                # 创建 BigQuery 表规范
                table_spec = f"{params['bq_project']}:{params['bq_dataset']}.{table_name}"
                
                # 数据处理管道
                (
                    pipeline
                    | f"Create-{db_name}-{table_name}" >> beam.Create([None])
                    | f"Read-{db_name}-{table_name}" >> beam.ParDo(MySQLReader(params, db_name, table_name))
                    | f"Write-{db_name}-{table_name}" >> beam.io.WriteToBigQuery(
                        table_spec,
                        schema=schema,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )
                )

if __name__ == "__main__":
    run_pipeline()
EOF

echo ""
echo "🚀 启动 Dataflow 作业..."

# 运行 Dataflow 作业
python3 dataflow_runner.py \
    "$REGION" \
    "$JOB_NAME" \
    "$TEMP_LOCATION" \
    "$STAGING_LOCATION"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Dataflow 作业已成功提交!"
    echo "🔗 查看作业状态: https://console.cloud.google.com/dataflow/jobs?project=$PROJECT_ID"
    echo "📊 查看 BigQuery 数据: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
else
    echo ""
    echo "❌ Dataflow 作业提交失败"
    echo "💡 建议使用本地版本: python3 simple_sync_fixed.py"
fi

# 清理临时文件
rm -f dataflow_runner.py

echo ""
echo "🎯 作业信息:"
echo "   项目: $PROJECT_ID"
echo "   作业名: $JOB_NAME"
echo "   区域: $REGION"