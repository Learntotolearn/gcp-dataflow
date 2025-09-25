#!/usr/bin/env python3
"""
直接运行 MySQL 到 BigQuery 数据同步脚本
无需容器，本地直接执行
"""

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import mysql.connector
import json
import os

# MySQL -> BigQuery 类型映射
MYSQL_TO_BQ_TYPE = {
    "int": "INT64",
    "bigint": "INT64", 
    "tinyint": "INT64",
    "smallint": "INT64",
    "mediumint": "INT64",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC", 
    "float": "FLOAT64",
    "double": "FLOAT64",
    "varchar": "STRING",
    "char": "STRING",
    "text": "STRING",
    "mediumtext": "STRING",
    "longtext": "STRING",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "time": "STRING",
    "json": "STRING",
    "blob": "BYTES",
    "binary": "BYTES", 
    "varbinary": "BYTES",
    "enum": "STRING",
    "set": "STRING"
}

def get_table_schema(db_host, db_port, db_user, db_pass, db_name, table_name):
    """获取 MySQL 表结构并转换为 BigQuery schema"""
    print(f"🔍 获取表结构: {db_name}.{table_name}")
    
    conn = mysql.connector.connect(
        host=db_host, 
        port=int(db_port), 
        user=db_user, 
        password=db_pass, 
        database=db_name
    )
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    
    schema = {"fields": []}
    for field, ftype, *_ in cursor.fetchall():
        base_type = ftype.split("(")[0].lower()
        bq_type = MYSQL_TO_BQ_TYPE.get(base_type, "STRING")
        schema["fields"].append({
            "name": field, 
            "type": bq_type,
            "mode": "NULLABLE"
        })
        print(f"  📋 {field}: {ftype} -> {bq_type}")
    
    # 增加 tenant_id 字段
    schema["fields"].append({
        "name": "tenant_id", 
        "type": "STRING",
        "mode": "NULLABLE"
    })
    
    cursor.close()
    conn.close()
    return schema

def process_table(p, db_name, table_name, args):
    """处理单个库的单张表"""
    print(f"🚀 开始处理: {db_name}.{table_name}")
    
    jdbc_url = f"jdbc:mysql://{args.db_host}:{args.db_port}/{db_name}?useSSL=false&allowPublicKeyRetrieval=true"
    
    schema = get_table_schema(
        args.db_host, args.db_port, args.db_user, args.db_pass, db_name, table_name
    )
    
    bq_table = f"{args.bq_project}:{args.bq_dataset}.{table_name}"
    print(f"📊 目标表: {bq_table}")
    
    (
        p
        | f"Read-{db_name}-{table_name}" >> ReadFromJdbc(
            table_name=table_name,
            driver_class_name="com.mysql.cj.jdbc.Driver", 
            jdbc_url=jdbc_url,
            username=args.db_user,
            password=args.db_pass,
        )
        | f"AddTenant-{db_name}-{table_name}" >> beam.Map(
            lambda row, tenant=db_name: {**row, "tenant_id": tenant}
        )
        | f"WriteBQ-{db_name}-{table_name}" >> WriteToBigQuery(
            table=bq_table,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
    )

def run():
    parser = argparse.ArgumentParser(description="MySQL 到 BigQuery 数据同步工具")
    
    # 数据库连接参数
    parser.add_argument("--db_host", required=True, help="MySQL 主机地址")
    parser.add_argument("--db_port", default="3306", help="MySQL 端口")
    parser.add_argument("--db_user", required=True, help="MySQL 用户名")
    parser.add_argument("--db_pass", required=True, help="MySQL 密码")
    parser.add_argument("--db_list", required=True, help="数据库列表，逗号分隔")
    parser.add_argument("--table_list", required=True, help="表名列表，逗号分隔")
    
    # BigQuery 参数
    parser.add_argument("--bq_project", required=True, help="BigQuery 项目ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery 数据集")
    
    # 运行模式
    parser.add_argument("--runner", default="DirectRunner", 
                       choices=["DirectRunner", "DataflowRunner"],
                       help="运行器类型")
    parser.add_argument("--region", default="us-central1", help="Dataflow 区域")
    parser.add_argument("--temp_location", help="临时文件位置 (GCS)")
    parser.add_argument("--staging_location", help="暂存文件位置 (GCS)")
    
    args = parser.parse_args()
    
    # 解析数据库和表列表
    db_names = [db.strip() for db in args.db_list.split(",")]
    table_names = [t.strip() for t in args.table_list.split(",")]
    
    print(f"🎯 数据库: {db_names}")
    print(f"📋 表名: {table_names}")
    print(f"🏃 运行器: {args.runner}")
    
    # 设置 Pipeline 选项
    pipeline_options = PipelineOptions()
    
    if args.runner == "DataflowRunner":
        # Dataflow 运行器配置
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = args.bq_project
        google_cloud_options.region = args.region
        google_cloud_options.job_name = f"mysql-to-bq-{int(time.time())}"
        
        if args.temp_location:
            google_cloud_options.temp_location = args.temp_location
        if args.staging_location:
            google_cloud_options.staging_location = args.staging_location
            
        pipeline_options.view_as(StandardOptions).runner = "DataflowRunner"
    else:
        # 本地运行器配置
        pipeline_options.view_as(StandardOptions).runner = "DirectRunner"
    
    pipeline_options.view_as(StandardOptions).streaming = False
    
    # 运行 Pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        for db_name in db_names:
            for table_name in table_names:
                process_table(p, db_name, table_name, args)
    
    print("✅ 数据同步完成!")

if __name__ == "__main__":
    import time
    run()