#!/usr/bin/env python3
"""
简化版 MySQL 到 BigQuery 数据同步工具
不使用 Apache Beam JDBC，直接使用 Python 连接器
"""

import mysql.connector
from google.cloud import bigquery
import json
import sys
from datetime import datetime
from decimal import Decimal

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
    
    schema = []
    for field, ftype, *_ in cursor.fetchall():
        base_type = ftype.split("(")[0].lower()
        bq_type = MYSQL_TO_BQ_TYPE.get(base_type, "STRING")
        schema.append(bigquery.SchemaField(field, bq_type, mode="NULLABLE"))
        print(f"  📋 {field}: {ftype} -> {bq_type}")
    
    # 增加 tenant_id 字段
    schema.append(bigquery.SchemaField("tenant_id", "STRING", mode="NULLABLE"))
    
    cursor.close()
    conn.close()
    return schema

def get_table_data(db_host, db_port, db_user, db_pass, db_name, table_name):
    """获取 MySQL 表数据"""
    print(f"📥 读取数据: {db_name}.{table_name}")
    
    conn = mysql.connector.connect(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_pass,
        database=db_name
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {table_name}")
    
    rows = []
    for row in cursor.fetchall():
        # 添加 tenant_id
        row['tenant_id'] = db_name
        
        # 处理特殊数据类型
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
            elif isinstance(value, Decimal):
                row[key] = float(value)
            elif value is None:
                row[key] = None
        
        rows.append(row)
    
    print(f"  📊 读取到 {len(rows)} 行数据")
    cursor.close()
    conn.close()
    return rows

def sync_table(params, db_name, table_name):
    """同步单个表"""
    print(f"\n🚀 开始同步: {db_name}.{table_name}")
    
    # 获取表结构
    schema = get_table_schema(
        params['db_host'], params['db_port'], 
        params['db_user'], params['db_pass'], 
        db_name, table_name
    )
    
    # 获取表数据
    rows = get_table_data(
        params['db_host'], params['db_port'],
        params['db_user'], params['db_pass'],
        db_name, table_name
    )
    
    # 创建 BigQuery 客户端
    client = bigquery.Client(project=params['bq_project'])
    
    # 创建数据集（如果不存在）
    dataset_id = params['bq_dataset']
    try:
        client.get_dataset(dataset_id)
        print(f"  ✅ 数据集已存在: {dataset_id}")
    except:
        dataset = bigquery.Dataset(f"{params['bq_project']}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"  🆕 创建数据集: {dataset_id}")
    
    # 创建表（如果不存在）
    table_id = f"{params['bq_project']}.{dataset_id}.{table_name}"
    try:
        table = client.get_table(table_id)
        print(f"  ✅ 表已存在: {table_name}")
    except:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"  🆕 创建表: {table_name}")
    
    # 插入数据
    if rows:
        # 根据配置选择写入模式
        write_mode = params.get('write_mode', 'TRUNCATE').upper()
        if write_mode == 'APPEND':
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            print(f"  📝 写入模式: 追加 (APPEND)")
        elif write_mode == 'EMPTY':
            write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
            print(f"  📝 写入模式: 仅空表 (EMPTY)")
        else:  # 默认 TRUNCATE
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            print(f"  📝 写入模式: 覆盖 (TRUNCATE)")
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema=schema
        )
        
        job = client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()  # 等待作业完成
        
        print(f"  ✅ 数据同步完成: {len(rows)} 行")
    else:
        print(f"  ⚠️ 表为空，跳过数据同步")

def main():
    print("🚀 MySQL 到 BigQuery 简化同步工具")
    print("=" * 50)
    
    # 读取配置
    with open('params.json', 'r') as f:
        params = json.load(f)
    
    # 解析数据库和表列表
    db_names = [db.strip() for db in params['db_list'].split(",")]
    table_names = [t.strip() for t in params['table_list'].split(",")]
    
    print(f"🎯 数据库: {db_names}")
    print(f"📋 表名: {table_names}")
    print(f"📊 目标: {params['bq_project']}.{params['bq_dataset']}")
    
    # 同步每个表
    total_tables = len(db_names) * len(table_names)
    current = 0
    success_count = 0
    
    for db_name in db_names:
        for table_name in table_names:
            current += 1
            print(f"\n[{current}/{total_tables}] 处理中...")
            try:
                sync_table(params, db_name, table_name)
                success_count += 1
            except Exception as e:
                print(f"❌ 同步失败: {str(e)}")
                import traceback
                traceback.print_exc()
    
    print(f"\n🎉 同步完成！成功处理了 {success_count}/{total_tables} 个表")

if __name__ == "__main__":
    main()