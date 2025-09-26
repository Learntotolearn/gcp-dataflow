#!/usr/bin/env python3
"""
MySQL 到 BigQuery 增量同步工具
基于时间戳字段实现真正的增量同步，避免数据重复
"""

import mysql.connector
from google.cloud import bigquery
import json
import sys
from datetime import datetime, timedelta
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

def detect_timestamp_fields(db_host, db_port, db_user, db_pass, db_name, table_name):
    """自动检测表中的时间戳字段"""
    conn = mysql.connector.connect(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_pass,
        database=db_name
    )
    cursor = conn.cursor()
    
    # 查询表结构，寻找时间戳字段
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{db_name}' 
        AND TABLE_NAME = '{table_name}'
        AND DATA_TYPE IN ('timestamp', 'datetime', 'int', 'bigint')
        ORDER BY ORDINAL_POSITION
    """)
    
    timestamp_fields = []
    for column_name, data_type, column_default in cursor.fetchall():
        # 常见的时间戳字段名
        if any(keyword in column_name.lower() for keyword in 
               ['time', 'date', 'created', 'updated', 'modified']):
            timestamp_fields.append({
                'name': column_name,
                'type': data_type,
                'default': column_default
            })
    
    cursor.close()
    conn.close()
    
    print(f"  🕐 检测到时间戳字段: {[f['name'] for f in timestamp_fields]}")
    return timestamp_fields

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
    # 增加同步时间戳字段
    schema.append(bigquery.SchemaField("sync_timestamp", "TIMESTAMP", mode="NULLABLE"))
    
    cursor.close()
    conn.close()
    return schema

def get_last_sync_timestamp(client, table_id, timestamp_field, tenant_id):
    """获取指定租户的最后同步时间戳"""
    try:
        # 根据时间戳字段类型构建查询
        if timestamp_field['type'] in ['timestamp', 'datetime']:
            query = f"""
            SELECT MAX(UNIX_TIMESTAMP({timestamp_field['name']})) as last_timestamp
            FROM `{table_id}`
            WHERE tenant_id = '{tenant_id}'
            """
        else:  # int, bigint类型的时间戳
            query = f"""
            SELECT MAX({timestamp_field['name']}) as last_timestamp
            FROM `{table_id}`
            WHERE tenant_id = '{tenant_id}'
            """
        
        result = client.query(query).result()
        for row in result:
            if row.last_timestamp:
                return int(row.last_timestamp)
    except Exception as e:
        print(f"  ⚠️ 无法获取上次同步时间戳: {e}")
    
    return None

def get_incremental_data(db_host, db_port, db_user, db_pass, db_name, table_name, 
                        timestamp_field, last_timestamp=None, lookback_hours=1):
    """获取增量数据"""
    print(f"📥 读取增量数据: {db_name}.{table_name}")
    
    conn = mysql.connector.connect(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_pass,
        database=db_name
    )
    cursor = conn.cursor(dictionary=True)
    
    # 构建增量查询条件
    if last_timestamp:
        # 减去回看时间，防止遗漏数据
        lookback_timestamp = last_timestamp - (lookback_hours * 3600)
        
        if timestamp_field['type'] in ['timestamp', 'datetime']:
            query = f"""
            SELECT * FROM {table_name} 
            WHERE UNIX_TIMESTAMP({timestamp_field['name']}) > %s
            ORDER BY {timestamp_field['name']}
            """
        else:  # int, bigint类型
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {timestamp_field['name']} > %s
            ORDER BY {timestamp_field['name']}
            """
        
        cursor.execute(query, (lookback_timestamp,))
        print(f"  🔄 增量同步: {timestamp_field['name']} > {lookback_timestamp}")
    else:
        # 首次同步，获取最近24小时的数据
        if timestamp_field['type'] in ['timestamp', 'datetime']:
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {timestamp_field['name']} >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY {timestamp_field['name']}
            """
        else:  # int类型时间戳
            current_timestamp = int(datetime.now().timestamp())
            day_ago_timestamp = current_timestamp - (24 * 3600)
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {timestamp_field['name']} >= %s
            ORDER BY {timestamp_field['name']}
            """
            cursor.execute(query, (day_ago_timestamp,))
        
        if timestamp_field['type'] in ['timestamp', 'datetime']:
            cursor.execute(query)
        
        print(f"  🆕 首次同步: 获取最近24小时数据")
    
    rows = []
    current_time = datetime.now()
    
    for row in cursor.fetchall():
        # 添加 tenant_id
        row['tenant_id'] = db_name
        # 添加同步时间戳
        row['sync_timestamp'] = current_time
        
        # 处理特殊数据类型
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
            elif isinstance(value, Decimal):
                row[key] = float(value)
            elif value is None:
                row[key] = None
        
        rows.append(row)
    
    print(f"  📊 读取到 {len(rows)} 行增量数据")
    cursor.close()
    conn.close()
    return rows

def sync_table_incremental(params, db_name, table_name):
    """增量同步单个表"""
    print(f"\n🚀 开始增量同步: {db_name}.{table_name}")
    
    # 检测时间戳字段
    timestamp_fields = detect_timestamp_fields(
        params['db_host'], params['db_port'],
        params['db_user'], params['db_pass'],
        db_name, table_name
    )
    
    if not timestamp_fields:
        print(f"  ⚠️ 未检测到时间戳字段，跳过增量同步")
        return
    
    # 选择主要的时间戳字段（优先选择update_time, modified_time等）
    primary_timestamp = None
    for field in timestamp_fields:
        if any(keyword in field['name'].lower() for keyword in ['update', 'modified']):
            primary_timestamp = field
            break
    
    if not primary_timestamp:
        primary_timestamp = timestamp_fields[0]  # 使用第一个时间戳字段
    
    print(f"  🎯 使用时间戳字段: {primary_timestamp['name']} ({primary_timestamp['type']})")
    
    # 获取表结构
    schema = get_table_schema(
        params['db_host'], params['db_port'], 
        params['db_user'], params['db_pass'], 
        db_name, table_name
    )
    
    # 创建 BigQuery 客户端
    client = bigquery.Client(project=params['bq_project'])
    
    # 创建数据集和表
    dataset_id = params['bq_dataset']
    table_id = f"{params['bq_project']}.{dataset_id}.{table_name}"
    
    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(f"{params['bq_project']}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"  🆕 创建数据集: {dataset_id}")
    
    try:
        table = client.get_table(table_id)
        print(f"  ✅ 表已存在: {table_name}")
    except:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"  🆕 创建表: {table_name}")
    
    # 获取上次同步时间戳
    last_timestamp = get_last_sync_timestamp(client, table_id, primary_timestamp, db_name)
    print(f"  🕐 上次同步时间戳: {last_timestamp}")
    
    # 获取增量数据
    lookback_hours = params.get('lookback_hours', 1)
    rows = get_incremental_data(
        params['db_host'], params['db_port'],
        params['db_user'], params['db_pass'],
        db_name, table_name, primary_timestamp, 
        last_timestamp, lookback_hours
    )
    
    if not rows:
        print(f"  ⚠️ 无增量数据，跳过同步")
        return
    
    # 使用MERGE语句进行upsert操作
    temp_table_id = f"{table_id}_temp_{int(datetime.now().timestamp())}"
    temp_table = bigquery.Table(temp_table_id, schema=schema)
    temp_table = client.create_table(temp_table)
    print(f"  🔄 创建临时表: {temp_table_id}")
    
    try:
        # 将数据加载到临时表
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema
        )
        
        job = client.load_table_from_json(rows, temp_table_id, job_config=job_config)
        job.result()
        print(f"  📥 数据加载到临时表: {len(rows)} 行")
        
        # 构建MERGE语句 - 基于主键或时间戳+tenant_id
        merge_condition = f"target.{primary_timestamp['name']} = source.{primary_timestamp['name']} AND target.tenant_id = source.tenant_id"
        
        # 获取所有字段
        all_fields = [field.name for field in schema]
        update_fields = ", ".join([f"{field} = source.{field}" for field in all_fields])
        insert_fields = ", ".join(all_fields)
        insert_values = ", ".join([f"source.{field}" for field in all_fields])
        
        merge_query = f"""
        MERGE `{table_id}` AS target
        USING `{temp_table_id}` AS source
        ON {merge_condition}
        WHEN MATCHED THEN
          UPDATE SET {update_fields}
        WHEN NOT MATCHED THEN
          INSERT ({insert_fields})
          VALUES ({insert_values})
        """
        
        print(f"  🔄 执行MERGE操作...")
        merge_job = client.query(merge_query)
        merge_result = merge_job.result()
        
        print(f"  ✅ 增量同步完成")
        
    finally:
        # 清理临时表
        client.delete_table(temp_table_id)
        print(f"  🗑️ 清理临时表")

def main():
    print("🚀 MySQL 到 BigQuery 增量同步工具")
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
    print(f"⏰ 回看时间: {params.get('lookback_hours', 1)} 小时")
    
    # 同步每个表
    total_tables = len(db_names) * len(table_names)
    current = 0
    success_count = 0
    
    for db_name in db_names:
        for table_name in table_names:
            current += 1
            print(f"\n[{current}/{total_tables}] 处理中...")
            try:
                sync_table_incremental(params, db_name, table_name)
                success_count += 1
            except Exception as e:
                print(f"❌ 同步失败: {str(e)}")
                import traceback
                traceback.print_exc()
    
    print(f"\n🎉 增量同步完成！成功处理了 {success_count}/{total_tables} 个表")

if __name__ == "__main__":
    main()