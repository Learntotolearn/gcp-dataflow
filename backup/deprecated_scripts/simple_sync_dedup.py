#!/usr/bin/env python3
"""
MySQL 到 BigQuery 数据同步工具 - 支持去重的APPEND模式
解决APPEND模式下的数据重复问题
"""

import mysql.connector
from google.cloud import bigquery
import json
import sys
from datetime import datetime
from decimal import Decimal
import hashlib

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

def get_table_primary_keys(db_host, db_port, db_user, db_pass, db_name, table_name):
    """获取表的主键字段"""
    conn = mysql.connector.connect(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_pass,
        database=db_name
    )
    cursor = conn.cursor()
    
    # 查询主键信息
    cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = '{db_name}' 
        AND TABLE_NAME = '{table_name}' 
        AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
    """)
    
    primary_keys = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    print(f"  🔑 主键字段: {primary_keys if primary_keys else '无主键'}")
    return primary_keys

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
    # 增加数据哈希字段（用于去重）
    schema.append(bigquery.SchemaField("data_hash", "STRING", mode="NULLABLE"))
    
    cursor.close()
    conn.close()
    return schema

def generate_data_hash(row_data, exclude_fields=None):
    """生成数据行的哈希值，用于去重"""
    if exclude_fields is None:
        exclude_fields = ['sync_timestamp', 'data_hash']
    
    # 创建用于哈希的数据副本
    hash_data = {}
    for key, value in row_data.items():
        if key not in exclude_fields:
            # 统一处理数据类型
            if isinstance(value, datetime):
                hash_data[key] = value.isoformat()
            elif isinstance(value, Decimal):
                hash_data[key] = str(value)
            elif value is None:
                hash_data[key] = "NULL"
            else:
                hash_data[key] = str(value)
    
    # 按键排序确保一致性
    sorted_data = json.dumps(hash_data, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(sorted_data.encode('utf-8')).hexdigest()

def get_table_data(db_host, db_port, db_user, db_pass, db_name, table_name, incremental_field=None, last_sync_time=None):
    """获取 MySQL 表数据，支持增量同步"""
    print(f"📥 读取数据: {db_name}.{table_name}")
    
    conn = mysql.connector.connect(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_pass,
        database=db_name
    )
    cursor = conn.cursor(dictionary=True)
    
    # 构建查询语句
    if incremental_field and last_sync_time:
        query = f"SELECT * FROM {table_name} WHERE {incremental_field} > %s"
        cursor.execute(query, (last_sync_time,))
        print(f"  🔄 增量同步: {incremental_field} > {last_sync_time}")
    else:
        cursor.execute(f"SELECT * FROM {table_name}")
        print(f"  📊 全量同步")
    
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
        
        # 生成数据哈希
        row['data_hash'] = generate_data_hash(row)
        rows.append(row)
    
    print(f"  📊 读取到 {len(rows)} 行数据")
    cursor.close()
    conn.close()
    return rows

def get_last_sync_time(client, table_id):
    """获取上次同步时间"""
    try:
        query = f"""
        SELECT MAX(sync_timestamp) as last_sync
        FROM `{table_id}`
        """
        result = client.query(query).result()
        for row in result:
            return row.last_sync
    except Exception as e:
        print(f"  ⚠️ 无法获取上次同步时间: {e}")
        return None

def sync_table_with_merge(params, db_name, table_name, primary_keys):
    """使用MERGE语句同步表数据，实现upsert操作"""
    print(f"\n🚀 开始MERGE同步: {db_name}.{table_name}")
    
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
    
    # 获取增量数据
    incremental_field = params.get('incremental_field')
    last_sync_time = None
    
    if incremental_field:
        last_sync_time = get_last_sync_time(client, table_id)
        print(f"  🕐 上次同步时间: {last_sync_time}")
    
    # 获取数据
    rows = get_table_data(
        params['db_host'], params['db_port'],
        params['db_user'], params['db_pass'],
        db_name, table_name, incremental_field, last_sync_time
    )
    
    if not rows:
        print(f"  ⚠️ 无新数据，跳过同步")
        return
    
    # 创建临时表
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
        
        # 构建MERGE语句
        if primary_keys:
            # 有主键的情况，使用主键进行MERGE
            merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        else:
            # 无主键的情况，使用数据哈希进行MERGE
            merge_condition = "target.data_hash = source.data_hash AND target.tenant_id = source.tenant_id"
        
        # 获取所有字段（除了用于匹配的字段）
        all_fields = [field.name for field in schema if field.name not in ['data_hash']]
        update_fields = ", ".join([f"{field} = source.{field}" for field in all_fields])
        insert_fields = ", ".join(all_fields + ['data_hash'])
        insert_values = ", ".join([f"source.{field}" for field in all_fields + ['data_hash']])
        
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
        
        # 获取MERGE统计信息
        print(f"  ✅ MERGE完成")
        
    finally:
        # 清理临时表
        client.delete_table(temp_table_id)
        print(f"  🗑️ 清理临时表")

def sync_table_with_dedup(params, db_name, table_name):
    """使用去重逻辑同步表数据"""
    print(f"\n🚀 开始去重同步: {db_name}.{table_name}")
    
    # 获取主键信息
    primary_keys = get_table_primary_keys(
        params['db_host'], params['db_port'],
        params['db_user'], params['db_pass'],
        db_name, table_name
    )
    
    # 根据是否有主键选择不同的同步策略
    if params.get('dedup_mode', 'merge').lower() == 'merge':
        sync_table_with_merge(params, db_name, table_name, primary_keys)
    else:
        # 传统的APPEND模式，但添加去重逻辑
        sync_table_traditional_dedup(params, db_name, table_name)

def sync_table_traditional_dedup(params, db_name, table_name):
    """传统APPEND模式 + 去重逻辑"""
    print(f"  📝 使用传统去重模式")
    
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
    
    if not rows:
        print(f"  ⚠️ 表为空，跳过数据同步")
        return
    
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
    
    # 查询现有数据的哈希值
    existing_hashes = set()
    try:
        query = f"SELECT DISTINCT data_hash FROM `{table_id}` WHERE tenant_id = '{db_name}'"
        result = client.query(query).result()
        existing_hashes = {row.data_hash for row in result}
        print(f"  🔍 现有数据哈希: {len(existing_hashes)} 个")
    except Exception as e:
        print(f"  ⚠️ 无法查询现有数据: {e}")
    
    # 过滤重复数据
    new_rows = []
    duplicate_count = 0
    
    for row in rows:
        if row['data_hash'] not in existing_hashes:
            new_rows.append(row)
        else:
            duplicate_count += 1
    
    print(f"  📊 新数据: {len(new_rows)} 行, 重复数据: {duplicate_count} 行")
    
    if new_rows:
        # 插入新数据
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema
        )
        
        job = client.load_table_from_json(new_rows, table_id, job_config=job_config)
        job.result()
        
        print(f"  ✅ 新数据同步完成: {len(new_rows)} 行")
    else:
        print(f"  ⚠️ 无新数据需要同步")

def main():
    print("🚀 MySQL 到 BigQuery 去重同步工具")
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
    print(f"🔧 去重模式: {params.get('dedup_mode', 'merge')}")
    
    # 同步每个表
    total_tables = len(db_names) * len(table_names)
    current = 0
    success_count = 0
    
    for db_name in db_names:
        for table_name in table_names:
            current += 1
            print(f"\n[{current}/{total_tables}] 处理中...")
            try:
                sync_table_with_dedup(params, db_name, table_name)
                success_count += 1
            except Exception as e:
                print(f"❌ 同步失败: {str(e)}")
                import traceback
                traceback.print_exc()
    
    print(f"\n🎉 去重同步完成！成功处理了 {success_count}/{total_tables} 个表")

if __name__ == "__main__":
    main()