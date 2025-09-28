#!/usr/bin/env python3
"""
简化版 MySQL 到 BigQuery 数据同步工具 - 多租户修复版
修复多租户场景下TRUNCATE模式导致数据覆盖的问题
"""

import mysql.connector
from google.cloud import bigquery
import json
import sys
import hashlib
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

def generate_data_hash(row_data):
    """生成数据行的哈希值，用于智能去重"""
    # 排除系统字段：哈希字段、租户字段、时间戳字段
    hash_data = {}
    for key, value in row_data.items():
        if key not in ['data_hash', 'tenant_id', 'last_updated']:
            if isinstance(value, datetime):
                hash_data[key] = value.isoformat()
            elif isinstance(value, Decimal):
                hash_data[key] = str(value)
            elif value is None:
                hash_data[key] = "NULL"
            else:
                hash_data[key] = value
    
    # 生成哈希
    row_str = json.dumps(hash_data, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

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
    
    # 增加多租户字段
    schema.append(bigquery.SchemaField("tenant_id", "STRING", mode="NULLABLE"))
    schema.append(bigquery.SchemaField("data_hash", "STRING", mode="NULLABLE"))
    schema.append(bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"))
    
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
    current_time = datetime.now().isoformat()
    
    for row in cursor.fetchall():
        # 添加 tenant_id
        row['tenant_id'] = db_name
        
        # 添加同步时间戳
        row['last_updated'] = current_time
        
        # 处理特殊数据类型
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
            elif isinstance(value, Decimal):
                row[key] = float(value)
            elif value is None:
                row[key] = None
        
        # 生成智能哈希用于去重
        row['data_hash'] = generate_data_hash(row)
        
        rows.append(row)
    
    print(f"  📊 读取到 {len(rows)} 行数据")
    cursor.close()
    conn.close()
    return rows

def sync_table_multitenant(params, table_name, db_names):
    """多租户同步单个表 - 修复版本"""
    print(f"\n🚀 开始多租户同步表: {table_name}")
    print(f"🏢 涉及租户: {db_names}")
    
    # 收集所有租户的数据
    all_rows = []
    schema = None
    
    for db_name in db_names:
        try:
            # 获取表结构（只需要获取一次，所有租户结构相同）
            if schema is None:
                schema = get_table_schema(
                    params['db_host'], params['db_port'], 
                    params['db_user'], params['db_pass'], 
                    db_name, table_name
                )
            
            # 获取该租户的表数据
            rows = get_table_data(
                params['db_host'], params['db_port'],
                params['db_user'], params['db_pass'],
                db_name, table_name
            )
            
            # 添加到总数据集
            all_rows.extend(rows)
            print(f"  ✅ 租户 {db_name}: {len(rows)} 行数据")
            
        except Exception as e:
            print(f"  ❌ 租户 {db_name} 同步失败: {str(e)}")
            continue
    
    if not all_rows:
        print(f"  ⚠️ 表 {table_name} 无数据，跳过同步")
        return
    
    print(f"📊 总计收集数据: {len(all_rows)} 行")
    
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
    
    # 一次性写入所有租户数据
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
    
    job = client.load_table_from_json(all_rows, table_id, job_config=job_config)
    job.result()  # 等待作业完成
    
    print(f"  ✅ 表 {table_name} 多租户数据同步完成: {len(all_rows)} 行")
    
    # 显示各租户数据统计
    tenant_stats = {}
    for row in all_rows:
        tenant_id = row['tenant_id']
        tenant_stats[tenant_id] = tenant_stats.get(tenant_id, 0) + 1
    
    print(f"  📈 各租户数据统计:")
    for tenant_id, count in tenant_stats.items():
        print(f"    🏢 {tenant_id}: {count} 行")

def sync_single_table_single_tenant(params, db_name, table_name, write_mode_override=None):
    """单租户单表同步 - 兼容原有逻辑"""
    print(f"\n🚀 开始单租户同步: {db_name}.{table_name}")
    
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
        write_mode = write_mode_override or params.get('write_mode', 'TRUNCATE').upper()
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
    print("🚀 MySQL 到 BigQuery 多租户修复同步工具")
    print("=" * 60)
    
    # 读取配置
    with open('params.json', 'r') as f:
        params = json.load(f)
    
    # 解析数据库和表列表
    db_names = [db.strip() for db in params['db_list'].split(",")]
    table_names = [t.strip() for t in params['table_list'].split(",")]
    
    print(f"🎯 数据库: {db_names}")
    print(f"📋 表名: {table_names}")
    print(f"📊 目标: {params['bq_project']}.{params['bq_dataset']}")
    print(f"🔧 写入模式: {params.get('write_mode', 'TRUNCATE')}")
    
    # 检测是否为多租户场景
    is_multitenant = len(db_names) > 1
    print(f"🏢 多租户模式: {'是' if is_multitenant else '否'}")
    
    if is_multitenant:
        print("\n🔄 使用多租户同步策略（按表分组）")
        # 多租户场景：按表分组同步
        success_count = 0
        for i, table_name in enumerate(table_names, 1):
            print(f"\n[{i}/{len(table_names)}] 处理表: {table_name}")
            try:
                sync_table_multitenant(params, table_name, db_names)
                success_count += 1
            except Exception as e:
                print(f"❌ 表 {table_name} 同步失败: {str(e)}")
                import traceback
                traceback.print_exc()
        
        print(f"\n🎉 多租户同步完成！成功处理了 {success_count}/{len(table_names)} 张表")
        
        # 显示最终统计
        print(f"\n📊 同步统计:")
        print(f"  🏢 租户数量: {len(db_names)}")
        print(f"  📋 表数量: {len(table_names)}")
        print(f"  ✅ 成功表数: {success_count}")
        print(f"  📈 总同步任务: {success_count * len(db_names)} 个租户表")
        
    else:
        print("\n🔄 使用单租户同步策略")
        # 单租户场景：使用原有逻辑
        total_tables = len(db_names) * len(table_names)
        current = 0
        success_count = 0
        
        for db_name in db_names:
            for table_name in table_names:
                current += 1
                print(f"\n[{current}/{total_tables}] 处理中...")
                try:
                    sync_single_table_single_tenant(params, db_name, table_name)
                    success_count += 1
                except Exception as e:
                    print(f"❌ 同步失败: {str(e)}")
                    import traceback
                    traceback.print_exc()
        
        print(f"\n🎉 单租户同步完成！成功处理了 {success_count}/{total_tables} 个表")

if __name__ == "__main__":
    main()