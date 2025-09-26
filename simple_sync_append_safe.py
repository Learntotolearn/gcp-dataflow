#!/usr/bin/env python3
"""
MySQL 到 BigQuery 安全追加同步工具
解决APPEND模式重复数据问题，使用简单有效的去重策略
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
    # 增加数据哈希字段用于去重
    schema.append(bigquery.SchemaField("data_hash", "STRING", mode="NULLABLE"))
    # 增加同步时间戳
    schema.append(bigquery.SchemaField("sync_timestamp", "TIMESTAMP", mode="NULLABLE"))
    
    cursor.close()
    conn.close()
    return schema

def generate_data_hash(row_data):
    """生成数据行的哈希值，用于去重"""
    # 排除哈希字段和同步时间戳
    hash_data = {}
    for key, value in row_data.items():
        if key not in ['data_hash', 'sync_timestamp']:
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

def get_existing_hashes(client, table_id, tenant_id):
    """获取现有数据的哈希值"""
    try:
        query = f"""
        SELECT DISTINCT data_hash 
        FROM `{table_id}` 
        WHERE tenant_id = '{tenant_id}' AND data_hash IS NOT NULL
        """
        result = client.query(query).result()
        existing_hashes = {row.data_hash for row in result}
        print(f"  🔍 租户 {tenant_id} 现有数据哈希: {len(existing_hashes)} 个")
        return existing_hashes
    except Exception as e:
        print(f"  ⚠️ 无法查询现有数据哈希: {e}")
        return set()

def sync_table_safe_append(params, table_name, db_names):
    """安全追加同步单个表"""
    print(f"\n🚀 开始安全追加同步表: {table_name}")
    print(f"🏢 涉及租户: {db_names}")
    
    # 获取表结构（使用第一个数据库）
    schema = get_table_schema(
        params['db_host'], params['db_port'], 
        params['db_user'], params['db_pass'], 
        db_names[0], table_name
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
    
    # 检查表是否存在
    table_exists = True
    try:
        existing_table = client.get_table(table_id)
        print(f"  ✅ 表已存在: {table_name}")
        
        # 检查表是否有data_hash字段
        existing_fields = [field.name for field in existing_table.schema]
        has_hash_field = 'data_hash' in existing_fields
        
        if not has_hash_field:
            print(f"  ⚠️ 表缺少data_hash字段，将重建表结构")
            # 备份现有数据
            backup_table_id = f"{table_id}_backup_{int(datetime.now().timestamp())}"
            backup_query = f"CREATE TABLE `{backup_table_id}` AS SELECT * FROM `{table_id}`"
            client.query(backup_query).result()
            print(f"  💾 数据已备份到: {backup_table_id}")
            
            # 删除原表
            client.delete_table(table_id)
            table_exists = False
            
    except:
        table_exists = False
    
    if not table_exists:
        # 创建新表
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"  🆕 创建表: {table_name}")
    
    # 为每个租户处理数据
    total_new_rows = 0
    total_duplicate_rows = 0
    
    for db_name in db_names:
        print(f"\n  🏢 处理租户: {db_name}")
        
        try:
            # 获取该租户的数据
            rows = get_table_data(
                params['db_host'], params['db_port'],
                params['db_user'], params['db_pass'],
                db_name, table_name
            )
            
            if not rows:
                print(f"    ⚠️ 租户 {db_name} 无数据")
                continue
            
            # 获取现有数据哈希
            existing_hashes = get_existing_hashes(client, table_id, db_name)
            
            # 过滤重复数据
            new_rows = []
            duplicate_count = 0
            
            for row in rows:
                if row['data_hash'] not in existing_hashes:
                    new_rows.append(row)
                else:
                    duplicate_count += 1
            
            print(f"    📊 新数据: {len(new_rows)} 行, 重复数据: {duplicate_count} 行")
            
            if new_rows:
                # 追加新数据
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema=schema
                )
                
                job = client.load_table_from_json(new_rows, table_id, job_config=job_config)
                job.result()
                
                print(f"    ✅ 租户 {db_name} 新数据同步完成: {len(new_rows)} 行")
                total_new_rows += len(new_rows)
            else:
                print(f"    ⚠️ 租户 {db_name} 无新数据需要同步")
            
            total_duplicate_rows += duplicate_count
            
        except Exception as e:
            print(f"    ❌ 租户 {db_name} 同步失败: {str(e)}")
            continue
    
    print(f"\n  📈 表 {table_name} 同步统计:")
    print(f"    🆕 新增数据: {total_new_rows} 行")
    print(f"    🔄 重复数据: {total_duplicate_rows} 行")
    print(f"    ✅ 同步完成")

def main():
    print("🚀 MySQL 到 BigQuery 安全追加同步工具")
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
    print(f"🔧 同步模式: 安全追加（去重）")
    
    # 检测是否为多租户场景
    is_multitenant = len(db_names) > 1
    print(f"🏢 多租户模式: {'是' if is_multitenant else '否'}")
    
    # 按表分组同步
    success_count = 0
    for i, table_name in enumerate(table_names, 1):
        print(f"\n[{i}/{len(table_names)}] 处理表: {table_name}")
        try:
            sync_table_safe_append(params, table_name, db_names)
            success_count += 1
        except Exception as e:
            print(f"❌ 表 {table_name} 同步失败: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\n🎉 安全追加同步完成！成功处理了 {success_count}/{len(table_names)} 张表")
    
    # 显示验证查询
    print(f"\n📊 验证查询示例:")
    for table_name in table_names:
        print(f"""
-- 查看表 {table_name} 的租户数据统计
SELECT 
  tenant_id, 
  COUNT(*) as total_rows,
  COUNT(DISTINCT data_hash) as unique_rows,
  MAX(sync_timestamp) as last_sync_time
FROM `{params['bq_project']}.{params['bq_dataset']}.{table_name}`
GROUP BY tenant_id
ORDER BY tenant_id;
        """)

if __name__ == "__main__":
    main()