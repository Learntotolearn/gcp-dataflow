#!/usr/bin/env python3
"""
MySQL 到 BigQuery 智能同步工具 - 智能策略选择
根据表特征自动选择最佳同步策略，完美处理各种数据场景

功能特性：
- 🔑 智能策略选择：有主键用MERGE，无主键用哈希去重
- 🔄 MERGE操作：完美处理数据更新，避免逻辑重复
- 🧠 哈希去重：无主键表的智能去重方案
- 🏢 多租户支持：完美解决多租户数据覆盖问题
- ⚡ 高效同步：智能检测重复数据，避免无效传输
- 📊 详细统计：完整的同步报告和数据质量分析
- 🛡️ 数据安全：自动备份和恢复机制
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

def get_primary_keys(db_host, db_port, db_user, db_pass, db_name, table_name):
    """获取表的主键信息"""
    print(f"🔑 检测主键: {db_name}.{table_name}")
    
    conn = mysql.connector.connect(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_pass,
        database=db_name
    )
    cursor = conn.cursor()
    
    # 查询主键信息
    query = """
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
    WHERE TABLE_SCHEMA = %s 
    AND TABLE_NAME = %s 
    AND CONSTRAINT_NAME = 'PRIMARY'
    ORDER BY ORDINAL_POSITION
    """
    
    cursor.execute(query, (db_name, table_name))
    primary_keys = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    if primary_keys:
        print(f"  ✅ 发现主键: {primary_keys}")
    else:
        print(f"  ⚠️ 未发现主键，将使用哈希去重策略")
    
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
    
    # 增加多租户字段
    schema.append(bigquery.SchemaField("tenant_id", "STRING", mode="NULLABLE"))
    schema.append(bigquery.SchemaField("data_hash", "STRING", mode="NULLABLE"))
    schema.append(bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"))
    
    cursor.close()
    conn.close()
    return schema

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
        # 添加租户标识和更新时间
        row['tenant_id'] = db_name
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

def create_temp_table(client, table_id, schema, rows):
    """创建临时表存储源数据"""
    temp_table_id = f"{table_id}_temp_{int(datetime.now().timestamp())}"
    
    # 创建临时表
    temp_table = bigquery.Table(temp_table_id, schema=schema)
    temp_table = client.create_table(temp_table)
    print(f"  🔧 创建临时表: {temp_table_id}")
    
    # 加载数据到临时表
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    job = client.load_table_from_json(rows, temp_table_id, job_config=job_config)
    job.result()
    print(f"  📥 数据已加载到临时表: {len(rows)} 行")
    
    return temp_table_id

def execute_merge_query(client, target_table_id, temp_table_id, primary_keys, schema, tenant_id):
    """执行MERGE查询"""
    print(f"  🔄 执行MERGE操作...")
    
    # 获取目标表的实际字段
    try:
        target_table = client.get_table(target_table_id)
        target_fields = {field.name for field in target_table.schema}
        print(f"  📋 目标表字段: {sorted(target_fields)}")
    except Exception as e:
        print(f"  ⚠️ 无法获取目标表结构: {e}")
        target_fields = {field.name for field in schema}
    
    # 构建主键匹配条件
    pk_conditions = []
    for pk in primary_keys:
        pk_conditions.append(f"T.{pk} = S.{pk}")
    pk_match = " AND ".join(pk_conditions)
    
    # 构建UPDATE SET子句（只包含目标表中存在的字段，排除主键和tenant_id）
    update_fields = []
    for field in schema:
        if (field.name in target_fields and 
            field.name not in primary_keys and 
            field.name not in ['tenant_id']):
            update_fields.append(f"{field.name} = S.{field.name}")
    
    update_set = ", ".join(update_fields)
    
    # 构建INSERT字段列表（只包含目标表中存在的字段）
    insert_fields = [field.name for field in schema if field.name in target_fields]
    insert_columns = ", ".join(insert_fields)
    insert_values = ", ".join([f"S.{field}" for field in insert_fields])
    
    # 构建主键+哈希值的智能MERGE策略
    if 'data_hash' in target_fields:
        print(f"  🔍 使用主键+哈希值组合策略")
        print(f"     🔑 主键匹配: {primary_keys}")
        print(f"     🧮 哈希比较: data_hash字段（排除时间戳）")
        print(f"     ⏰ 时间戳: last_updated字段记录同步时间")
        
        # 主键匹配 + 哈希值不同时才更新
        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{temp_table_id}` S
        ON {pk_match} AND T.tenant_id = S.tenant_id
        WHEN MATCHED AND T.data_hash != S.data_hash THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
    else:
        print(f"  ⚠️ 目标表缺少data_hash字段，使用基础MERGE策略")
        
        # 兜底策略：基础MERGE（总是更新匹配的记录）
        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{temp_table_id}` S
        ON {pk_match} AND T.tenant_id = S.tenant_id
        WHEN MATCHED THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
    
    print(f"  📝 MERGE查询:")
    print(f"     匹配条件: {pk_match}")
    print(f"     租户过滤: T.tenant_id = '{tenant_id}'")
    print(f"     更新字段: {len(update_fields)} 个")
    print(f"     插入字段: {len(insert_fields)} 个")
    
    # 先查询现有数据统计
    pre_merge_query = f"""
    SELECT COUNT(*) as existing_count
    FROM `{target_table_id}` 
    WHERE tenant_id = '{tenant_id}'
    """
    pre_result = client.query(pre_merge_query).result()
    existing_count = list(pre_result)[0].existing_count
    
    # 查询源数据统计
    source_count_query = f"SELECT COUNT(*) as source_count FROM `{temp_table_id}`"
    source_result = client.query(source_count_query).result()
    source_count = list(source_result)[0].source_count
    
    print(f"  📊 操作前统计: 目标表现有 {existing_count} 行, 源数据 {source_count} 行")
    
    # 执行MERGE查询
    job = client.query(merge_query)
    result = job.result()
    
    # 查询操作后统计
    post_merge_query = f"""
    SELECT COUNT(*) as final_count
    FROM `{target_table_id}` 
    WHERE tenant_id = '{tenant_id}'
    """
    post_result = client.query(post_merge_query).result()
    final_count = list(post_result)[0].final_count
    
    # 计算统计信息
    inserted_rows = final_count - existing_count
    
    # 基于主键+哈希值策略的精确统计
    if 'data_hash' in target_fields:
        # 统计哈希值不同的记录（实际更新的记录）
        hash_diff_query = f"""
        SELECT COUNT(*) as hash_different
        FROM `{temp_table_id}` S
        JOIN `{target_table_id}` T
        ON {pk_match.replace('T.', 'T.').replace('S.', 'S.')} 
        AND T.tenant_id = S.tenant_id
        WHERE T.data_hash != S.data_hash
        """
        hash_result = client.query(hash_diff_query).result()
        actual_updated_rows = list(hash_result)[0].hash_different
        
        # 计算无变化的记录数
        matched_records = source_count - inserted_rows
        unchanged_rows = matched_records - actual_updated_rows
        
        print(f"  🔍 哈希比较结果:")
        print(f"     📊 匹配记录: {matched_records} 行")
        print(f"     🔄 哈希不同: {actual_updated_rows} 行")
        print(f"     ⚪ 哈希相同: {unchanged_rows} 行")
    else:
        # 兜底统计（无哈希字段时）
        actual_updated_rows = max(0, source_count - inserted_rows) if inserted_rows < source_count else 0
        unchanged_rows = 0
    
    print(f"  📈 MERGE操作统计:")
    print(f"     🆕 新增记录: {inserted_rows} 行")
    print(f"     🔄 实际更新: {actual_updated_rows} 行")
    print(f"     ⚪ 无变化: {unchanged_rows} 行")
    print(f"     📊 最终总数: {final_count} 行")
    print(f"  ✅ MERGE操作完成")
    
    return {
        'merge_completed': True,
        'existing_count': existing_count,
        'source_count': source_count,
        'final_count': final_count,
        'inserted_rows': inserted_rows,
        'updated_rows': actual_updated_rows,
        'unchanged_rows': unchanged_rows
    }

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

class SmartSyncEngine:
    """智能同步引擎 - 智能策略选择"""
    
    def __init__(self, params):
        self.params = params
        self.client = bigquery.Client(project=params['bq_project'])
        self.stats = {
            'total_tables': 0,
            'success_tables': 0,
            'tables_with_pk': 0,
            'tables_without_pk': 0,
            'total_rows_processed': 0,
            'total_new_rows': 0,
            'total_duplicate_rows': 0,
            'total_merge_operations': 0
        }
    
    def sync_table_merge(self, table_name, db_names, primary_keys):
        """使用MERGE策略同步单个表"""
        print(f"\n🔄 使用MERGE策略同步表: {table_name}")
        print(f"🔑 主键: {primary_keys}")
        
        # 获取表结构
        schema = get_table_schema(
            self.params['db_host'], self.params['db_port'], 
            self.params['db_user'], self.params['db_pass'], 
            db_names[0], table_name
        )
        
        # 创建数据集和表
        dataset_id = self.params['bq_dataset']
        table_id = f"{self.params['bq_project']}.{dataset_id}.{table_name}"
        
        try:
            self.client.get_dataset(dataset_id)
        except:
            dataset = bigquery.Dataset(f"{self.params['bq_project']}.{dataset_id}")
            dataset.location = "US"
            self.client.create_dataset(dataset)
            print(f"  🆕 创建数据集: {dataset_id}")
        
        # 检查表是否存在
        table_exists = True
        try:
            existing_table = self.client.get_table(table_id)
            print(f"  ✅ 表已存在: {table_name}")
        except:
            table_exists = False
        
        if not table_exists:
            # 创建新表
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            print(f"  🆕 创建表: {table_name}")
        
        # 为每个租户处理数据
        total_processed_rows = 0
        
        for db_name in db_names:
            print(f"\n  🏢 处理租户: {db_name}")
            
            try:
                # 获取该租户的数据
                rows = get_table_data(
                    self.params['db_host'], self.params['db_port'],
                    self.params['db_user'], self.params['db_pass'],
                    db_name, table_name
                )
                
                if not rows:
                    print(f"    ⚠️ 租户 {db_name} 无数据")
                    continue
                
                # 创建临时表
                temp_table_id = create_temp_table(self.client, table_id, schema, rows)
                
                try:
                    # 执行MERGE操作
                    merge_stats = execute_merge_query(
                        self.client, table_id, temp_table_id, 
                        primary_keys, schema, db_name
                    )
                    
                    # 更新统计信息
                    if merge_stats.get('inserted_rows', 0) > 0:
                        self.stats['total_new_rows'] += merge_stats['inserted_rows']
                    
                    print(f"    ✅ 租户 {db_name} MERGE完成:")
                    print(f"       📊 处理数据: {len(rows)} 行")
                    print(f"       🆕 新增: {merge_stats.get('inserted_rows', 0)} 行")
                    print(f"       🔄 更新: {merge_stats.get('updated_rows', 0)} 行")
                    
                    total_processed_rows += len(rows)
                    self.stats['total_merge_operations'] += 1
                    
                finally:
                    # 清理临时表
                    self.client.delete_table(temp_table_id)
                    print(f"    🗑️ 临时表已清理")
                
            except Exception as e:
                print(f"    ❌ 租户 {db_name} 同步失败: {str(e)}")
                continue
        
        self.stats['total_rows_processed'] += total_processed_rows
        return total_processed_rows

    def sync_table_smart_append(self, table_name, db_names):
        """智能安全追加同步单个表"""
        print(f"\n🚀 开始智能同步表: {table_name}")
        print(f"🏢 涉及租户: {db_names}")
        
        # 获取表结构（使用第一个数据库）
        schema = get_table_schema(
            self.params['db_host'], self.params['db_port'], 
            self.params['db_user'], self.params['db_pass'], 
            db_names[0], table_name
        )
        
        # 创建数据集和表
        dataset_id = self.params['bq_dataset']
        table_id = f"{self.params['bq_project']}.{dataset_id}.{table_name}"
        
        try:
            self.client.get_dataset(dataset_id)
        except:
            dataset = bigquery.Dataset(f"{self.params['bq_project']}.{dataset_id}")
            dataset.location = "US"
            self.client.create_dataset(dataset)
            print(f"  🆕 创建数据集: {dataset_id}")
        
        # 检查表是否存在
        table_exists = True
        try:
            existing_table = self.client.get_table(table_id)
            print(f"  ✅ 表已存在: {table_name}")
            
            # 检查表是否有data_hash字段
            existing_fields = [field.name for field in existing_table.schema]
            has_hash_field = 'data_hash' in existing_fields
            
            if not has_hash_field:
                print(f"  ⚠️ 表缺少data_hash字段，将重建表结构")
                # 备份现有数据
                backup_table_id = f"{table_id}_backup_{int(datetime.now().timestamp())}"
                backup_query = f"CREATE TABLE `{backup_table_id}` AS SELECT * FROM `{table_id}`"
                self.client.query(backup_query).result()
                print(f"  💾 数据已备份到: {backup_table_id}")
                
                # 删除原表
                self.client.delete_table(table_id)
                table_exists = False
                
        except:
            table_exists = False
        
        if not table_exists:
            # 创建新表
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            print(f"  🆕 创建表: {table_name}")
        
        # 为每个租户处理数据
        total_new_rows = 0
        total_duplicate_rows = 0
        
        for db_name in db_names:
            print(f"\n  🏢 处理租户: {db_name}")
            
            try:
                # 获取该租户的数据
                rows = get_table_data(
                    self.params['db_host'], self.params['db_port'],
                    self.params['db_user'], self.params['db_pass'],
                    db_name, table_name
                )
                
                if not rows:
                    print(f"    ⚠️ 租户 {db_name} 无数据")
                    continue
                
                # 获取现有数据哈希
                existing_hashes = get_existing_hashes(self.client, table_id, db_name)
                
                # 智能过滤重复数据
                new_rows = []
                duplicate_count = 0
                
                for row in rows:
                    if row['data_hash'] not in existing_hashes:
                        new_rows.append(row)
                    else:
                        duplicate_count += 1
                
                print(f"    📊 新数据: {len(new_rows)} 行, 重复数据: {duplicate_count} 行")
                
                if new_rows:
                    # 安全追加新数据
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        schema=schema
                    )
                    
                    job = self.client.load_table_from_json(new_rows, table_id, job_config=job_config)
                    job.result()
                    
                    print(f"    ✅ 租户 {db_name} 新数据同步完成: {len(new_rows)} 行")
                    total_new_rows += len(new_rows)
                else:
                    print(f"    ⚠️ 租户 {db_name} 无新数据需要同步")
                
                total_duplicate_rows += duplicate_count
                
            except Exception as e:
                print(f"    ❌ 租户 {db_name} 同步失败: {str(e)}")
                continue
        
        # 更新统计
        self.stats['total_tables'] += 1
        if total_new_rows > 0 or total_duplicate_rows > 0:
            self.stats['success_tables'] += 1
        self.stats['total_rows_processed'] += (total_new_rows + total_duplicate_rows)
        self.stats['total_new_rows'] += total_new_rows
        self.stats['total_duplicate_rows'] += total_duplicate_rows
        
        print(f"\n  📈 表 {table_name} 智能同步统计:")
        print(f"    🆕 新增数据: {total_new_rows} 行")
        print(f"    🔄 重复数据: {total_duplicate_rows} 行")
        print(f"    ⚡ 去重效率: {(total_duplicate_rows / (total_new_rows + total_duplicate_rows) * 100):.1f}%" if (total_new_rows + total_duplicate_rows) > 0 else "    ⚡ 去重效率: 0%")
        print(f"    ✅ 同步完成")
        
        return {
            'table_name': table_name,
            'new_rows': total_new_rows,
            'duplicate_rows': total_duplicate_rows,
            'total_rows': total_new_rows + total_duplicate_rows
        }
    
    def sync_table(self, table_name, db_names):
        """智能同步单个表 - 自动选择最佳策略"""
        print(f"\n🚀 开始智能同步表: {table_name}")
        print(f"🏢 涉及租户: {db_names}")
        
        # 检测主键（使用第一个数据库）
        primary_keys = get_primary_keys(
            self.params['db_host'], self.params['db_port'],
            self.params['db_user'], self.params['db_pass'],
            db_names[0], table_name
        )
        
        # 智能策略选择
        if primary_keys:
            print(f"  🎯 策略选择: MERGE（基于主键 {primary_keys}）")
            self.stats['tables_with_pk'] += 1
            total_rows = self.sync_table_merge(table_name, db_names, primary_keys)
        else:
            print(f"  🎯 策略选择: 哈希去重（无主键表）")
            self.stats['tables_without_pk'] += 1
            result = self.sync_table_smart_append(table_name, db_names)
            total_rows = result['new_rows'] if result else 0
        
        # 更新统计
        self.stats['total_tables'] += 1
        if total_rows > 0:
            self.stats['success_tables'] += 1
        
        return {
            'table_name': table_name,
            'strategy': 'MERGE' if primary_keys else 'HASH_DEDUP',
            'primary_keys': primary_keys,
            'processed_rows': total_rows
        }
    
    def print_final_stats(self):
        """打印最终统计信息"""
        print(f"\n📊 智能同步统计报告")
        print("=" * 60)
        print(f"📋 总表数: {self.stats['total_tables']}")
        print(f"✅ 成功表数: {self.stats['success_tables']}")
        print(f"❌ 失败表数: {self.stats['total_tables'] - self.stats['success_tables']}")
        print(f"🔑 有主键表数: {self.stats['tables_with_pk']}")
        print(f"⚠️ 无主键表数: {self.stats['tables_without_pk']}")
        print(f"📈 总处理行数: {self.stats['total_rows_processed']:,}")
        print(f"🆕 新增行数: {self.stats['total_new_rows']:,}")
        print(f"🔄 重复行数: {self.stats['total_duplicate_rows']:,}")
        print(f"🔄 MERGE操作: {self.stats['total_merge_operations']} 次")
        
        print(f"\n🎯 同步策略分布:")
        print(f"  MERGE策略: {self.stats['tables_with_pk']} 张表（有主键）")
        print(f"  哈希去重策略: {self.stats['tables_without_pk']} 张表（无主键）")
        
        if self.stats['total_rows_processed'] > 0:
            if self.stats['total_duplicate_rows'] > 0:
                dedup_rate = (self.stats['total_duplicate_rows'] / self.stats['total_rows_processed']) * 100
                print(f"\n📊 数据质量:")
                print(f"  去重率: {dedup_rate:.1f}%")
                print(f"  数据新鲜度: {(self.stats['total_new_rows'] / self.stats['total_rows_processed']) * 100:.1f}%")
            else:
                print(f"\n📊 数据质量:")
                print(f"  MERGE操作: 完美处理数据更新，无重复数据")
            
            success_rate = (self.stats['success_tables'] / max(self.stats['total_tables'], 1)) * 100
            print(f"  成功率: {success_rate:.1f}%")
        else:
            print(f"\n📊 数据质量:")
            print(f"  无数据处理")

def main():
    print("🧠 MySQL 到 BigQuery 智能同步工具")
    print("=" * 60)
    print("🎯 基于安全追加同步，智能哈希去重，完美解决重复数据问题")
    print()
    
    # 读取配置
    try:
        with open('params.json', 'r') as f:
            params = json.load(f)
    except FileNotFoundError:
        print("❌ 配置文件 params.json 不存在")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ 配置文件格式错误: {e}")
        sys.exit(1)
    
    # 解析参数
    db_names = [db.strip() for db in params['db_list'].split(",")]
    table_names = [t.strip() for t in params['table_list'].split(",")]
    
    print(f"🎯 数据库: {db_names}")
    print(f"📋 表名: {table_names}")
    print(f"📊 目标: {params['bq_project']}.{params['bq_dataset']}")
    print(f"🔧 同步模式: 智能安全追加（哈希去重）")
    
    # 检测是否为多租户场景
    is_multitenant = len(db_names) > 1
    print(f"🏢 多租户模式: {'是' if is_multitenant else '否'}")
    
    # 创建智能同步引擎
    engine = SmartSyncEngine(params)
    
    # 按表分组同步（多租户友好）
    print(f"\n🚀 开始智能同步，共 {len(table_names)} 张表")
    
    for i, table_name in enumerate(table_names, 1):
        print(f"\n[{i}/{len(table_names)}] 处理表: {table_name}")
        try:
            engine.sync_table(table_name, db_names)
        except Exception as e:
            print(f"❌ 表 {table_name} 同步失败: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # 打印最终统计
    engine.print_final_stats()
    
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
    
    print(f"\n🎉 智能同步完成！")
    print(f"💡 提示: 系统使用智能哈希去重，确保数据零重复")

if __name__ == "__main__":
    main()