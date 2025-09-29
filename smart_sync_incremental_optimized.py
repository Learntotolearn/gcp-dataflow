#!/usr/bin/env python3
"""
智能增量同步工具 - 性能优化版
集成表结构缓存、连接池复用、批量数据处理等性能优化
"""

import mysql.connector
import mysql.connector.pooling
from google.cloud import bigquery
import json
import sys
from datetime import datetime, timedelta
from decimal import Decimal
import time
import logging
from typing import Dict, List, Optional, Tuple
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_incremental.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# 常见时间戳字段名（按优先级排序）
TIMESTAMP_FIELDS = [
    'updated_at', 'update_time', 'last_updated', 'modified_at', 'last_modified',
    'created_at', 'create_time', 'insert_time', 'timestamp', 'sync_time'
]

class TableInfoCache:
    """表信息缓存类"""
    
    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()
    
    def get_table_info(self, db_name: str, table_name: str) -> Optional[Dict]:
        """获取缓存的表信息"""
        key = f"{db_name}.{table_name}"
        with self._lock:
            return self._cache.get(key)
    
    def set_table_info(self, db_name: str, table_name: str, info: Dict):
        """设置表信息缓存"""
        key = f"{db_name}.{table_name}"
        with self._lock:
            self._cache[key] = info
            logger.info(f"  💾 缓存表信息: {key}")
    
    def clear(self):
        """清空缓存"""
        with self._lock:
            self._cache.clear()

class LocalFileStatusManager:
    """本地文件状态管理器 - 按数据库分组"""
    
    def __init__(self, status_dir: str = "sync_status"):
        self.status_dir = Path(status_dir)
        self.status_dir.mkdir(exist_ok=True)
        self._lock = threading.Lock()
        logger.info(f"✅ 本地状态目录已准备就绪: {self.status_dir}")
    
    def _get_status_file(self, tenant_id: str) -> Path:
        """获取数据库状态文件路径"""
        return self.status_dir / f"{tenant_id}.json"
    
    def _load_database_status(self, tenant_id: str) -> Dict:
        """加载数据库的所有表状态"""
        status_file = self._get_status_file(tenant_id)
        
        if not status_file.exists():
            return {}
            
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ 读取状态文件失败 {status_file}: {e}")
            return {}
    
    def _save_database_status(self, tenant_id: str, status_data: Dict):
        """保存数据库的所有表状态"""
        status_file = self._get_status_file(tenant_id)
        
        try:
            with open(status_file, 'w', encoding='utf-8') as f:
                json.dump(status_data, f, indent=2, ensure_ascii=False)
            logger.info(f"  💾 更新数据库状态文件: {status_file.name}")
        except Exception as e:
            logger.error(f"❌ 写入状态文件失败 {status_file}: {e}")
    
    def get_last_sync_time(self, tenant_id: str, table_name: str) -> Optional[datetime]:
        """获取上次同步时间"""
        with self._lock:
            db_status = self._load_database_status(tenant_id)
            table_status = db_status.get('tables', {}).get(table_name, {})
            
            if table_status.get('last_sync_time'):
                try:
                    return datetime.fromisoformat(table_status['last_sync_time'])
                except Exception as e:
                    logger.warning(f"⚠️ 解析同步时间失败 {tenant_id}.{table_name}: {e}")
                    
        return None
    
    def update_sync_status(self, tenant_id: str, table_name: str,
                          sync_time: datetime, sync_mode: str, 
                          records_synced: int, status: str = 'SUCCESS', 
                          error_message: str = None):
        """更新同步状态"""
        with self._lock:
            # 加载现有状态
            db_status = self._load_database_status(tenant_id)
            
            # 初始化结构
            if 'database_info' not in db_status:
                db_status['database_info'] = {
                    'tenant_id': tenant_id,
                    'last_updated': datetime.now().isoformat()
                }
            
            if 'tables' not in db_status:
                db_status['tables'] = {}
            
            # 更新表状态
            db_status['tables'][table_name] = {
                'table_name': table_name,
                'last_sync_time': sync_time.isoformat(),
                'sync_status': status,
                'sync_mode': sync_mode,
                'records_synced': records_synced,
                'error_message': error_message,
                'updated_at': datetime.now().isoformat()
            }
            
            # 更新数据库级别信息
            db_status['database_info']['last_updated'] = datetime.now().isoformat()
            db_status['database_info']['total_tables'] = len(db_status['tables'])
            
            # 保存状态
            self._save_database_status(tenant_id, db_status)
    
    def get_database_summary(self, tenant_id: str) -> Dict:
        """获取数据库同步摘要"""
        with self._lock:
            db_status = self._load_database_status(tenant_id)
            
            if not db_status:
                return {'tenant_id': tenant_id, 'total_tables': 0, 'tables': {}}
            
            return {
                'tenant_id': tenant_id,
                'database_info': db_status.get('database_info', {}),
                'total_tables': len(db_status.get('tables', {})),
                'tables': db_status.get('tables', {}),
                'last_updated': db_status.get('database_info', {}).get('last_updated')
            }

class TableAnalyzer:
    """表结构分析器 - 优化版"""
    
    def __init__(self, connection_pool, cache: TableInfoCache):
        self.connection_pool = connection_pool
        self.cache = cache
    
    def get_table_info(self, db_name: str, table_name: str) -> Dict:
        """获取表的完整信息（使用缓存）"""
        # 检查缓存
        cached_info = self.cache.get_table_info(db_name, table_name)
        if cached_info:
            logger.info(f"  💾 使用缓存表信息: {db_name}.{table_name}")
            return cached_info
        
        # 缓存未命中，查询数据库
        logger.info(f"  🔍 分析表结构: {db_name}.{table_name}")
        
        conn = self.connection_pool.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE {db_name}")
            
            # 一次性获取所有表信息
            table_info = {
                'schema': [],
                'field_types': {},
                'timestamp_field': None,
                'primary_keys': []
            }
            
            # 1. 获取字段信息
            cursor.execute(f"DESCRIBE {table_name}")
            describe_results = cursor.fetchall()
            
            for field, ftype, *_ in describe_results:
                base_type = ftype.split("(")[0].lower()
                bq_type = MYSQL_TO_BQ_TYPE.get(base_type, "STRING")
                table_info['schema'].append(bigquery.SchemaField(field, bq_type, mode="NULLABLE"))
                table_info['field_types'][field] = ftype.lower()
            
            # 添加系统字段
            table_info['schema'].extend([
                bigquery.SchemaField("tenant_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("sync_timestamp", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("sync_mode", "STRING", mode="NULLABLE")
            ])
            
            # 2. 检测时间戳字段
            available_timestamp_fields = []
            for field, ftype in table_info['field_types'].items():
                field_lower = field.lower()
                if any(ts_field in field_lower for ts_field in ['time', 'date', 'created', 'updated', 'modified']):
                    if (any(ftype.startswith(dt) for dt in ['datetime', 'timestamp']) or
                        ('int' in ftype and any(kw in field_lower for kw in ['time', 'created', 'updated']))):
                        available_timestamp_fields.append((field, ftype))
            
            # 按优先级选择时间戳字段
            for preferred_field in TIMESTAMP_FIELDS:
                for available_field, field_type in available_timestamp_fields:
                    if preferred_field.lower() == available_field.lower():
                        table_info['timestamp_field'] = available_field
                        break
                if table_info['timestamp_field']:
                    break
            
            if not table_info['timestamp_field'] and available_timestamp_fields:
                table_info['timestamp_field'] = available_timestamp_fields[0][0]
            
            # 3. 获取主键信息
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}' 
                AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """)
            table_info['primary_keys'] = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            
            # 缓存结果
            self.cache.set_table_info(db_name, table_name, table_info)
            
            logger.info(f"  📊 表分析完成:")
            logger.info(f"    🕐 时间戳字段: {table_info['timestamp_field'] or '无'}")
            logger.info(f"    🔑 主键字段: {table_info['primary_keys'] or '无'}")
            logger.info(f"    📋 字段数量: {len(table_info['field_types'])}")
            
            return table_info
            
        finally:
            conn.close()

class BatchDataProcessor:
    """批量数据处理器"""
    
    @staticmethod
    def batch_normalize_data_types(rows: List[Dict], field_types: Dict[str, str]) -> List[Dict]:
        """批量标准化数据类型"""
        if not rows:
            return rows
        
        logger.info(f"  🔄 批量处理数据类型标准化: {len(rows)} 行")
        
        # 预计算类型转换映射
        type_converters = {}
        for field, mysql_type in field_types.items():
            base_type = mysql_type.split("(")[0].lower()
            bq_type = MYSQL_TO_BQ_TYPE.get(base_type, "STRING")
            type_converters[field] = (bq_type, mysql_type)
        
        # 批量处理
        normalized_rows = []
        conversion_stats = {}
        
        for row in rows:
            normalized_row = {}
            for key, value in row.items():
                if key in ['tenant_id', 'sync_timestamp', 'sync_mode']:
                    # 系统字段保持不变
                    normalized_row[key] = value
                elif value is None:
                    normalized_row[key] = None
                elif key in type_converters:
                    # 使用预计算的转换器
                    bq_type, mysql_type = type_converters[key]
                    old_value = value
                    normalized_row[key] = BatchDataProcessor._convert_value_to_bq_type(value, bq_type, mysql_type)
                    
                    # 统计转换
                    if key not in conversion_stats:
                        conversion_stats[key] = 0
                    if str(old_value) != str(normalized_row[key]):
                        conversion_stats[key] += 1
                else:
                    # 未知字段转为字符串
                    normalized_row[key] = str(value) if value is not None else None
            
            normalized_rows.append(normalized_row)
        
        # 显示转换统计
        for field, count in conversion_stats.items():
            if count > 0:
                bq_type, mysql_type = type_converters.get(field, ('STRING', 'unknown'))
                logger.info(f"    🔄 {field}: {mysql_type} → {bq_type} ({count} 次转换)")
        
        logger.info(f"  ✅ 批量数据类型标准化完成")
        return normalized_rows
    
    @staticmethod
    def _convert_value_to_bq_type(value, bq_type: str, mysql_type: str):
        """将值转换为BigQuery兼容的类型"""
        if value is None:
            return None
        
        try:
            if bq_type == "STRING":
                return str(value)
            elif bq_type == "INT64":
                return int(value) if value != '' else None
            elif bq_type == "FLOAT64":
                return float(value) if value != '' else None
            elif bq_type == "NUMERIC":
                return float(value) if value != '' else None
            elif bq_type == "BOOLEAN":
                if isinstance(value, bool):
                    return value
                elif isinstance(value, (int, str)):
                    return bool(int(value)) if str(value).isdigit() else bool(value)
                else:
                    return bool(value)
            elif bq_type == "TIMESTAMP":
                if isinstance(value, datetime):
                    return value.isoformat()
                elif isinstance(value, int) and 'time' in mysql_type:
                    # Unix时间戳转换
                    return datetime.fromtimestamp(value).isoformat()
                else:
                    return str(value)
            elif bq_type == "DATE":
                if isinstance(value, datetime):
                    return value.date().isoformat()
                else:
                    return str(value)
            else:
                return str(value)
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ 类型转换失败 {value} -> {bq_type}: {e}, 使用字符串类型")
            return str(value)
    


class OptimizedIncrementalSyncer:
    """优化版增量同步器"""
    
    def __init__(self, params: Dict):
        self.params = params
        
        # 解析数据库列表
        db_names = [db.strip() for db in params['db_list'].split(",")]
        
        # 创建连接池（不指定默认数据库）
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="sync_pool",
            pool_size=params.get('pool_size', 5),
            pool_reset_session=True,
            host=params['db_host'],
            port=int(params['db_port']),
            user=params['db_user'],
            password=params['db_pass']
        )
        logger.info(f"✅ 创建连接池: {params.get('pool_size', 5)} 个连接")
        
        # 初始化缓存和组件
        self.table_cache = TableInfoCache()
        self.status_manager = LocalFileStatusManager(params.get('status_dir', 'sync_status'))
        self.table_analyzer = TableAnalyzer(self.connection_pool, self.table_cache)
        self.bq_client = bigquery.Client(project=params['bq_project'])
        
        # 配置参数
        self.lookback_minutes = params.get('lookback_minutes', 10)
        self.batch_size = params.get('batch_size', 1000)
        self.max_retries = params.get('max_retries', 3)
        self.retry_delay = params.get('retry_delay', 5)
    
    def get_table_data(self, db_name: str, table_name: str, table_info: Dict, 
                      sync_mode: str, last_sync_time: datetime = None, 
                      current_sync_time: datetime = None) -> List[Dict]:
        """获取表数据（增量或全量）"""
        conn = self.connection_pool.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"USE {db_name}")
            
            if sync_mode == 'INCREMENTAL' and last_sync_time and table_info['timestamp_field']:
                # 增量查询
                timestamp_field = table_info['timestamp_field']
                timestamp_field_type = table_info['field_types'].get(timestamp_field, '').lower()
                
                # 安全回退时间窗口
                safe_start_time = last_sync_time - timedelta(minutes=self.lookback_minutes)
                
                if 'int' in timestamp_field_type:
                    # Unix时间戳查询
                    safe_start_timestamp = int(safe_start_time.timestamp())
                    current_timestamp = int(current_sync_time.timestamp())
                    
                    query = f"""
                        SELECT * FROM {table_name} 
                        WHERE {timestamp_field} > %s 
                        AND {timestamp_field} <= %s
                        ORDER BY {timestamp_field} ASC
                    """
                    cursor.execute(query, (safe_start_timestamp, current_timestamp))
                    logger.info(f"  🔍 Unix时间戳查询: {timestamp_field} > {safe_start_timestamp} AND <= {current_timestamp}")
                else:
                    # 日期时间查询
                    query = f"""
                        SELECT * FROM {table_name} 
                        WHERE {timestamp_field} > %s 
                        AND {timestamp_field} <= %s
                        ORDER BY {timestamp_field} ASC
                    """
                    cursor.execute(query, (safe_start_time, current_sync_time))
                    logger.info(f"  🔍 日期时间查询: {timestamp_field} > {safe_start_time} AND <= {current_sync_time}")
            else:
                # 全量查询
                cursor.execute(f"SELECT * FROM {table_name}")
                logger.info(f"  🔍 全量数据查询")
            
            rows = cursor.fetchall()
            cursor.close()
            
            if not rows:
                logger.info(f"  ℹ️ 无数据返回")
                return []
            
            logger.info(f"  📥 获取数据: {len(rows)} 行")
            
            # 批量添加系统字段
            for row in rows:
                row['tenant_id'] = db_name
                row['sync_timestamp'] = current_sync_time.isoformat()
                row['sync_mode'] = sync_mode
                
                # 基础类型处理
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
                    elif isinstance(value, Decimal):
                        row[key] = float(value)
            
            # 批量数据处理
            rows = BatchDataProcessor.batch_normalize_data_types(rows, table_info['field_types'])
            
            return rows
            
        finally:
            conn.close()
    
    def ensure_bq_table(self, table_name: str, schema: List[bigquery.SchemaField]):
        """确保BigQuery表存在"""
        dataset_id = self.params['bq_dataset']
        
        # 创建数据集（如果不存在）
        try:
            self.bq_client.get_dataset(dataset_id)
        except:
            dataset = bigquery.Dataset(f"{self.params['bq_project']}.{dataset_id}")
            dataset.location = "US"
            self.bq_client.create_dataset(dataset)
            logger.info(f"🆕 创建数据集: {dataset_id}")
        
        # 创建表（如果不存在）- 所有租户共享同一个表
        table_id = f"{self.params['bq_project']}.{dataset_id}.{table_name}"
        try:
            self.bq_client.get_table(table_id)
        except:
            table = bigquery.Table(table_id, schema=schema)
            # 设置分区和聚簇
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="sync_timestamp"
            )
            table.clustering_fields = ["tenant_id"]
            table = self.bq_client.create_table(table)
            logger.info(f"🆕 创建表: {table_name} (多租户共享)")
    
    def write_to_bigquery(self, table_name: str, rows: List[Dict], 
                         schema: List[bigquery.SchemaField], 
                         primary_keys: List[str], sync_mode: str):
        """写入BigQuery"""
        if not rows:
            return
        
        table_id = f"{self.params['bq_project']}.{self.params['bq_dataset']}.{table_name}"
        
        if sync_mode == 'FULL':
            # 全量同步：先删除该租户的数据，再插入新数据
            tenant_id = rows[0]['tenant_id'] if rows else None
            if tenant_id:
                # 删除该租户的现有数据
                delete_sql = f"""
                DELETE FROM `{table_id}` 
                WHERE tenant_id = '{tenant_id}'
                """
                delete_job = self.bq_client.query(delete_sql)
                delete_job.result()
                logger.info(f"🗑️ 已删除租户 {tenant_id} 的现有数据")
            
            # 插入新数据
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema
            )
            job = self.bq_client.load_table_from_json(rows, table_id, job_config=job_config)
            job.result()
            logger.info(f"✅ 全量写入完成: {len(rows)} 行 (租户: {tenant_id})")
            
        else:
            # 增量同步：优先使用MERGE操作确保数据一致性
            if primary_keys:
                # 有主键：使用MERGE操作（支持插入和更新）
                self._merge_data(table_id, rows, primary_keys, schema)
                logger.info(f"✅ MERGE操作完成: {len(rows)} 行")
            else:
                # 无主键：使用APPEND模式（仅追加）
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema=schema
                )
                job = self.bq_client.load_table_from_json(rows, table_id, job_config=job_config)
                job.result()
                logger.info(f"✅ 增量追加完成: {len(rows)} 行（无主键，仅追加）")
    
    def _merge_data(self, table_id: str, rows: List[Dict], primary_keys: List[str], schema: List[bigquery.SchemaField]):
        """使用MERGE操作更新数据"""
        # 创建临时表
        temp_table_id = f"{table_id}_temp_{int(time.time())}"
        
        # 上传数据到临时表，使用与目标表相同的schema
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema
        )
        job = self.bq_client.load_table_from_json(rows, temp_table_id, job_config=job_config)
        job.result()
        
        # 构建MERGE SQL
        pk_conditions = " AND ".join([f"T.{pk} = S.{pk}" for pk in primary_keys])
        pk_conditions += " AND T.tenant_id = S.tenant_id"
        
        # 获取所有字段（除了主键和系统字段）
        sample_row = rows[0]
        update_fields = []
        insert_fields = []
        insert_values = []
        
        for field in sample_row.keys():
            # 所有字段都参与INSERT
            insert_fields.append(field)
            insert_values.append(f"S.{field}")
            
            # UPDATE时排除主键字段（主键不能被更新）
            if field not in primary_keys:
                update_fields.append(f"{field} = S.{field}")
        
        merge_sql = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON {pk_conditions}
        WHEN MATCHED THEN
          UPDATE SET {', '.join(update_fields)}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(insert_fields)})
          VALUES ({', '.join(insert_values)})
        """
        
        # 执行MERGE
        query_job = self.bq_client.query(merge_sql)
        query_job.result()
        
        # 删除临时表
        self.bq_client.delete_table(temp_table_id)
        
        logger.info(f"✅ MERGE操作完成: {len(rows)} 行")
    
    def sync_table(self, db_name: str, table_name: str, force_full: bool = False) -> Dict:
        """同步单个表"""
        logger.info(f"\n🚀 开始同步表: {db_name}.{table_name}")
        
        current_sync_time = datetime.now()
        sync_stats = {
            'tenant_id': db_name,
            'table_name': table_name,
            'sync_mode': 'FULL',
            'records_synced': 0,
            'status': 'SUCCESS',
            'error_message': None,
            'start_time': current_sync_time
        }
        
        try:
            # 获取表信息（使用缓存）
            table_info = self.table_analyzer.get_table_info(db_name, table_name)
            
            # 确保BigQuery表存在
            self.ensure_bq_table(table_name, table_info['schema'])
            
            # 决定同步模式
            last_sync_time = None if force_full else self.status_manager.get_last_sync_time(db_name, table_name)
            
            if last_sync_time and table_info['timestamp_field'] and not force_full:
                # 增量同步
                sync_stats['sync_mode'] = 'INCREMENTAL'
                logger.info(f"🔄 执行增量同步，上次同步时间: {last_sync_time}")
                
                rows = self.get_table_data(
                    db_name, table_name, table_info, 'INCREMENTAL',
                    last_sync_time, current_sync_time
                )
            else:
                # 全量同步
                sync_stats['sync_mode'] = 'FULL'
                reason = "强制全量" if force_full else ("首次同步" if not last_sync_time else "无时间戳字段")
                logger.info(f"🔄 执行全量同步，原因: {reason}")
                
                rows = self.get_table_data(
                    db_name, table_name, table_info, 'FULL',
                    current_sync_time=current_sync_time
                )
            
            # 写入BigQuery
            if rows:
                self.write_to_bigquery(
                    table_name, rows, table_info['schema'], 
                    table_info['primary_keys'], sync_stats['sync_mode']
                )
                sync_stats['records_synced'] = len(rows)
                logger.info(f"✅ 同步完成: {len(rows)} 行数据")
            else:
                logger.info("ℹ️ 无新数据需要同步")
            
            # 更新同步状态
            self.status_manager.update_sync_status(
                db_name, table_name, current_sync_time, 
                sync_stats['sync_mode'], sync_stats['records_synced']
            )
            
        except Exception as e:
            sync_stats['status'] = 'FAILED'
            sync_stats['error_message'] = str(e)
            logger.error(f"❌ 同步失败: {str(e)}")
            logger.error(traceback.format_exc())
            
            # 更新失败状态
            self.status_manager.update_sync_status(
                db_name, table_name, current_sync_time, 
                sync_stats['sync_mode'], 0, 'FAILED', str(e)
            )
        
        sync_stats['end_time'] = datetime.now()
        sync_stats['duration'] = (sync_stats['end_time'] - sync_stats['start_time']).total_seconds()
        
        return sync_stats
    
    def sync_database_parallel(self, db_name: str, table_names: List[str], force_full: bool = False) -> List[Dict]:
        """并行同步单个数据库的所有表"""
        logger.info(f"📂 并行处理数据库: {db_name} ({len(table_names)} 张表)")
        
        database_stats = []
        max_workers = min(len(table_names), 3)  # 最多3个并发线程
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有表的同步任务
            future_to_table = {
                executor.submit(self.sync_table_safe, db_name, table_name, force_full): table_name
                for table_name in table_names
            }
            
            # 收集结果
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    table_stats = future.result()
                    table_stats['database'] = db_name
                    table_stats['table'] = table_name
                    table_stats['status'] = 'SUCCESS'
                    database_stats.append(table_stats)
                    logger.info(f"✅ 表同步完成: {db_name}.{table_name}")
                    
                except Exception as e:
                    logger.error(f"❌ 表同步失败: {db_name}.{table_name}, 错误: {e}")
                    database_stats.append({
                        'database': db_name,
                        'table': table_name,
                        'status': 'FAILED',
                        'error_message': str(e),
                        'records_synced': 0,
                        'duration': 0,
                        'sync_mode': 'UNKNOWN'
                    })
        
        return database_stats
    
    def sync_table_safe(self, db_name: str, table_name: str, force_full: bool = False) -> Dict:
        """线程安全的表同步方法"""
        thread_id = threading.current_thread().ident
        logger.info(f"🚀 [线程{thread_id}] 开始同步表: {db_name}.{table_name}")
        
        try:
            return self.sync_table(db_name, table_name, force_full)
        except Exception as e:
            logger.error(f"❌ [线程{thread_id}] 表同步异常: {db_name}.{table_name}, 错误: {e}")
            raise
    
    def sync_all_tables(self, force_full: bool = False) -> Dict:
        """安全并行同步所有表"""
        logger.info("🚀 开始并行智能增量同步 - 性能优化版")
        logger.info("=" * 60)
        
        # 解析配置
        db_names = [db.strip() for db in self.params['db_list'].split(",")]
        table_names = [t.strip() for t in self.params['table_list'].split(",")]
        
        logger.info(f"🎯 数据库: {db_names}")
        logger.info(f"📋 表名: {table_names}")
        logger.info(f"📊 目标: {self.params['bq_project']}.{self.params['bq_dataset']}")
        logger.info(f"🔧 同步模式: {'强制全量' if force_full else '智能增量'}")
        logger.info(f"⚡ 性能优化: 连接池({self.params.get('pool_size', 5)}) + 表结构缓存 + 批量处理 + 并行同步")
        
        # 同步统计
        total_stats = {
            'total_tables': len(db_names) * len(table_names),
            'success_count': 0,
            'failed_count': 0,
            'full_sync_count': 0,
            'incremental_sync_count': 0,
            'total_records': 0,
            'start_time': datetime.now(),
            'table_stats': []
        }
        
        # 数据库级串行处理，表级并行处理（安全方案）
        for db_name in db_names:
            logger.info(f"📂 开始处理数据库: {db_name}")
            db_start_time = datetime.now()
            
            # 并行处理当前数据库的所有表
            database_stats = self.sync_database_parallel(db_name, table_names, force_full)
            
            # 汇总数据库统计
            for table_stat in database_stats:
                total_stats['table_stats'].append(table_stat)
                
                if table_stat['status'] == 'SUCCESS':
                    total_stats['success_count'] += 1
                    total_stats['total_records'] += table_stat.get('records_synced', 0)
                    
                    if table_stat.get('sync_mode') == 'FULL':
                        total_stats['full_sync_count'] += 1
                    else:
                        total_stats['incremental_sync_count'] += 1
                else:
                    total_stats['failed_count'] += 1
            
            db_duration = (datetime.now() - db_start_time).total_seconds()
            db_records = sum(stat.get('records_synced', 0) for stat in database_stats if stat['status'] == 'SUCCESS')
            logger.info(f"✅ 数据库处理完成: {db_name} ({db_records} 行, {db_duration:.1f}秒)")
        
        total_stats['end_time'] = datetime.now()
        total_stats['total_duration'] = (total_stats['end_time'] - total_stats['start_time']).total_seconds()
        
        # 打印统计报告
        self._print_sync_report(total_stats)
        
        return total_stats
    
    def _print_sync_report(self, stats: Dict):
        """打印同步报告"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 智能增量同步统计报告 - 性能优化版")
        logger.info("=" * 60)
        logger.info(f"📋 总表数: {stats['total_tables']}")
        logger.info(f"✅ 成功表数: {stats['success_count']}")
        logger.info(f"❌ 失败表数: {stats['failed_count']}")
        logger.info(f"📈 总处理行数: {stats['total_records']:,}")
        logger.info(f"⏱️ 总耗时: {stats['total_duration']:.2f} 秒")
        
        if stats['total_records'] > 0 and stats['total_duration'] > 0:
            throughput = stats['total_records'] / stats['total_duration']
            logger.info(f"🚀 处理速度: {throughput:.1f} 行/秒")
        
        logger.info(f"\n🎯 同步模式统计:")
        logger.info(f"  🔄 全量同步: {stats['full_sync_count']} 张表")
        logger.info(f"  ⚡ 增量同步: {stats['incremental_sync_count']} 张表")
        
        logger.info(f"\n⚡ 性能优化效果:")
        logger.info(f"  💾 表结构缓存命中: {len(self.table_cache._cache)} 张表")
        logger.info(f"  🔗 连接池复用: 减少连接建立开销")
        logger.info(f"  📦 批量数据处理: 提升处理效率")
        logger.info(f"  🚀 并行同步: 数据库串行 + 表级并行（安全模式）")
        
        if stats['failed_count'] > 0:
            logger.info(f"\n❌ 失败表详情:")
            for table_stat in stats['table_stats']:
                if table_stat['status'] == 'FAILED':
                    logger.info(f"  📋 {table_stat['tenant_id']}.{table_stat['table_name']}: {table_stat['error_message']}")
        
        logger.info("\n🎉 同步完成！")
    
    def cleanup(self):
        """清理资源"""
        try:
            self.table_cache.clear()
            # 连接池会自动管理连接
            logger.info("✅ 资源清理完成")
        except Exception as e:
            logger.warning(f"⚠️ 资源清理警告: {e}")

def main():
    """主函数"""
    if len(sys.argv) > 1 and sys.argv[1] == '--full':
        force_full = True
        print("🔄 强制全量同步模式")
    else:
        force_full = False
        print("⚡ 智能增量同步模式")
    
    # 读取配置
    try:
        with open('params.json', 'r') as f:
            params = json.load(f)
    except FileNotFoundError:
        logger.error("❌ 配置文件 params.json 不存在")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error("❌ 配置文件 params.json 格式错误")
        sys.exit(1)
    
    # 创建优化版同步器并执行同步
    syncer = OptimizedIncrementalSyncer(params)
    
    try:
        stats = syncer.sync_all_tables(force_full=force_full)
        
        # 根据结果设置退出码
        if stats['failed_count'] > 0:
            sys.exit(1)
        else:
            sys.exit(0)
    finally:
        syncer.cleanup()

if __name__ == "__main__":
    main()