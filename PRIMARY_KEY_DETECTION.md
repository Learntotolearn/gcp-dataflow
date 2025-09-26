# 🔑 MySQL主键检测完整指南

## 📋 核心问题

如何在代码中自动检测MySQL表是否有主键，以便选择正确的同步策略？

## 🔍 检测方法

### 方法1：查询INFORMATION_SCHEMA（推荐）

```python
def has_primary_key(db_host, db_port, db_user, db_pass, db_name, table_name):
    """检测表是否有主键"""
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
    
    return len(primary_keys) > 0, primary_keys

# 使用示例
has_pk, pk_columns = has_primary_key('localhost', 3306, 'root', 'password', 'shop1', 'users')
if has_pk:
    print(f"表有主键: {pk_columns}")
else:
    print("表没有主键")
```

### 方法2：使用SHOW KEYS命令

```python
def get_primary_keys_show_keys(conn, table_name):
    """使用SHOW KEYS检测主键"""
    cursor = conn.cursor()
    cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
    
    primary_keys = []
    for row in cursor.fetchall():
        # SHOW KEYS返回格式：(Table, Non_unique, Key_name, Seq_in_index, Column_name, ...)
        column_name = row[4]  # Column_name在第5列（索引4）
        seq_in_index = row[3]  # Seq_in_index在第4列（索引3）
        primary_keys.append((seq_in_index, column_name))
    
    # 按序号排序，返回列名列表
    primary_keys.sort(key=lambda x: x[0])
    pk_columns = [col[1] for col in primary_keys]
    
    cursor.close()
    return len(pk_columns) > 0, pk_columns
```

### 方法3：使用DESCRIBE命令

```python
def get_primary_keys_describe(conn, table_name):
    """使用DESCRIBE检测主键"""
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    
    primary_keys = []
    for row in cursor.fetchall():
        # DESCRIBE返回格式：(Field, Type, Null, Key, Default, Extra)
        field_name = row[0]
        key_type = row[3]
        
        if key_type == 'PRI':  # Primary Key
            primary_keys.append(field_name)
    
    cursor.close()
    return len(primary_keys) > 0, primary_keys
```

## 🎯 完整的表分析器

```python
class TableAnalyzer:
    """完整的表特征分析器"""
    
    def __init__(self, db_host, db_port, db_user, db_pass):
        self.db_host = db_host
        self.db_port = int(db_port)
        self.db_user = db_user
        self.db_pass = db_pass
    
    def analyze_table(self, db_name, table_name):
        """分析表的完整特征"""
        print(f"🔍 分析表特征: {db_name}.{table_name}")
        
        conn = mysql.connector.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            database=db_name
        )
        
        # 1. 检测主键
        has_pk, primary_keys = self._get_primary_keys(conn, db_name, table_name)
        
        # 2. 检测时间戳字段
        timestamp_fields = self._get_timestamp_fields(conn, db_name, table_name)
        
        # 3. 获取数据量
        row_count = self._get_row_count(conn, table_name)
        
        # 4. 检测表类型
        table_type = self._detect_table_type(conn, table_name, primary_keys, timestamp_fields)
        
        conn.close()
        
        analysis = {
            'db_name': db_name,
            'table_name': table_name,
            'has_primary_key': has_pk,
            'primary_keys': primary_keys,
            'timestamp_fields': timestamp_fields,
            'row_count': row_count,
            'table_type': table_type,
            'recommended_strategy': self._recommend_strategy(has_pk, timestamp_fields, table_type)
        }
        
        self._print_analysis(analysis)
        return analysis
    
    def _get_primary_keys(self, conn, db_name, table_name):
        """获取主键信息"""
        cursor = conn.cursor()
        
        # 使用INFORMATION_SCHEMA查询主键
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
        
        return len(primary_keys) > 0, primary_keys
    
    def _get_timestamp_fields(self, conn, db_name, table_name):
        """获取时间戳字段"""
        cursor = conn.cursor()
        
        # 查询时间相关字段
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = %s
        AND (
            DATA_TYPE IN ('timestamp', 'datetime', 'date', 'int', 'bigint')
            OR COLUMN_NAME REGEXP '(time|date|created|updated|modified)'
        )
        ORDER BY ORDINAL_POSITION
        """
        
        cursor.execute(query, (db_name, table_name))
        
        timestamp_fields = []
        for column_name, data_type, column_default, is_nullable in cursor.fetchall():
            # 判断是否为时间戳字段
            is_timestamp = (
                data_type in ['timestamp', 'datetime', 'date'] or
                any(keyword in column_name.lower() for keyword in 
                    ['time', 'date', 'created', 'updated', 'modified']) or
                (data_type in ['int', 'bigint'] and 'time' in column_name.lower())
            )
            
            if is_timestamp:
                timestamp_fields.append({
                    'name': column_name,
                    'type': data_type,
                    'default': column_default,
                    'nullable': is_nullable == 'YES'
                })
        
        cursor.close()
        return timestamp_fields
    
    def _get_row_count(self, conn, table_name):
        """获取表行数"""
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        cursor.close()
        return row_count
    
    def _detect_table_type(self, conn, table_name, primary_keys, timestamp_fields):
        """检测表类型"""
        # 基于表名和字段特征判断表类型
        table_name_lower = table_name.lower()
        
        if any(keyword in table_name_lower for keyword in ['log', 'event', 'history']):
            return 'append_only'  # 纯追加表
        elif any(keyword in table_name_lower for keyword in ['user', 'customer', 'product', 'order']):
            return 'business_data'  # 业务数据表
        elif any(keyword in table_name_lower for keyword in ['config', 'setting', 'dict']):
            return 'config_data'  # 配置数据表
        else:
            return 'unknown'
    
    def _recommend_strategy(self, has_primary_key, timestamp_fields, table_type):
        """推荐同步策略"""
        if has_primary_key:
            return 'merge'  # 有主键，使用MERGE
        elif timestamp_fields and len(timestamp_fields) > 0:
            # 检查是否有合适的更新时间字段
            update_fields = [f for f in timestamp_fields 
                           if any(keyword in f['name'].lower() 
                                 for keyword in ['update', 'modified'])]
            if update_fields:
                return 'incremental'  # 有更新时间，使用增量
        
        if table_type == 'append_only':
            return 'hash_dedup'  # 纯追加表，使用哈希去重
        else:
            return 'full_replace'  # 其他情况，使用全量替换
    
    def _print_analysis(self, analysis):
        """打印分析结果"""
        print(f"  📊 行数: {analysis['row_count']:,}")
        print(f"  🔑 主键: {analysis['primary_keys'] if analysis['has_primary_key'] else '无'}")
        
        if analysis['timestamp_fields']:
            ts_names = [f['name'] for f in analysis['timestamp_fields']]
            print(f"  🕐 时间戳字段: {ts_names}")
        else:
            print(f"  🕐 时间戳字段: 无")
        
        print(f"  📋 表类型: {analysis['table_type']}")
        print(f"  🎯 推荐策略: {analysis['recommended_strategy']}")
```

## 🎯 实际使用示例

```python
def main():
    # 创建表分析器
    analyzer = TableAnalyzer('localhost', 3306, 'root', 'password')
    
    # 分析不同类型的表
    tables = ['users', 'orders', 'products', 'user_logs']
    
    for table in tables:
        analysis = analyzer.analyze_table('shop1', table)
        
        # 根据分析结果选择策略
        if analysis['recommended_strategy'] == 'merge':
            print(f"  ✅ 使用MERGE策略，主键: {analysis['primary_keys']}")
        elif analysis['recommended_strategy'] == 'incremental':
            ts_field = analysis['timestamp_fields'][0]['name']
            print(f"  ✅ 使用增量同步，时间戳字段: {ts_field}")
        elif analysis['recommended_strategy'] == 'hash_dedup':
            print(f"  ✅ 使用哈希去重（纯追加表）")
        else:
            print(f"  ✅ 使用全量替换")
        
        print("-" * 50)
```

## 📊 输出示例

```bash
🔍 分析表特征: shop1.users
  📊 行数: 1,234
  🔑 主键: ['id']
  🕐 时间戳字段: ['created_at', 'updated_at']
  📋 表类型: business_data
  🎯 推荐策略: merge
  ✅ 使用MERGE策略，主键: ['id']

🔍 分析表特征: shop1.user_logs
  📊 行数: 50,000
  🔑 主键: 无
  🕐 时间戳字段: ['log_time']
  📋 表类型: append_only
  🎯 推荐策略: hash_dedup
  ✅ 使用哈希去重（纯追加表）

🔍 分析表特征: shop1.orders
  📊 行数: 5,678
  🔑 主键: ['order_id']
  🕐 时间戳字段: ['created_at', 'updated_at']
  📋 表类型: business_data
  🎯 推荐策略: merge
  ✅ 使用MERGE策略，主键: ['order_id']
```

## 🔧 集成到智能同步工具

```python
class SmartSyncEngine:
    def __init__(self, params):
        self.params = params
        self.analyzer = TableAnalyzer(
            params['db_host'], params['db_port'],
            params['db_user'], params['db_pass']
        )
        self.client = bigquery.Client(project=params['bq_project'])
    
    def sync_table(self, table_name, db_names):
        """智能同步单个表"""
        print(f"\n🚀 开始智能同步表: {table_name}")
        
        # 分析表特征（使用第一个数据库）
        analysis = self.analyzer.analyze_table(db_names[0], table_name)
        strategy = analysis['recommended_strategy']
        
        # 根据策略执行同步
        if strategy == 'merge':
            return self.sync_with_merge(analysis, db_names)
        elif strategy == 'incremental':
            return self.sync_with_incremental(analysis, db_names)
        elif strategy == 'hash_dedup':
            return self.sync_with_hash_dedup(analysis, db_names)
        else:
            return self.sync_with_full_replace(analysis, db_names)
```

## 🎊 总结

检测MySQL表是否有主键的方法：

### 🔍 **检测方法**
1. **INFORMATION_SCHEMA查询**（推荐）- 最标准的方法
2. **SHOW KEYS命令** - 简单直接
3. **DESCRIBE命令** - 最基础的方法

### 🎯 **智能策略选择**
- **有主键** → MERGE策略（正确处理UPDATE）
- **有时间戳字段** → 增量同步（高效）
- **纯追加表** → 哈希去重（安全）
- **其他情况** → 全量替换（可靠）

这样我们就能根据表的实际特征，自动选择最合适的同步策略，避免哈希去重导致的重复数据问题！🎯