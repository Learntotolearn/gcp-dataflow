# 🔄 增量同步SQL查询详解

## 📋 增量同步原理

增量同步通过**时间戳字段**来识别数据变更，只同步在上次同步时间之后发生变化的记录。

---

## 🕐 时间戳字段检测

### 自动检测的字段名（按优先级）
```python
TIMESTAMP_FIELDS = [
    'updated_at',     # 最常用的更新时间字段
    'update_time',    # 更新时间
    'last_updated',   # 最后更新时间
    'created_at',     # 创建时间
    'create_time',    # 创建时间
    'insert_time',    # 插入时间
    'timestamp',      # 通用时间戳
    'sync_time',      # 同步时间
    'modify_time'     # 修改时间
]
```

### 字段检测SQL
```sql
-- 检测表中是否存在时间戳字段
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'your_database' 
  AND TABLE_NAME = 'your_table'
  AND COLUMN_NAME IN ('updated_at', 'update_time', 'last_updated', 
                      'created_at', 'create_time', 'insert_time',
                      'timestamp', 'sync_time', 'modify_time')
ORDER BY 
  CASE COLUMN_NAME
    WHEN 'updated_at' THEN 1
    WHEN 'update_time' THEN 2
    WHEN 'last_updated' THEN 3
    WHEN 'created_at' THEN 4
    ELSE 5
  END;
```

---

## 🔍 增量查询条件

### 1. 基本增量查询
```sql
-- 基础增量同步SQL模板
SELECT * FROM {table_name} 
WHERE {timestamp_field} > '{last_sync_time}'
ORDER BY {timestamp_field} ASC;
```

### 2. 安全时间窗口查询
```sql
-- 带安全回退的增量查询（推荐）
-- 回退10分钟避免时钟偏差和事务延迟
SELECT * FROM {table_name} 
WHERE {timestamp_field} > '{safe_sync_time}'
ORDER BY {timestamp_field} ASC;

-- safe_sync_time = last_sync_time - 10分钟
```

### 3. 实际查询示例

#### 示例1：用户表增量同步
```sql
-- 表：users，时间戳字段：updated_at
-- 上次同步时间：2025-09-29 10:00:00
-- 安全回退时间：2025-09-29 09:50:00

SELECT 
    id,
    username, 
    email,
    phone,
    status,
    created_at,
    updated_at
FROM users 
WHERE updated_at > '2025-09-29 09:50:00'
ORDER BY updated_at ASC;
```

#### 示例2：订单表增量同步
```sql
-- 表：orders，时间戳字段：update_time
-- 上次同步时间：2025-09-29 10:30:00
-- 安全回退时间：2025-09-29 10:20:00

SELECT 
    order_id,
    user_id,
    total_amount,
    order_status,
    payment_status,
    create_time,
    update_time
FROM orders 
WHERE update_time > '2025-09-29 10:20:00'
ORDER BY update_time ASC;
```

#### 示例3：商品表增量同步
```sql
-- 表：products，时间戳字段：last_updated
-- 上次同步时间：2025-09-29 11:00:00
-- 安全回退时间：2025-09-29 10:50:00

SELECT 
    product_id,
    product_name,
    category_id,
    price,
    stock_quantity,
    status,
    created_at,
    last_updated
FROM products 
WHERE last_updated > '2025-09-29 10:50:00'
ORDER BY last_updated ASC;
```

---

## ⏰ 时间计算逻辑

### Python时间计算代码
```python
from datetime import datetime, timedelta

def calculate_sync_time(last_sync_time, lookback_minutes=10):
    """
    计算安全的同步时间点
    
    Args:
        last_sync_time: 上次同步时间
        lookback_minutes: 安全回退分钟数
    
    Returns:
        safe_sync_time: 安全的同步时间点
    """
    if last_sync_time is None:
        # 首次同步，返回很早的时间
        return datetime(1970, 1, 1)
    
    # 安全回退时间
    safe_sync_time = last_sync_time - timedelta(minutes=lookback_minutes)
    
    return safe_sync_time

# 示例使用
last_sync = datetime(2025, 9, 29, 10, 30, 0)  # 2025-09-29 10:30:00
safe_time = calculate_sync_time(last_sync, 10)  # 2025-09-29 10:20:00

print(f"上次同步时间: {last_sync}")
print(f"安全同步时间: {safe_time}")
```

### SQL时间格式化
```sql
-- MySQL时间格式化
SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') as current_time;
SELECT DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%Y-%m-%d %H:%i:%s') as safe_time;

-- 时间比较示例
SELECT COUNT(*) as changed_records
FROM your_table 
WHERE updated_at > '2025-09-29 10:20:00';
```

---

## 🎯 不同场景的查询策略

### 1. 高频更新表（如订单状态）
```sql
-- 每5分钟同步一次，回退2分钟
SELECT * FROM order_status_log 
WHERE update_time > DATE_SUB('{last_sync_time}', INTERVAL 2 MINUTE)
ORDER BY update_time ASC
LIMIT 1000;  -- 限制批量大小
```

### 2. 中频更新表（如用户信息）
```sql
-- 每小时同步一次，回退10分钟
SELECT * FROM user_profiles 
WHERE updated_at > DATE_SUB('{last_sync_time}', INTERVAL 10 MINUTE)
ORDER BY updated_at ASC;
```

### 3. 低频更新表（如商品信息）
```sql
-- 每天同步一次，回退30分钟
SELECT * FROM products 
WHERE last_updated > DATE_SUB('{last_sync_time}', INTERVAL 30 MINUTE)
ORDER BY last_updated ASC;
```

### 4. 只有创建时间的表
```sql
-- 对于只有created_at字段的表，只能检测新增记录
SELECT * FROM logs 
WHERE created_at > '{last_sync_time}'
ORDER BY created_at ASC;
```

---

## 🔄 完整的增量同步流程

### 1. 检测时间戳字段
```python
def detect_timestamp_field(cursor, table_name):
    """检测表的时间戳字段"""
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = DATABASE() 
      AND TABLE_NAME = %s
      AND COLUMN_NAME IN ('updated_at', 'update_time', 'last_updated', 
                          'created_at', 'create_time', 'insert_time',
                          'timestamp', 'sync_time', 'modify_time')
    ORDER BY 
      CASE COLUMN_NAME
        WHEN 'updated_at' THEN 1
        WHEN 'update_time' THEN 2
        WHEN 'last_updated' THEN 3
        WHEN 'created_at' THEN 4
        ELSE 5
      END
    """
    
    cursor.execute(sql, (table_name,))
    result = cursor.fetchone()
    
    return result[0] if result else None
```

### 2. 获取上次同步时间
```python
def get_last_sync_time(tenant_id, table_name):
    """从状态文件获取上次同步时间"""
    status_file = f"sync_status/{tenant_id}.json"
    
    if not os.path.exists(status_file):
        return None
    
    with open(status_file, 'r') as f:
        status = json.load(f)
    
    table_status = status.get('tables', {}).get(table_name, {})
    last_sync = table_status.get('last_sync_time')
    
    if last_sync:
        return datetime.fromisoformat(last_sync)
    
    return None
```

### 3. 构建增量查询
```python
def build_incremental_query(table_name, timestamp_field, last_sync_time, lookback_minutes=10):
    """构建增量同步查询"""
    
    if last_sync_time is None:
        # 首次同步，使用全量查询
        return f"SELECT * FROM {table_name} ORDER BY {timestamp_field} ASC"
    
    # 计算安全时间
    safe_time = last_sync_time - timedelta(minutes=lookback_minutes)
    safe_time_str = safe_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # 构建增量查询
    query = f"""
    SELECT * FROM {table_name} 
    WHERE {timestamp_field} > '{safe_time_str}'
    ORDER BY {timestamp_field} ASC
    """
    
    return query
```

### 4. 执行增量查询
```python
def execute_incremental_sync(cursor, table_name, timestamp_field, last_sync_time):
    """执行增量同步"""
    
    # 构建查询
    query = build_incremental_query(table_name, timestamp_field, last_sync_time)
    
    print(f"执行增量查询: {query}")
    
    # 执行查询
    cursor.execute(query)
    
    # 获取结果
    results = cursor.fetchall()
    
    print(f"找到 {len(results)} 条变更记录")
    
    return results
```

---

## 📊 查询性能优化

### 1. 索引优化
```sql
-- 为时间戳字段创建索引
CREATE INDEX idx_updated_at ON users(updated_at);
CREATE INDEX idx_update_time ON orders(update_time);
CREATE INDEX idx_last_updated ON products(last_updated);

-- 复合索引（如果需要按其他字段过滤）
CREATE INDEX idx_status_updated ON users(status, updated_at);
```

### 2. 分页查询（大表优化）
```sql
-- 大表分页增量查询
SELECT * FROM large_table 
WHERE updated_at > '2025-09-29 10:20:00'
ORDER BY updated_at ASC
LIMIT 1000 OFFSET 0;

-- 下一页
SELECT * FROM large_table 
WHERE updated_at > '2025-09-29 10:20:00'
ORDER BY updated_at ASC
LIMIT 1000 OFFSET 1000;
```

### 3. 批量处理示例
```python
def sync_large_table_incremental(cursor, table_name, timestamp_field, last_sync_time, batch_size=1000):
    """大表增量同步批量处理"""
    
    safe_time = last_sync_time - timedelta(minutes=10)
    safe_time_str = safe_time.strftime('%Y-%m-%d %H:%M:%S')
    
    offset = 0
    total_records = 0
    
    while True:
        # 分页查询
        query = f"""
        SELECT * FROM {table_name} 
        WHERE {timestamp_field} > '{safe_time_str}'
        ORDER BY {timestamp_field} ASC
        LIMIT {batch_size} OFFSET {offset}
        """
        
        cursor.execute(query)
        batch_data = cursor.fetchall()
        
        if not batch_data:
            break
        
        # 处理这批数据
        process_batch_data(batch_data)
        
        total_records += len(batch_data)
        offset += batch_size
        
        print(f"已处理 {total_records} 条记录")
        
        # 如果这批数据少于batch_size，说明已经是最后一批
        if len(batch_data) < batch_size:
            break
    
    return total_records
```

---

## 🚨 常见问题和解决方案

### 1. 时间戳字段不存在
```sql
-- 问题：表中没有时间戳字段
-- 解决：添加时间戳字段
ALTER TABLE your_table 
ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

-- 或者使用创建时间字段
ALTER TABLE your_table 
ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
```

### 2. 时钟偏差问题
```python
# 问题：服务器时钟不同步导致数据丢失
# 解决：增加安全回退时间
def safe_sync_time(last_sync, lookback_minutes=10):
    """安全的同步时间，回退指定分钟数"""
    return last_sync - timedelta(minutes=lookback_minutes)
```

### 3. 大量历史数据
```sql
-- 问题：首次同步数据量太大
-- 解决：分批次同步
SELECT * FROM large_table 
WHERE created_at >= '2025-01-01'  -- 只同步今年的数据
  AND created_at < '2025-02-01'   -- 按月分批
ORDER BY created_at ASC;
```

### 4. 时间戳精度问题
```sql
-- 问题：时间戳精度不够，可能丢失同一秒的数据
-- 解决：使用微秒精度或添加自增ID辅助
SELECT * FROM your_table 
WHERE updated_at > '2025-09-29 10:20:00.000000'
   OR (updated_at = '2025-09-29 10:20:00.000000' AND id > last_sync_id)
ORDER BY updated_at ASC, id ASC;
```

---

## 📈 监控和调试

### 1. 查询执行计划
```sql
-- 检查查询是否使用了索引
EXPLAIN SELECT * FROM users 
WHERE updated_at > '2025-09-29 10:20:00'
ORDER BY updated_at ASC;
```

### 2. 统计变更数据量
```sql
-- 统计指定时间段的变更记录数
SELECT 
    COUNT(*) as total_changes,
    MIN(updated_at) as earliest_change,
    MAX(updated_at) as latest_change
FROM users 
WHERE updated_at > '2025-09-29 10:20:00';
```

### 3. 检查数据分布
```sql
-- 按小时统计变更数据分布
SELECT 
    DATE_FORMAT(updated_at, '%Y-%m-%d %H:00:00') as hour_bucket,
    COUNT(*) as change_count
FROM users 
WHERE updated_at > '2025-09-29 00:00:00'
GROUP BY hour_bucket
ORDER BY hour_bucket;
```

---

## 🎯 总结

增量同步的核心是：

1. **时间戳字段检测** - 自动找到合适的时间字段
2. **安全时间窗口** - 回退一定时间避免数据丢失  
3. **增量查询构建** - WHERE条件过滤变更数据
4. **性能优化** - 索引、分页、批量处理
5. **错误处理** - 处理各种边界情况

**关键SQL模板**：
```sql
SELECT * FROM {table_name} 
WHERE {timestamp_field} > '{safe_sync_time}'
ORDER BY {timestamp_field} ASC;
```

这样就能准确识别和同步所有变更过的数据！