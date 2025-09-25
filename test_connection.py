#!/usr/bin/env python3
"""
测试 MySQL 和 BigQuery 连接
"""

import mysql.connector
from google.cloud import bigquery
import json
import sys

def test_mysql_connection():
    """测试 MySQL 连接"""
    print("🔍 测试 MySQL 连接...")
    
    with open('params.json', 'r') as f:
        params = json.load(f)
    
    try:
        # 测试每个数据库
        db_list = params['db_list'].split(',')
        table_list = params['table_list'].split(',')
        
        for db_name in db_list:
            db_name = db_name.strip()
            print(f"  📊 连接数据库: {db_name}")
            
            conn = mysql.connector.connect(
                host=params['db_host'],
                port=int(params['db_port']),
                user=params['db_user'],
                password=params['db_pass'],
                database=db_name
            )
            
            cursor = conn.cursor()
            
            # 测试每个表
            for table_name in table_list:
                table_name = table_name.strip()
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"    ✅ {table_name}: {count} 行")
                except Exception as e:
                    print(f"    ❌ {table_name}: {str(e)}")
            
            cursor.close()
            conn.close()
            
        print("✅ MySQL 连接测试完成")
        return True
        
    except Exception as e:
        print(f"❌ MySQL 连接失败: {str(e)}")
        return False

def test_bigquery_connection():
    """测试 BigQuery 连接"""
    print("🔍 测试 BigQuery 连接...")
    
    with open('params.json', 'r') as f:
        params = json.load(f)
    
    try:
        client = bigquery.Client(project=params['bq_project'])
        
        # 检查数据集是否存在
        dataset_id = params['bq_dataset']
        try:
            dataset = client.get_dataset(dataset_id)
            print(f"  ✅ 数据集存在: {dataset_id}")
        except:
            print(f"  ⚠️ 数据集不存在，将自动创建: {dataset_id}")
        
        # 测试查询权限
        query = "SELECT 1 as test"
        job = client.query(query)
        result = list(job.result())
        print(f"  ✅ 查询权限正常")
        
        print("✅ BigQuery 连接测试完成")
        return True
        
    except Exception as e:
        print(f"❌ BigQuery 连接失败: {str(e)}")
        return False

def main():
    print("🧪 开始连接测试...\n")
    
    mysql_ok = test_mysql_connection()
    print()
    bq_ok = test_bigquery_connection()
    print()
    
    if mysql_ok and bq_ok:
        print("🎉 所有连接测试通过！可以开始数据同步。")
        sys.exit(0)
    else:
        print("💥 连接测试失败，请检查配置。")
        sys.exit(1)

if __name__ == "__main__":
    main()