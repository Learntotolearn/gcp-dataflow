#!/usr/bin/env python3
"""
测试新的状态管理器功能
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# 添加当前目录到路径
sys.path.append('.')

# 导入状态管理器
from smart_sync_incremental_optimized import LocalFileStatusManager

def test_status_manager():
    """测试状态管理器功能"""
    print("🧪 测试新的状态管理器功能")
    print("=" * 50)
    
    # 创建状态管理器
    status_manager = LocalFileStatusManager("sync_status")
    
    # 测试1: 读取现有状态
    print("\n📖 测试1: 读取现有状态")
    tenant_id = "shop4282489245696000"
    table_name = "ttpos_member"
    
    last_sync_time = status_manager.get_last_sync_time(tenant_id, table_name)
    print(f"  📅 {tenant_id}.{table_name} 上次同步时间: {last_sync_time}")
    
    # 测试2: 获取数据库摘要
    print("\n📊 测试2: 获取数据库摘要")
    summary = status_manager.get_database_summary(tenant_id)
    print(f"  🏪 数据库: {summary['tenant_id']}")
    print(f"  📋 总表数: {summary['total_tables']}")
    print(f"  🕐 最后更新: {summary['last_updated']}")
    print("  📄 表列表:")
    for table, info in summary['tables'].items():
        print(f"    - {table}: {info['sync_mode']} ({info['records_synced']} 行)")
    
    # 测试3: 更新状态
    print("\n✏️ 测试3: 更新状态")
    test_table = "test_table"
    current_time = datetime.now()
    
    status_manager.update_sync_status(
        tenant_id, test_table, current_time, 
        "INCREMENTAL", 100, "SUCCESS"
    )
    print(f"  ✅ 已更新 {tenant_id}.{test_table} 状态")
    
    # 验证更新
    updated_time = status_manager.get_last_sync_time(tenant_id, test_table)
    print(f"  📅 验证更新时间: {updated_time}")
    
    # 测试4: 查看更新后的摘要
    print("\n📊 测试4: 更新后的数据库摘要")
    updated_summary = status_manager.get_database_summary(tenant_id)
    print(f"  📋 总表数: {updated_summary['total_tables']}")
    print(f"  🆕 新增表: {test_table}")
    
    print("\n🎉 所有测试完成！")

def show_all_databases():
    """显示所有数据库状态"""
    print("\n🗄️ 所有数据库状态概览")
    print("=" * 50)
    
    status_dir = Path("sync_status")
    if not status_dir.exists():
        print("❌ 状态目录不存在")
        return
    
    database_files = [f for f in status_dir.glob("*.json") if '_' not in f.stem]
    
    for db_file in database_files:
        try:
            with open(db_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            db_info = data.get('database_info', {})
            tables = data.get('tables', {})
            
            print(f"\n🏪 数据库: {db_info.get('tenant_id', db_file.stem)}")
            print(f"  📋 表数量: {len(tables)}")
            print(f"  🕐 最后更新: {db_info.get('last_updated', '未知')}")
            
            # 统计同步模式
            full_count = sum(1 for t in tables.values() if t.get('sync_mode') == 'FULL')
            incremental_count = sum(1 for t in tables.values() if t.get('sync_mode') == 'INCREMENTAL')
            success_count = sum(1 for t in tables.values() if t.get('sync_status') == 'SUCCESS')
            
            print(f"  📊 同步统计:")
            print(f"    ✅ 成功: {success_count}/{len(tables)}")
            print(f"    🔄 全量: {full_count}, ⚡ 增量: {incremental_count}")
            
            # 显示最近同步的表
            recent_tables = sorted(
                tables.items(), 
                key=lambda x: x[1].get('updated_at', ''), 
                reverse=True
            )[:3]
            
            print(f"  📄 最近同步的表:")
            for table_name, table_info in recent_tables:
                sync_time = table_info.get('last_sync_time', '未知')[:19] if table_info.get('last_sync_time') else '未知'
                records = table_info.get('records_synced', 0)
                mode = table_info.get('sync_mode', '未知')
                print(f"    - {table_name}: {sync_time} ({records} 行, {mode})")
                
        except Exception as e:
            print(f"❌ 读取数据库文件失败 {db_file.name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--overview':
        show_all_databases()
    else:
        test_status_manager()
        show_all_databases()