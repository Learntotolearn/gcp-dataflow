#!/usr/bin/env python3
"""
状态文件迁移工具
将单表状态文件合并为按数据库分组的状态文件
"""

import json
import os
from pathlib import Path
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def migrate_status_files(status_dir: str = "sync_status"):
    """迁移状态文件"""
    status_path = Path(status_dir)
    
    if not status_path.exists():
        logger.info("状态目录不存在，无需迁移")
        return
    
    # 扫描所有单表状态文件
    single_table_files = list(status_path.glob("*_*.json"))
    
    if not single_table_files:
        logger.info("未找到需要迁移的单表状态文件")
        return
    
    logger.info(f"发现 {len(single_table_files)} 个单表状态文件，开始迁移...")
    
    # 按数据库分组
    databases = {}
    
    for file_path in single_table_files:
        try:
            # 解析文件名：tenant_id_table_name.json
            filename = file_path.stem
            parts = filename.split('_')
            
            if len(parts) < 2:
                logger.warning(f"跳过格式不正确的文件: {file_path.name}")
                continue
            
            # 找到tenant_id和table_name的分界点
            # 假设tenant_id不包含下划线，table_name可能包含下划线
            tenant_id = parts[0]
            table_name = '_'.join(parts[1:])
            
            # 读取文件内容
            with open(file_path, 'r', encoding='utf-8') as f:
                table_data = json.load(f)
            
            # 验证数据格式
            if not isinstance(table_data, dict) or 'table_name' not in table_data:
                logger.warning(f"跳过格式不正确的数据文件: {file_path.name}")
                continue
            
            # 按数据库分组
            if tenant_id not in databases:
                databases[tenant_id] = {
                    'database_info': {
                        'tenant_id': tenant_id,
                        'last_updated': datetime.now().isoformat()
                    },
                    'tables': {}
                }
            
            # 添加表数据
            databases[tenant_id]['tables'][table_name] = {
                'table_name': table_data.get('table_name', table_name),
                'last_sync_time': table_data.get('last_sync_time'),
                'sync_status': table_data.get('sync_status', 'UNKNOWN'),
                'sync_mode': table_data.get('sync_mode', 'UNKNOWN'),
                'records_synced': table_data.get('records_synced', 0),
                'error_message': table_data.get('error_message'),
                'updated_at': table_data.get('updated_at', datetime.now().isoformat())
            }
            
            logger.info(f"  ✅ 迁移表: {tenant_id}.{table_name}")
            
        except Exception as e:
            logger.error(f"  ❌ 迁移失败 {file_path.name}: {e}")
    
    # 保存合并后的数据库状态文件
    migrated_count = 0
    for tenant_id, db_data in databases.items():
        try:
            # 更新数据库信息
            db_data['database_info']['total_tables'] = len(db_data['tables'])
            
            # 保存新格式文件
            new_file_path = status_path / f"{tenant_id}.json"
            with open(new_file_path, 'w', encoding='utf-8') as f:
                json.dump(db_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ 创建数据库状态文件: {new_file_path.name} ({len(db_data['tables'])} 张表)")
            migrated_count += 1
            
        except Exception as e:
            logger.error(f"❌ 保存数据库状态文件失败 {tenant_id}: {e}")
    
    # 创建备份目录并移动旧文件
    if migrated_count > 0:
        backup_dir = status_path / "backup_single_table_files"
        backup_dir.mkdir(exist_ok=True)
        
        moved_count = 0
        for file_path in single_table_files:
            try:
                backup_path = backup_dir / file_path.name
                file_path.rename(backup_path)
                moved_count += 1
            except Exception as e:
                logger.warning(f"⚠️ 移动备份文件失败 {file_path.name}: {e}")
        
        logger.info(f"📦 已将 {moved_count} 个旧文件移动到备份目录: {backup_dir}")
    
    logger.info(f"🎉 迁移完成！成功迁移 {migrated_count} 个数据库的状态文件")

def preview_migration(status_dir: str = "sync_status"):
    """预览迁移结果"""
    status_path = Path(status_dir)
    
    if not status_path.exists():
        logger.info("状态目录不存在")
        return
    
    # 扫描单表文件
    single_table_files = list(status_path.glob("*_*.json"))
    
    # 扫描数据库文件（排除包含下划线的文件）
    database_files = [f for f in status_path.glob("*.json") 
                     if '_' not in f.stem and f in status_path.iterdir()]
    
    logger.info("📊 当前状态文件概览:")
    logger.info(f"  📄 单表文件: {len(single_table_files)} 个")
    logger.info(f"  🗄️ 数据库文件: {len(database_files)} 个")
    
    if single_table_files:
        logger.info("\n📄 单表文件列表:")
        databases_preview = {}
        for file_path in single_table_files:
            filename = file_path.stem
            parts = filename.split('_')
            if len(parts) >= 2:
                tenant_id = parts[0]
                table_name = '_'.join(parts[1:])
                
                if tenant_id not in databases_preview:
                    databases_preview[tenant_id] = []
                databases_preview[tenant_id].append(table_name)
        
        for tenant_id, tables in databases_preview.items():
            logger.info(f"  🏪 {tenant_id}: {len(tables)} 张表 ({', '.join(tables[:3])}{'...' if len(tables) > 3 else ''})")
    
    if database_files:
        logger.info("\n🗄️ 数据库文件列表:")
        for file_path in database_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    table_count = len(data.get('tables', {}))
                    last_updated = data.get('database_info', {}).get('last_updated', '未知')
                    logger.info(f"  🏪 {file_path.stem}: {table_count} 张表 (更新: {last_updated})")
            except Exception as e:
                logger.warning(f"  ⚠️ 读取失败 {file_path.name}: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--preview':
        preview_migration()
    else:
        migrate_status_files()