#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版批量分类更新程序
直接在代码中修改配置参数，无需命令行
"""

import sqlite3
import os
from pathlib import Path

# ==================== 配置区域 ====================
# 在这里修改您的配置

# 基础文件夹路径（包含数字文件夹的目录）
BASE_FOLDER = r"D:\火车采集器V10.28\Data"  # 当前文件夹，可以改为 "D:\\我的数据" 等

# 分类文件路径
CATEGORIES_FILE = r"D:\project\reef\分类\分类.txt"

# 数据库文件名
DB_FILENAME = "SpiderResult.db3"

# 处理范围设置
START_FOLDER = 1642    # 起始文件夹编号，设为 None 表示从第一个开始
END_FOLDER = 1710   # 结束文件夹编号，设为 None 表示到最后一个结束

# 是否仅预览（不实际更新数据库）
PREVIEW_ONLY = False  # True=仅预览，False=实际执行更新

# ==================== 程序代码 ====================

class SimpleCategoryUpdater:
    def __init__(self):
        self.base_folder = Path(BASE_FOLDER)
        self.categories_file = Path(CATEGORIES_FILE)
        self.db_filename = DB_FILENAME
        self.start_folder = START_FOLDER
        self.end_folder = END_FOLDER
        self.preview_only = PREVIEW_ONLY
        
    def read_categories(self):
        """读取分类文件"""
        categories = []
        try:
            if not self.categories_file.exists():
                print(f"错误: 分类文件 {self.categories_file} 不存在！")
                return []
            
            with open(self.categories_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # 跳过空行
                        categories.append((line_num, line))
            
            print(f"✓ 成功读取分类文件，共 {len(categories)} 行")
            return categories
            
        except Exception as e:
            print(f"读取分类文件时出错: {e}")
            return []
    
    def is_numeric_folder(self, folder_name):
        """判断文件夹名是否为纯数字"""
        return folder_name.isdigit()
    
    def get_folder_number(self, folder_name):
        """获取文件夹的数字值"""
        if self.is_numeric_folder(folder_name):
            return int(folder_name)
        return None
    
    def is_folder_in_range(self, folder_name):
        """判断文件夹是否在指定范围内"""
        if not self.is_numeric_folder(folder_name):
            return False
        
        folder_num = self.get_folder_number(folder_name)
        
        # 如果没有指定范围，则包含所有数字文件夹
        if self.start_folder is None and self.end_folder is None:
            return True
        
        # 只指定了起始文件夹
        if self.start_folder is not None and self.end_folder is None:
            return folder_num >= self.start_folder
        
        # 只指定了结束文件夹
        if self.start_folder is None and self.end_folder is not None:
            return folder_num <= self.end_folder
        
        # 指定了完整范围
        return self.start_folder <= folder_num <= self.end_folder
    
    def find_database_folders(self):
        """查找包含SpiderResult.db3的文件夹"""
        db_folders = []
        
        try:
            # 遍历基础文件夹下的所有子文件夹
            for folder in self.base_folder.iterdir():
                if folder.is_dir():
                    db_file = folder / self.db_filename
                    if db_file.exists():
                        # 检查是否在指定范围内
                        if self.is_folder_in_range(folder.name):
                            db_folders.append(folder)
            
            # 按数字顺序排序
            db_folders.sort(key=lambda x: self.get_folder_number(x.name) or 0)
            
            range_info = ""
            if self.start_folder is not None or self.end_folder is not None:
                if self.start_folder is not None and self.end_folder is not None:
                    range_info = f" (范围: {self.start_folder}-{self.end_folder})"
                elif self.start_folder is not None:
                    range_info = f" (从 {self.start_folder} 开始)"
                elif self.end_folder is not None:
                    range_info = f" (到 {self.end_folder} 结束)"
            
            print(f"✓ 找到 {len(db_folders)} 个包含数据库的文件夹{range_info}")
            return db_folders
            
        except Exception as e:
            print(f"查找数据库文件夹时出错: {e}")
            return []
    
    def update_single_database(self, db_folder, category):
        """更新单个数据库文件"""
        db_path = db_folder / self.db_filename
        
        try:
            # 连接数据库
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # 获取所有表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            total_updated = 0
            
            # 处理每个表
            for table_name in tables:
                # 获取表的列信息
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                # 查找分类相关列
                category_columns = []
                for col in columns:
                    col_name = col[1].lower()
                    if any(keyword in col_name for keyword in ['category', '分类', 'cat', 'type', '类型', 'tag', '标签']):
                        category_columns.append(col[1])
                
                # 更新分类
                for column in category_columns:
                    try:
                        # 更新空值或NULL值的分类字段
                        update_sql = f"""
                        UPDATE {table_name} 
                        SET {column} = ? 
                        WHERE {column} IS NULL OR {column} = '' OR {column} = 'NULL'
                        """
                        cursor.execute(update_sql, (category,))
                        updated_rows = cursor.rowcount
                        total_updated += updated_rows
                        
                    except Exception as e:
                        print(f"    更新列 {column} 时出错: {e}")
            
            # 提交更改
            conn.commit()
            conn.close()
            
            return total_updated
            
        except Exception as e:
            print(f"  更新数据库 {db_path} 时出错: {e}")
            return 0
    
    def run_update(self):
        """执行更新操作"""
        if self.preview_only:
            print("=== 批量分类更新程序 (预览模式) ===")
        else:
            print("=== 批量分类更新程序 ===")
            
        print(f"基础文件夹: {self.base_folder}")
        print(f"分类文件: {self.categories_file}")
        print(f"数据库文件名: {self.db_filename}")
        
        # 显示范围信息
        if self.start_folder is not None or self.end_folder is not None:
            if self.start_folder is not None and self.end_folder is not None:
                print(f"处理范围: 文件夹 {self.start_folder} 到 {self.end_folder}")
            elif self.start_folder is not None:
                print(f"处理范围: 从文件夹 {self.start_folder} 开始")
            elif self.end_folder is not None:
                print(f"处理范围: 到文件夹 {self.end_folder} 结束")
        
        print("=" * 60)
        
        # 读取分类信息
        categories = self.read_categories()
        if not categories:
            return
        
        # 查找数据库文件夹
        db_folders = self.find_database_folders()
        if not db_folders:
            return
        
        # 检查数量是否匹配
        if len(categories) != len(db_folders):
            print(f"警告: 分类数量 ({len(categories)}) 与数据库文件夹数量 ({len(db_folders)}) 不匹配！")
            print("将使用较少的数量进行处理")
        
        # 确定处理数量
        process_count = min(len(categories), len(db_folders))
        
        if self.preview_only:
            print(f"\n预览模式 - 将要处理 {process_count} 个数据库...")
        else:
            print(f"\n开始批量更新，共处理 {process_count} 个数据库...")
        print("-" * 60)
        
        total_updated_all = 0
        success_count = 0
        
        # 逐个处理数据库
        for i in range(process_count):
            folder = db_folders[i]
            line_num, category = categories[i]
            
            print(f"\n[{i+1}/{process_count}] 处理文件夹: {folder.name}")
            print(f"  分类信息 (第{line_num}行): {category}")
            
            if self.preview_only:
                print(f"  [预览] 将要更新此数据库")
                success_count += 1
            else:
                # 更新数据库
                updated_count = self.update_single_database(folder, category)
                
                if updated_count > 0:
                    print(f"  ✓ 更新了 {updated_count} 条记录")
                    total_updated_all += updated_count
                    success_count += 1
                else:
                    print(f"  - 无需更新或更新失败")
        
        # 显示总结
        print("\n" + "=" * 60)
        if self.preview_only:
            print("预览完成！")
            print(f"将要处理: {success_count}/{process_count} 个数据库")
        else:
            print("批量更新完成！")
            print(f"成功处理: {success_count}/{process_count} 个数据库")
            print(f"总共更新: {total_updated_all} 条记录")
        
        if success_count < process_count:
            print(f"失败: {process_count - success_count} 个数据库")

def main():
    """主函数"""
    updater = SimpleCategoryUpdater()
    updater.run_update()

if __name__ == "__main__":
    main()
