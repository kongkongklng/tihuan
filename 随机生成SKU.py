#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
随机SKU生成器
对SpiderResult.db3中的Content表进行随机SKU赋值，每个SKU都不重复
支持数字文件夹范围功能
"""

import sqlite3
import os
import random
import string
from pathlib import Path

# ==================== 配置区域 ====================
# 在这里修改您的配置

# 基础文件夹路径（包含数字文件夹的目录）
BASE_FOLDER = "D:\\火车采集器V10.28\\Data"

# 数据库文件名
DB_FILENAME = "SpiderResult.db3"

# 处理范围设置
START_FOLDER = 1642    # 起始文件夹编号，设为 None 表示从第一个开始
END_FOLDER = 1710     # 结束文件夹编号，设为 None 表示到最后一个结束

# 是否仅预览（不实际更新数据库）
PREVIEW_ONLY = False  # True=仅预览，False=实际执行更新

# SKU生成配置
SKU_PREFIX = "SKU"    # SKU前缀
SKU_LENGTH = 10        # SKU长度（不包括前缀）
SKU_COLUMN = "SKU"    # 数据库中的SKU列名

# ==================== 程序代码 ====================

class RandomSKUGenerator:
    def __init__(self):
        self.base_folder = Path(BASE_FOLDER)
        self.db_filename = DB_FILENAME
        self.start_folder = START_FOLDER
        self.end_folder = END_FOLDER
        self.preview_only = PREVIEW_ONLY
        self.sku_prefix = SKU_PREFIX
        self.sku_length = SKU_LENGTH
        self.sku_column = SKU_COLUMN
        self.used_skus = set()  # 用于存储已使用的SKU，确保不重复
        
    def generate_random_sku(self):
        """生成随机SKU"""
        while True:
            # 生成随机字符（数字和大写字母）
            chars = string.digits + string.ascii_uppercase
            random_part = ''.join(random.choice(chars) for _ in range(self.sku_length))
            sku = f"{self.sku_prefix}{random_part}"
            
            # 检查是否已存在
            if sku not in self.used_skus:
                self.used_skus.add(sku)
                return sku
    
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
    
    def check_content_table(self, db_path):
        """检查Content表是否存在SKU列"""
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # 检查Content表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
            if not cursor.fetchone():
                conn.close()
                return False, "Content表不存在"
            
            # 检查SKU列是否存在
            cursor.execute(f"PRAGMA table_info(Content)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            conn.close()
            
            if self.sku_column not in column_names:
                return False, f"SKU列 '{self.sku_column}' 不存在"
            
            return True, "OK"
            
        except Exception as e:
            return False, f"检查表结构时出错: {e}"
    
    def get_content_records_count(self, db_path):
        """获取Content表的记录数"""
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM Content")
            count = cursor.fetchone()[0]
            
            conn.close()
            return count
            
        except Exception as e:
            print(f"  获取记录数时出错: {e}")
            return 0
    
    def update_content_sku(self, db_folder):
        """更新单个数据库的Content表SKU"""
        db_path = db_folder / self.db_filename
        
        try:
            # 检查表结构
            table_ok, message = self.check_content_table(db_path)
            if not table_ok:
                print(f"  - {message}")
                return 0
            
            # 获取记录数
            record_count = self.get_content_records_count(db_path)
            if record_count == 0:
                print(f"  - Content表为空")
                return 0
            
            if self.preview_only:
                print(f"  [预览] Content表有 {record_count} 条记录，将要生成 {record_count} 个SKU")
                return record_count
            
            # 连接数据库
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # 获取所有需要更新的记录ID
            cursor.execute("SELECT id FROM Content")
            record_ids = [row[0] for row in cursor.fetchall()]
            
            updated_count = 0
            
            # 为每条记录生成SKU
            for record_id in record_ids:
                sku = self.generate_random_sku()
                
                try:
                    update_sql = f"""
                    UPDATE Content 
                    SET {self.sku_column} = ? 
                    WHERE id = ?
                    """
                    cursor.execute(update_sql, (sku, record_id))
                    updated_count += 1
                    
                except Exception as e:
                    print(f"    更新记录 {record_id} 时出错: {e}")
            
            # 提交更改
            conn.commit()
            conn.close()
            
            return updated_count
            
        except Exception as e:
            print(f"  更新数据库 {db_path} 时出错: {e}")
            return 0
    
    def run_generation(self):
        """执行SKU生成操作"""
        if self.preview_only:
            print("=== 随机SKU生成器 (预览模式) ===")
        else:
            print("=== 随机SKU生成器 ===")
            
        print(f"基础文件夹: {self.base_folder}")
        print(f"数据库文件名: {self.db_filename}")
        print(f"SKU前缀: {self.sku_prefix}")
        print(f"SKU长度: {self.sku_length}")
        print(f"SKU列名: {self.sku_column}")
        
        # 显示范围信息
        if self.start_folder is not None or self.end_folder is not None:
            if self.start_folder is not None and self.end_folder is not None:
                print(f"处理范围: 文件夹 {self.start_folder} 到 {self.end_folder}")
            elif self.start_folder is not None:
                print(f"处理范围: 从文件夹 {self.start_folder} 开始")
            elif self.end_folder is not None:
                print(f"处理范围: 到文件夹 {self.end_folder} 结束")
        
        print("=" * 60)
        
        # 查找数据库文件夹
        db_folders = self.find_database_folders()
        if not db_folders:
            return
        
        if self.preview_only:
            print(f"\n预览模式 - 将要处理 {len(db_folders)} 个数据库...")
        else:
            print(f"\n开始生成SKU，共处理 {len(db_folders)} 个数据库...")
        print("-" * 60)
        
        total_updated_all = 0
        success_count = 0
        
        # 逐个处理数据库
        for i, folder in enumerate(db_folders):
            print(f"\n[{i+1}/{len(db_folders)}] 处理文件夹: {folder.name}")
            
            # 更新数据库
            updated_count = self.update_content_sku(folder)
            
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
            print(f"将要处理: {success_count}/{len(db_folders)} 个数据库")
        else:
            print("SKU生成完成！")
            print(f"成功处理: {success_count}/{len(db_folders)} 个数据库")
            print(f"总共更新: {total_updated_all} 条记录")
            print(f"生成的SKU数量: {len(self.used_skus)}")
        
        if success_count < len(db_folders):
            print(f"失败: {len(db_folders) - success_count} 个数据库")

def main():
    """主函数"""
    generator = RandomSKUGenerator()
    generator.run_generation()

if __name__ == "__main__":
    main()
