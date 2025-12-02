#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
采集后的数据处理整合工具
整合了去重、折扣价、随机生成SKU、批量增加分类四个功能
按照指定顺序执行：去重 → 折扣价 → 随机生成SKU → 批量增加分类
"""

import os
import sqlite3
import shutil
import random
import string
from pathlib import Path
from typing import List, Tuple, Set
import logging
from datetime import datetime

# 尝试导入tqdm，如果不存在则使用简单的进度显示
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("提示: 安装 tqdm 库可以获得更好的进度条显示: pip install tqdm")

class SimpleProgressBar:
    """简单的进度条实现，当tqdm不可用时使用"""
    def __init__(self, total, desc="处理中"):
        self.total = total
        self.current = 0
        self.desc = desc
        self.start_time = datetime.now()
        
    def update(self, n=1):
        self.current += n
        self._display()
        
    def _display(self):
        if self.total > 0:
            percentage = (self.current / self.total) * 100
            elapsed = datetime.now() - self.start_time
            if self.current > 0:
                eta = (elapsed / self.current) * (self.total - self.current)
                eta_str = f"预计剩余: {eta.total_seconds():.1f}秒"
            else:
                eta_str = "预计剩余: 计算中..."
            
            print(f"\r{self.desc}: {self.current}/{self.total} ({percentage:.1f}%) - {eta_str}", end="", flush=True)
            
    def close(self):
        print()  # 换行

# ==================== 配置区域 ====================

# 基础文件夹路径（包含数字文件夹的目录）
BASE_FOLDER = r"D:\火车采集器V10.28\Data"

# 分类文件路径
CATEGORIES_FILE = r"D:\project\amiri\分类\分类.txt"

# 数据库文件名
DB_FILENAME = "SpiderResult.db3"

# 处理范围设置
START_FOLDER = 5211    # 起始文件夹编号
END_FOLDER = 5523      # 结束文件夹编号

# 折扣价格设置
DISCOUNT_RATE = 0.2  # 折扣率（0.3表示3折）

# SKU生成配置
SKU_PREFIX = "SKU"     # SKU前缀
SKU_LENGTH = 10        # SKU长度（不包括前缀）
SKU_COLUMN = "SKU"     # 数据库中的SKU列名

# ==================== 功能开关 ====================
ENABLE_DEDUPLICATION = True  # True=开启去重，False=跳过去重
ENABLE_DISCOUNT     = True   # True=开启折扣价，False=跳过折扣价
ENABLE_RANDOM_SKU   = True   # True=开启随机SKU，False=跳过生成
ENABLE_BATCH_CAT    = False   # True=开启批量分类，False=跳过分类更新

# 是否仅预览（不实际更新数据库）
PREVIEW_ONLY = False   # True=仅预览，False=实际执行更新

# 日志开关
ENABLE_LOGGING = False  # True=启用日志，False=禁用日志
LOG_LEVEL = "INFO"     # 日志级别：DEBUG, INFO, WARNING, ERROR

# 进度条开关
ENABLE_PROGRESS_BAR = True  # True=启用进度条，False=禁用进度条

# ==================== 程序代码 ====================

class DataProcessor:
    def __init__(self):
        self.base_folder = Path(BASE_FOLDER)
        self.categories_file = Path(CATEGORIES_FILE)
        self.db_filename = DB_FILENAME
        self.start_folder = START_FOLDER
        self.end_folder = END_FOLDER
        self.discount_rate = DISCOUNT_RATE
        self.sku_prefix = SKU_PREFIX
        self.sku_length = SKU_LENGTH
        self.sku_column = SKU_COLUMN
        self.preview_only = PREVIEW_ONLY
        self.used_skus = set()  # 用于存储已使用的SKU，确保不重复
        
        # 日志和进度条配置
        self.enable_logging = ENABLE_LOGGING
        self.log_level = LOG_LEVEL
        self.enable_progress_bar = ENABLE_PROGRESS_BAR
        
        # 设置日志
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志配置"""
        if not self.enable_logging:
            # 禁用日志
            logging.getLogger().disabled = True
            self.logger = logging.getLogger(__name__)
            return
            
        log_filename = f"数据处理整合_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # 设置日志级别
        log_level = getattr(logging, self.log_level.upper(), logging.INFO)
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def backup_database(self, db_path: Path) -> bool:
        """在相同目录下的 backup 文件夹中备份数据库。
        成功返回 True，失败返回 False。
        """
        try:
            if not db_path.exists():
                self.logger.error(f"数据库不存在，无法备份: {db_path}")
                return False

            backup_dir = db_path.parent / "backup"
            backup_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"{db_path.stem}_{timestamp}.bak"
            backup_path = backup_dir / backup_name

            shutil.copy2(str(db_path), str(backup_path))
            self.logger.info(f"✅ 已备份数据库到: {backup_path}")
            return True
        except Exception as e:
            self.logger.error(f"备份数据库失败: {e}")
            return False

    def is_numeric_folder(self, folder_name: str) -> bool:
        """判断文件夹名是否为纯数字"""
        return folder_name.isdigit()
    
    def get_folder_number(self, folder_name: str) -> int:
        """获取文件夹的数字值"""
        if self.is_numeric_folder(folder_name):
            return int(folder_name)
        return 0
    
    def is_folder_in_range(self, folder_name: str) -> bool:
        """判断文件夹是否在指定范围内"""
        if not self.is_numeric_folder(folder_name):
            return False
        
        folder_num = self.get_folder_number(folder_name)
        return self.start_folder <= folder_num <= self.end_folder
    
    def find_database_folders(self) -> List[Path]:
        """查找包含SpiderResult.db3的文件夹"""
        db_folders = []
        try:
            for folder in self.base_folder.iterdir():
                if folder.is_dir():
                    db_file = folder / self.db_filename
                    if db_file.exists() and self.is_folder_in_range(folder.name):
                        db_folders.append(folder)
            db_folders.sort(key=lambda x: self.get_folder_number(x.name))
            return db_folders
        except Exception as e:
            self.logger.error(f"查找数据库文件夹时出错: {e}")
            return []

    # ========== step1_deduplication, step2_discount_price, step3_random_sku, step4_batch_categories ==========
    # （保持和你原始文件完全一致，这里省略重复粘贴）
    def step1_deduplication(self, db_folders: List[Path]) -> int:
        """步骤1：去重处理"""
        self.logger.info("\n" + "="*60)
        self.logger.info("步骤1：开始去重处理")
        self.logger.info("="*60)
        
        success_count = 0
        total_deleted = 0
        
        # 创建进度条
        if self.enable_progress_bar:
            if HAS_TQDM:
                pbar = tqdm(total=len(db_folders), desc="步骤1: 去重处理", unit="个")
            else:
                pbar = SimpleProgressBar(len(db_folders), "步骤1: 去重处理")
        else:
            pbar = None
        
        for i, folder in enumerate(db_folders):
            db_path = folder / self.db_filename
            self.logger.info(f"\n[{i+1}/{len(db_folders)}] 处理：{folder.name}/SpiderResult.db3")
            
            if not self.preview_only:
                # 备份数据库
                if not self.backup_database(db_path):
                    continue
            
            try:
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                
                if self.preview_only:
                    # 预览模式：统计重复数据
                    cursor.execute("SELECT COUNT(*) FROM Content")
                    total_count = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*) FROM (
                            SELECT SKU, COUNT(*) as cnt 
                            FROM Content 
                            GROUP BY SKU 
                            HAVING cnt > 1
                        )
                    """)
                    duplicate_groups = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*) FROM Content 
                        WHERE 图片 IS NULL OR TRIM(图片) = ''
                    """)
                    empty_img_count = cursor.fetchone()[0]
                    
                    self.logger.info(f"  [预览] 总记录: {total_count}, 重复组: {duplicate_groups}, 空图片: {empty_img_count}")
                    
                else:
                    # 备份 Content 表
                    cursor.execute("DROP TABLE IF EXISTS Content_backup")
                    cursor.execute("CREATE TABLE Content_backup AS SELECT * FROM Content")
                    self.logger.info("✅ 已创建 Content_backup 表")
                    
                    # 删除重复 SKU，仅保留 ID 最小的那条
                    cursor.execute("""
                        DELETE FROM Content 
                        WHERE ID NOT IN ( 
                            SELECT MIN(ID) 
                            FROM Content 
                            GROUP BY SKU 
                        ) 
                    """)
                    dup_deleted = cursor.rowcount
                    self.logger.info(f"✅ 去重完成，删除 {dup_deleted} 条重复数据")
                    
                    # 删除图片字段为空或NULL的行
                    cursor.execute("""
                        DELETE FROM Content 
                        WHERE 图片 IS NULL OR TRIM(图片) = '' 
                    """)
                    img_deleted = cursor.rowcount
                    self.logger.info(f"✅ 已删除 {img_deleted} 条图片为空的数据")
                    
                    total_deleted += dup_deleted + img_deleted
                    conn.commit()
                
                conn.close()
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"❌ 处理失败：{e}")
            
            # 更新进度条
            if pbar:
                pbar.update(1)
        
        # 关闭进度条
        if pbar:
            pbar.close()
        
        self.logger.info(f"\n去重处理完成！成功处理: {success_count}/{len(db_folders)} 个数据库")
        if not self.preview_only:
            self.logger.info(f"总共删除: {total_deleted} 条记录")
        
        return success_count
    
    def step2_discount_price(self, db_folders: List[Path]) -> int:
        """步骤2：折扣价处理"""
        self.logger.info("\n" + "="*60)
        self.logger.info("步骤2：开始折扣价处理")
        self.logger.info("="*60)
        
        success_count = 0
        total_updated = 0
        
        # 创建进度条
        if self.enable_progress_bar:
            if HAS_TQDM:
                pbar = tqdm(total=len(db_folders), desc="步骤2: 折扣价处理", unit="个")
            else:
                pbar = SimpleProgressBar(len(db_folders), "步骤2: 折扣价处理")
        else:
            pbar = None
        
        for i, folder in enumerate(db_folders):
            db_path = folder / self.db_filename
            self.logger.info(f"\n[{i+1}/{len(db_folders)}] 处理：{folder.name}/SpiderResult.db3")
            
            try:
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                
                # 检查是否有 Content 表和 销售价/折扣价字段
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
                if cursor.fetchone():
                    if self.preview_only:
                        # 预览模式：统计需要更新的记录
                        cursor.execute("""
                            SELECT COUNT(*) FROM Content
                            WHERE 销售价 GLOB '[0-9]*'
                        """)
                        count = cursor.fetchone()[0]
                        self.logger.info(f"  [预览] 将要更新 {count} 条记录的折扣价")
                    else:
                        # 将折扣价更新为销售价 × 折扣率
                        cursor.execute(f"""
                            UPDATE Content
                            SET 折扣价 = printf('%.2f', CAST(销售价 AS REAL) * {self.discount_rate})
                            WHERE 销售价 GLOB '[0-9]*'
                        """)
                        updated_count = cursor.rowcount
                        conn.commit()
                        self.logger.info(f"✅ 已更新折扣价：{updated_count} 条记录")
                        total_updated += updated_count
                else:
                    self.logger.info(f"⚠️ 跳过（无 Content 表）")
                
                conn.close()
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"❌ 处理失败：{e}")
            
            # 更新进度条
            if pbar:
                pbar.update(1)
        
        # 关闭进度条
        if pbar:
            pbar.close()
        
        self.logger.info(f"\n折扣价处理完成！成功处理: {success_count}/{len(db_folders)} 个数据库")
        if not self.preview_only:
            self.logger.info(f"总共更新: {total_updated} 条记录")
        
        return success_count
    def generate_random_sku(self) -> str:
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

    def step3_random_sku(self, db_folders: List[Path]) -> int:
        """步骤3：随机生成SKU"""
        self.logger.info("\n" + "="*60)
        self.logger.info("步骤3：开始随机生成SKU")
        self.logger.info("="*60)
        
        success_count = 0
        total_updated = 0
        
        # 创建进度条
        if self.enable_progress_bar:
            if HAS_TQDM:
                pbar = tqdm(total=len(db_folders), desc="步骤3: 随机生成SKU", unit="个")
            else:
                pbar = SimpleProgressBar(len(db_folders), "步骤3: 随机生成SKU")
        else:
            pbar = None
        
        for i, folder in enumerate(db_folders):
            db_path = folder / self.db_filename
            self.logger.info(f"\n[{i+1}/{len(db_folders)}] 处理：{folder.name}/SpiderResult.db3")
            
            try:
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                
                # 检查Content表是否存在SKU列
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
                if not cursor.fetchone():
                    self.logger.info(f"  - Content表不存在")
                    conn.close()
                    continue
                
                cursor.execute(f"PRAGMA table_info(Content)")
                columns = cursor.fetchall()
                column_names = [col[1] for col in columns]
                
                if self.sku_column not in column_names:
                    self.logger.info(f"  - SKU列 '{self.sku_column}' 不存在")
                    conn.close()
                    continue
                
                # 获取记录数
                cursor.execute("SELECT COUNT(*) FROM Content")
                record_count = cursor.fetchone()[0]
                
                if record_count == 0:
                    self.logger.info(f"  - Content表为空")
                    conn.close()
                    continue
                
                if self.preview_only:
                    self.logger.info(f"  [预览] Content表有 {record_count} 条记录，将要生成 {record_count} 个SKU")
                else:
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
                            self.logger.error(f"    更新记录 {record_id} 时出错: {e}")
                    
                    conn.commit()
                    self.logger.info(f"  ✓ 更新了 {updated_count} 条记录")
                    total_updated += updated_count
                
                conn.close()
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"❌ 处理失败：{e}")
            
            # 更新进度条
            if pbar:
                pbar.update(1)
        
        # 关闭进度条
        if pbar:
            pbar.close()
        
        self.logger.info(f"\nSKU生成完成！成功处理: {success_count}/{len(db_folders)} 个数据库")
        if not self.preview_only:
            self.logger.info(f"总共更新: {total_updated} 条记录")
            self.logger.info(f"生成的SKU数量: {len(self.used_skus)}")
        
        return success_count
   
    def read_categories(self) -> List[Tuple[int, str]]:
        """读取分类文件"""
        categories = []
        try:
            if not self.categories_file.exists():
                self.logger.error(f"错误: 分类文件 {self.categories_file} 不存在！")
                return []
            
            with open(self.categories_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # 跳过空行
                        categories.append((line_num, line))
            
            self.logger.info(f"✓ 成功读取分类文件，共 {len(categories)} 行")
            return categories
            
        except Exception as e:
            self.logger.error(f"读取分类文件时出错: {e}")
            return []
   
    def step4_batch_categories(self, db_folders: List[Path]) -> int:
        """步骤4：批量增加分类"""
        self.logger.info("\n" + "="*60)
        self.logger.info("步骤4：开始批量增加分类")
        self.logger.info("="*60)
        
        # 读取分类信息
        categories = self.read_categories()
        if not categories:
            return 0
        
        # 检查数量是否匹配
        if len(categories) != len(db_folders):
            self.logger.warning(f"警告: 分类数量 ({len(categories)}) 与数据库文件夹数量 ({len(db_folders)}) 不匹配！")
            self.logger.info("将使用较少的数量进行处理")
        
        # 确定处理数量
        process_count = min(len(categories), len(db_folders))
        
        success_count = 0
        total_updated = 0
        
        # 创建进度条
        if self.enable_progress_bar:
            if HAS_TQDM:
                pbar = tqdm(total=process_count, desc="步骤4: 批量增加分类", unit="个")
            else:
                pbar = SimpleProgressBar(process_count, "步骤4: 批量增加分类")
        else:
            pbar = None
        
        for i in range(process_count):
            folder = db_folders[i]
            line_num, category = categories[i]
            
            db_path = folder / self.db_filename
            self.logger.info(f"\n[{i+1}/{process_count}] 处理：{folder.name}/SpiderResult.db3")
            self.logger.info(f"  分类信息 (第{line_num}行): {category}")
            
            try:
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                
                # 获取所有表
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                
                if self.preview_only:
                    self.logger.info(f"  [预览] 将要更新此数据库的分类信息")
                else:
                    updated_count = 0
                    
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
                                updated_count += updated_rows
                                
                            except Exception as e:
                                self.logger.error(f"    更新列 {column} 时出错: {e}")
                    
                    conn.commit()
                    self.logger.info(f"  ✓ 更新了 {updated_count} 条记录")
                    total_updated += updated_count
                
                conn.close()
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"❌ 处理失败：{e}")
            
            # 更新进度条
            if pbar:
                pbar.update(1)
        
        # 关闭进度条
        if pbar:
            pbar.close()
        
        self.logger.info(f"\n批量分类更新完成！成功处理: {success_count}/{process_count} 个数据库")
        if not self.preview_only:
            self.logger.info(f"总共更新: {total_updated} 条记录")
        
        return success_count
    
    # ==================== 改造后的调度逻辑 ====================
    def run_processing(self):
        """执行完整的数据处理流程"""
        self.logger.info("=== 采集后的数据处理整合工具 ===")
        if self.preview_only:
            self.logger.info("(预览模式)")
        
        self.logger.info(f"基础文件夹: {self.base_folder}")
        self.logger.info(f"分类文件: {self.categories_file}")
        self.logger.info(f"数据库文件名: {self.db_filename}")
        self.logger.info(f"处理范围: 文件夹 {self.start_folder} 到 {self.end_folder}")
        self.logger.info(f"折扣率: {self.discount_rate}")
        self.logger.info(f"SKU前缀: {self.sku_prefix}")
        self.logger.info(f"SKU长度: {self.sku_length}")
        self.logger.info(f"日志开关: {'启用' if self.enable_logging else '禁用'}")
        self.logger.info(f"进度条开关: {'启用' if self.enable_progress_bar else '禁用'}")
        self.logger.info(f"功能开关: 去重={ENABLE_DEDUPLICATION}, 折扣价={ENABLE_DISCOUNT}, 随机SKU={ENABLE_RANDOM_SKU}, 批量分类={ENABLE_BATCH_CAT}")
        
        db_folders = self.find_database_folders()
        if not db_folders:
            self.logger.error("未找到符合条件的数据库文件夹！")
            return
        
        self.logger.info(f"\n开始处理，共 {len(db_folders)} 个数据库...")

        # 组装步骤
        steps = []
        if ENABLE_DEDUPLICATION: steps.append(("去重处理", self.step1_deduplication))
        if ENABLE_DISCOUNT:      steps.append(("折扣价处理", self.step2_discount_price))
        if ENABLE_RANDOM_SKU:    steps.append(("随机SKU", self.step3_random_sku))
        if ENABLE_BATCH_CAT:     steps.append(("批量分类", self.step4_batch_categories))

        # 总体进度条
        if self.enable_progress_bar:
            if HAS_TQDM:
                total_pbar = tqdm(total=len(steps), desc="总体进度", unit="步骤")
            else:
                total_pbar = SimpleProgressBar(len(steps), "总体进度")
        else:
            total_pbar = None

        results = {}
        for step_name, step_func in steps:
            self.logger.info(f"\n>>> 开始 {step_name}")
            success = step_func(db_folders)
            results[step_name] = success
            if total_pbar:
                total_pbar.update(1)

        if total_pbar:
            total_pbar.close()

        # 总结
        self.logger.info("\n" + "="*60)
        self.logger.info("数据处理完成！")
        self.logger.info("="*60)
        for step_name in ["去重处理", "折扣价处理", "随机SKU", "批量分类"]:
            if step_name in results:
                self.logger.info(f"{step_name}: {results[step_name]}/{len(db_folders)} 成功")
            else:
                self.logger.info(f"{step_name}: 跳过 (开关关闭)")

        if self.preview_only:
            self.logger.info("\n预览模式完成！请检查配置后设置 PREVIEW_ONLY = False 执行实际处理。")

def main():
    processor = DataProcessor()
    processor.run_processing()

if __name__ == "__main__":
    main()
