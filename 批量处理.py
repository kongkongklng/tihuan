import os
import sqlite3
import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple
import re
import logging
import sys
from datetime import datetime
import traceback

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

# ================ 固定配置（按需修改）================
DB_PATH: str = r"D:\火车采集器V10.28\Configuration\config.db3"## 固定
START_JOB_ID: int = 2096
END_JOB_ID: int = 2185
CATEGORY_FILE: str = r"D:\project\vuoriclothing\分类\分类.txt"   # 每行一个分类/任务名
CATEGORY_LINKS_FILE: str = r"D:\project\vuoriclothing\分类\分类+链接.txt"   # 分类名|||链接 格式
LINKS_DIR: str = r"D:\project\vuoriclothing\链接"  # 目录内放置txt（当 START_ADDRESS_USE_CATEGORY=False 时使用）
USE_FILE_PREFIX: bool = True      # 是否在路径前加 #FILE#（当 START_ADDRESS_USE_CATEGORY=False 时生效）
START_ADDRESS_USE_CATEGORY: bool = False  # True: <StartAddress> 使用分类名；False: 使用 txt 绝对路径
DRY_RUN: bool = False              # 预览/不落库

# 日志开关
ENABLE_LOGGING: bool = True  # True=启用日志，False=禁用日志
LOG_LEVEL: str = "INFO"      # 日志级别：DEBUG, INFO, WARNING, ERROR

# 进度条开关
ENABLE_PROGRESS_BAR: bool = True  # True=启用进度条，False=禁用进度条
# ====================================================

# 配置日志
def setup_logging():
    """设置日志配置"""
    if not ENABLE_LOGGING:
        # 禁用日志
        logging.getLogger().disabled = True
        return logging.getLogger(__name__)
        
    log_filename = f"批量添加_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # 设置日志级别
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class ConfigError(Exception):
    """配置错误异常"""
    pass

class DatabaseError(Exception):
    """数据库操作错误异常"""
    pass

class FileOperationError(Exception):
    """文件操作错误异常"""
    pass

def validate_config():
    """验证配置参数"""
    errors = []
    
    # 验证数据库路径
    if not os.path.exists(DB_PATH):
        errors.append(f"数据库文件不存在: {DB_PATH}")
    
    # 验证Job ID范围
    if START_JOB_ID <= 0 or END_JOB_ID <= 0:
        errors.append("Job ID 必须大于0")
    if START_JOB_ID > END_JOB_ID:
        errors.append("START_JOB_ID 不能大于 END_JOB_ID")
    
    # 验证分类文件
    if not os.path.exists(CATEGORY_FILE):
        errors.append(f"分类文件不存在: {CATEGORY_FILE}")
    
    # 验证分类+链接文件（可选）
    if not os.path.exists(CATEGORY_LINKS_FILE):
        logger.warning(f"分类+链接文件不存在: {CATEGORY_LINKS_FILE}，将不会输出失败分类的链接")
    
    # 验证链接目录
    if not START_ADDRESS_USE_CATEGORY and not os.path.exists(LINKS_DIR):
        errors.append(f"链接目录不存在: {LINKS_DIR}")
    
    if errors:
        raise ConfigError("配置验证失败:\n" + "\n".join(f"- {error}" for error in errors))

def safe_connect_db(db_path: str) -> sqlite3.Connection:
    """安全连接数据库"""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        # 测试连接
        conn.execute("SELECT 1")
        logger.info(f"成功连接到数据库: {db_path}")
        return conn
    except sqlite3.Error as e:
        raise DatabaseError(f"数据库连接失败: {e}")
    except Exception as e:
        raise DatabaseError(f"未知数据库错误: {e}")

def load_categories(path: str) -> List[str]:
    """安全加载分类文件"""
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"分类文件不存在: {path}")
        
        cats: List[str] = []
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                cats.append(line)
        
        if not cats:
            raise ValueError("分类文件为空")
        
        logger.info(f"成功加载 {len(cats)} 个分类")
        return cats
    except UnicodeDecodeError as e:
        raise FileOperationError(f"文件编码错误: {e}")
    except Exception as e:
        raise FileOperationError(f"读取分类文件失败: {e}")

def load_category_links(path: str) -> dict:
    """安全加载分类+链接文件
    返回: {分类名: 链接, ...}
    """
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"分类+链接文件不存在: {path}")
        
        category_links = {}
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                # 解析格式：分类名|||链接
                parts = line.split('|||')
                if len(parts) >= 2:
                    # 分类名是除了最后一个部分（链接）之外的所有部分
                    category = '|||'.join(parts[:-1])
                    link = parts[-1]  # 取最后一个部分作为链接
                    category_links[category] = link
                else:
                    logger.warning(f"第{line_num}行格式错误: {line}")
        
        if not category_links:
            raise ValueError("分类+链接文件为空")
        
        logger.info(f"成功加载 {len(category_links)} 个分类链接")
        return category_links
    except UnicodeDecodeError as e:
        raise FileOperationError(f"文件编码错误: {e}")
    except Exception as e:
        raise FileOperationError(f"读取分类+链接文件失败: {e}")

def _natural_key(name: str):
    # 自然排序键：数字按数值比较、字母大小写不敏感
    return [int(s) if s.isdigit() else s.lower() for s in re.split(r"(\d+)", name)]

def list_txt_files(dir_path: str) -> List[str]:
    """安全列出txt文件"""
    try:
        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"目录不存在: {dir_path}")
        
        names = [f for f in os.listdir(dir_path) if f.lower().endswith('.txt')]
        names.sort(key=_natural_key)  # 与资源管理器"名称 递增"一致
        files = [os.path.join(dir_path, f) for f in names]
        
        if not files:
            raise ValueError(f"目录 {dir_path} 中未找到任何 .txt 文件")
        
        logger.info(f"在目录 {dir_path} 中找到 {len(files)} 个txt文件")
        return files
    except PermissionError as e:
        raise FileOperationError(f"权限不足，无法访问目录: {e}")
    except Exception as e:
        raise FileOperationError(f"列出txt文件失败: {e}")

def _cat_to_filename_base(cat: str) -> str:
    """将分类名转换为期望的文件名（不含扩展名）的基名。
    规则：将 '|||' 替换为 '___'。
    """
    return cat.replace('|||', '___')

def _build_category_to_txt_map(categories: List[str], dir_path: str) -> Tuple[dict, List[Tuple[str, str]]]:
    """将分类名与同名（或按规则转换后的同名）的 txt 文件一一匹配。
    返回: (匹配映射字典, 未匹配的分类列表)
    """
    try:
        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"目录不存在: {dir_path}")

        # 以文件名为键，值为绝对路径
        file_name_to_path = {}
        for name in os.listdir(dir_path):
            if not name.lower().endswith('.txt'):
                continue
            base = os.path.splitext(name)[0]
            file_name_to_path[base] = os.path.abspath(os.path.join(dir_path, name))

        mapping = {}
        missing = []
        for cat in categories:
            transformed = _cat_to_filename_base(cat)
            if cat in file_name_to_path:
                mapping[cat] = file_name_to_path[cat]
            elif transformed in file_name_to_path:
                mapping[cat] = file_name_to_path[transformed]
            else:
                missing.append((cat, transformed))

        if missing:
            hint = "\n".join([f" ___分类: {m[0]}  期望文件名: {m[1]}.txt" for m in missing])
            logger.warning(f"以下分类未找到匹配的 txt 文件:\n{hint}")
            # 不抛出异常，而是记录警告并继续

        logger.info(f"成功匹配 {len(mapping)} 个分类到txt文件")
        return mapping, missing
    except Exception as e:
        raise FileOperationError(f"构建分类到txt文件映射失败: {e}")

def fetch_job_by_id(conn: sqlite3.Connection, job_id: int) -> Optional[sqlite3.Row]:
    """安全获取Job记录"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT JobId, JobName, XmlData FROM Job WHERE JobId = ?", (job_id,))
        result = cur.fetchone()
        if result is None:
            logger.warning(f"JobId={job_id} 不存在")
        return result
    except sqlite3.Error as e:
        logger.error(f"查询JobId={job_id}失败: {e}")
        raise DatabaseError(f"数据库查询失败: {e}")

def update_job_record(conn: sqlite3.Connection, job_id: int, new_job_name: str, new_xml: str) -> bool:
    """安全更新Job记录"""
    try:
        cur = conn.cursor()
        cur.execute("UPDATE Job SET JobName = ?, XmlData = ? WHERE JobId = ?", (new_job_name, new_xml, job_id))
        
        if cur.rowcount == 0:
            logger.warning(f"JobId={job_id} 更新失败，可能记录不存在")
            return False
        
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"更新JobId={job_id}失败: {e}")
        conn.rollback()
        raise DatabaseError(f"数据库更新失败: {e}")

def update_xml_fields(template_xml: str, new_job_name: str, txt_abs_path: str, *, use_file_prefix: bool, use_category_for_start: bool) -> str:
    """安全更新XML字段"""
    try:
        # 确保XML不为空
        if not template_xml or template_xml.strip() == "":
            template_xml = "<root></root>"
        
        root = ET.fromstring(template_xml)
    except ET.ParseError as e:
        logger.error(f"XML解析失败: {e}")
        # 尝试使用默认XML
        try:
            root = ET.fromstring("<root></root>")
            logger.info("使用默认XML模板")
        except ET.ParseError:
            raise ValueError("无法解析XML，且默认模板也失败")

    try:
        # 1) 仅修改根节点 JobName 属性
        root.set('JobName', new_job_name)

        # 2) 修改 <StartAddress> 文本
        start_node = root.find('StartAddress')
        if start_node is None:
            start_node = ET.Element('StartAddress')
            # 插入到首位，符合常见模板结构
            root.insert(0, start_node)

        if use_category_for_start:
            start_node.text = new_job_name
        else:
            start_node.text = ("#FILE#" + txt_abs_path) if use_file_prefix else txt_abs_path

        return ET.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
    except Exception as e:
        logger.error(f"更新XML字段失败: {e}")
        raise ValueError(f"XML字段更新失败: {e}")

def list_unmatched_categories(categories: List[str], dir_path: str) -> List[Tuple[str, str]]:
    """列出没有匹配到txt文件的分类
    返回: [(分类名, 期望文件名), ...]
    """
    try:
        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"目录不存在: {dir_path}")

        # 获取所有txt文件名（不含扩展名）
        txt_files = set()
        for name in os.listdir(dir_path):
            if name.lower().endswith('.txt'):
                base = os.path.splitext(name)[0]
                txt_files.add(base)

        unmatched = []
        for cat in categories:
            transformed = _cat_to_filename_base(cat)
            if cat not in txt_files and transformed not in txt_files:
                unmatched.append((cat, transformed))

        return unmatched
    except Exception as e:
        logger.error(f"列出未匹配分类时出错: {e}")
        return []

def list_unmatched_categories_with_progress(categories: List[str], dir_path: str, pbar) -> List[Tuple[str, str]]:
    """列出没有匹配到txt文件的分类（带进度条）
    返回: [(分类名, 期望文件名), ...]
    """
    try:
        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"目录不存在: {dir_path}")

        # 获取所有txt文件名（不含扩展名）
        txt_files = set()
        for name in os.listdir(dir_path):
            if name.lower().endswith('.txt'):
                base = os.path.splitext(name)[0]
                txt_files.add(base)

        unmatched = []
        for cat in categories:
            transformed = _cat_to_filename_base(cat)
            if cat not in txt_files and transformed not in txt_files:
                unmatched.append((cat, transformed))
            if pbar:
                pbar.update(1)

        return unmatched
    except Exception as e:
        logger.error(f"列出未匹配分类时出错: {e}")
        return []

def show_unmatched_categories():
    """独立函数：显示未匹配的分类"""
    try:
        logger.info("开始检查未匹配的分类...")
        
        # 验证配置
        if not os.path.exists(CATEGORY_FILE):
            logger.error(f"分类文件不存在: {CATEGORY_FILE}")
            return
        
        if not os.path.exists(LINKS_DIR):
            logger.error(f"链接目录不存在: {LINKS_DIR}")
            return
        
        # 加载分类
        categories = load_categories(CATEGORY_FILE)
        
        # 创建进度条
        if ENABLE_PROGRESS_BAR:
            if HAS_TQDM:
                pbar = tqdm(total=len(categories), desc="检查分类匹配", unit="个")
            else:
                pbar = SimpleProgressBar(len(categories), "检查分类匹配")
        else:
            pbar = None
        
        # 获取未匹配的分类
        unmatched = list_unmatched_categories_with_progress(categories, LINKS_DIR, pbar)
        
        if not unmatched:
            logger.info("所有分类都已匹配到对应的txt文件！")
            return
        
        # 显示未匹配的分类
        logger.info("=" * 60)
        logger.info("未匹配的分类列表:")
        logger.info("=" * 60)
        for i, (cat, expected_file) in enumerate(unmatched, 1):
            logger.info(f"{i:3d}. 分类: {cat}")
            logger.info(f"     期望文件名: {expected_file}.txt")
            logger.info("-" * 40)
        logger.info(f"总计: {len(unmatched)} 个分类未匹配")
        logger.info("=" * 60)
        
        # 保存到文件
        output_file = f"未匹配分类_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"未匹配分类列表 - 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 60 + "\n")
            for i, (cat, expected_file) in enumerate(unmatched, 1):
                f.write(f"{i:3d}. 分类: {cat}\n")
                f.write(f"     期望文件名: {expected_file}.txt\n")
                f.write("-" * 40 + "\n")
            f.write(f"总计: {len(unmatched)} 个分类未匹配\n")
        
        logger.info(f"未匹配分类列表已保存到: {output_file}")
        
        # 关闭进度条
        if pbar:
            pbar.close()
        
    except Exception as e:
        logger.error(f"检查未匹配分类时出错: {e}")
        logger.error(traceback.format_exc())

def main():
    """主函数"""
    try:
        logger.info("=== 批量添加工具 ===")
        if DRY_RUN:
            logger.info("(预览模式)")
        
        logger.info(f"数据库路径: {DB_PATH}")
        logger.info(f"任务范围: JobId {START_JOB_ID} 到 {END_JOB_ID}")
        logger.info(f"分类文件: {CATEGORY_FILE}")
        logger.info(f"分类链接文件: {CATEGORY_LINKS_FILE}")
        logger.info(f"链接目录: {LINKS_DIR}")
        logger.info(f"日志开关: {'启用' if ENABLE_LOGGING else '禁用'}")
        logger.info(f"进度条开关: {'启用' if ENABLE_PROGRESS_BAR else '禁用'}")
        
        logger.info("开始执行批量添加任务")
        
        # 验证配置
        validate_config()
        
        job_count = END_JOB_ID - START_JOB_ID + 1
        logger.info(f"任务范围: JobId {START_JOB_ID} 到 {END_JOB_ID}，共 {job_count} 个任务")
        
        # 加载分类
        categories = load_categories(CATEGORY_FILE)
        
        # 加载分类+链接文件
        category_links = {}
        try:
            category_links = load_category_links(CATEGORY_LINKS_FILE)
        except Exception as e:
            logger.warning(f"加载分类+链接文件失败: {e}，将不会输出失败分类的链接")

        # 当使用 txt 作为 <StartAddress> 时，采用"同名或'|||'->'___'"的映射
        category_to_txt = {}
        unmatched_categories = []
        if not START_ADDRESS_USE_CATEGORY:
            category_to_txt, unmatched_categories = _build_category_to_txt_map(categories, LINKS_DIR)
            
            # 显示未匹配的分类
            if unmatched_categories:
                logger.info("=" * 60)
                logger.info("未匹配的分类列表:")
                logger.info("=" * 60)
                for i, (cat, expected_file) in enumerate(unmatched_categories, 1):
                    logger.info(f"{i:3d}. 分类: {cat}")
                    logger.info(f"     期望文件名: {expected_file}.txt")
                    logger.info("-" * 40)
                logger.info(f"总计: {len(unmatched_categories)} 个分类未匹配")
                logger.info("=" * 60)

        if len(categories) < job_count:
            logger.warning(f"分类数量不足：需要 {job_count} 行，实际 {len(categories)} 行")
            job_count = len(categories)  # 调整处理数量
        
        if not START_ADDRESS_USE_CATEGORY and len(category_to_txt) < job_count:
            logger.warning(f"匹配到的 txt 文件数量不足：需要 {job_count} 个，实际 {len(category_to_txt)} 个")
            job_count = len(category_to_txt)  # 调整处理数量

        # 连接数据库
        conn = safe_connect_db(DB_PATH)

        processed = 0
        failed = 0
        skipped = 0
        failed_categories = []  # 记录失败的分类
        
        # 将未匹配的分类也记录为失败分类
        if unmatched_categories:
            for cat, _ in unmatched_categories:
                failed_categories.append(cat)
                failed += 1
        
        # 创建进度条（只处理有匹配txt文件的分类）
        actual_job_count = job_count - len(unmatched_categories) if unmatched_categories else job_count
        if ENABLE_PROGRESS_BAR:
            if HAS_TQDM:
                pbar = tqdm(total=actual_job_count, desc="批量添加进度", unit="个")
            else:
                pbar = SimpleProgressBar(actual_job_count, "批量添加进度")
        else:
            pbar = None
        
        for index, job_id in enumerate(range(START_JOB_ID, START_JOB_ID + job_count)):
            try:
                category = categories[index]
                
                # 跳过未匹配的分类（已经在前面记录为失败）
                if not START_ADDRESS_USE_CATEGORY and category not in category_to_txt:
                    logger.warning(f"分类 '{category}' 没有对应的txt文件，跳过")
                    skipped += 1
                    continue
                
                if START_ADDRESS_USE_CATEGORY:
                    txt_abs = ''
                    start_addr_display = category
                else:
                    txt_abs = category_to_txt[category]
                    start_addr_display = (("#FILE#" if USE_FILE_PREFIX else '') + txt_abs)

                row = fetch_job_by_id(conn, job_id)
                if row is None:
                    logger.warning(f"JobId={job_id} 不存在，跳过")
                    skipped += 1
                    continue

                try:
                    new_xml = update_xml_fields(
                        row['XmlData'] or "<root></root>",
                        category,
                        txt_abs,
                        use_file_prefix=USE_FILE_PREFIX,
                        use_category_for_start=START_ADDRESS_USE_CATEGORY,
                    )
                except Exception as e:
                    logger.error(f"JobId={job_id} 更新XML失败：{e}")
                    failed += 1
                    failed_categories.append(category)
                    continue

                if DRY_RUN:
                    logger.info(f"[DRY-RUN] JobId={job_id}")
                    logger.info(f"  原JobName: {row['JobName']}  -> 新JobName: {category}")
                    logger.info(f"  StartAddress: {start_addr_display}")
                    processed += 1
                    continue

                if update_job_record(conn, job_id, category, new_xml):
                    processed += 1
                    logger.info(f"已更新 JobId={job_id}，JobName='{category}'，StartAddress= {start_addr_display}")
                else:
                    failed += 1
                    failed_categories.append(category)
                
                # 更新进度条
                if pbar:
                    pbar.update(1)
                    
            except Exception as e:
                logger.error(f"处理JobId={job_id}时发生错误: {e}")
                logger.error(traceback.format_exc())
                failed += 1
                failed_categories.append(category)
                if pbar:
                    pbar.update(1)  # 即使失败也要更新进度
                continue

        # 关闭进度条
        if pbar:
            pbar.close()
        
        logger.info(f"\n完成：共处理 {processed} 条，失败 {failed} 条，跳过 {skipped} 条（目标 {job_count} 条）")
        
        # 调试信息
        logger.info(f"失败分类列表: {failed_categories}")
        logger.info(f"分类链接数量: {len(category_links)}")
        logger.info(f"未匹配分类数量: {len(unmatched_categories) if unmatched_categories else 0}")
        
        if failed > 0:
            logger.warning(f"有 {failed} 个任务处理失败，请检查日志")
            
            # 输出失败分类的链接
            if category_links and failed_categories:
                output_failed_file = f"失败分类链接_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                try:
                    with open(output_failed_file, 'w', encoding='utf-8') as f:
                        f.write(f"失败分类链接列表 - 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        f.write(f"总计失败: {len(failed_categories)} 个分类\n")
                        f.write("=" * 60 + "\n\n")
                        
                        for category in failed_categories:
                            link = category_links.get(category, "未找到对应链接")
                            f.write(f"{category}|||{link}\n")
                        
                        f.write(f"\n总计: {len(failed_categories)} 个失败分类\n")
                    
                    logger.info(f"失败分类链接已保存到: {output_failed_file}")
                except Exception as e:
                    logger.error(f"保存失败分类链接文件时出错: {e}")
            else:
                if not category_links:
                    logger.warning("没有加载到分类链接数据，无法生成失败分类链接文件")
                if not failed_categories:
                    logger.info("没有失败分类，无需生成失败分类链接文件")
        
    except ConfigError as e:
        logger.error(f"配置错误: {e}")
        sys.exit(1)
    except DatabaseError as e:
        logger.error(f"数据库错误: {e}")
        sys.exit(1)
    except FileOperationError as e:
        logger.error(f"文件操作错误: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"未知错误: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        try:
            if 'conn' in locals():
                conn.close()
                logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {e}")

if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "--list-unmatched":
            # 只显示未匹配的分类
            show_unmatched_categories()
        elif sys.argv[1] == "--test-links":
            # 测试分类+链接文件加载
            try:
                logger.info("测试分类+链接文件加载...")
                category_links = load_category_links(CATEGORY_LINKS_FILE)
                logger.info(f"成功加载 {len(category_links)} 个分类链接")
                logger.info("前5个分类链接示例:")
                for i, (category, link) in enumerate(list(category_links.items())[:5], 1):
                    logger.info(f"{i}. {category} -> {link}")
            except Exception as e:
                logger.error(f"测试失败: {e}")
        else:
            # 执行正常的批量添加任务
            main()
    else:
        # 执行正常的批量添加任务
        main()
