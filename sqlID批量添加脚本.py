import os
import sqlite3
import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple
import re

# ================ 固定配置（按需修改）================
DB_PATH: str = r"D:\火车采集器V10.28\Configuration\config.db3"
START_JOB_ID: int = 1893
END_JOB_ID: int = 2094
CATEGORY_FILE: str = r"D:\project\adameve\分类\分类.txt"   # 每行一个分类/任务名
LINKS_DIR: str = r"D:\project\adameve\链接"  # 目录内放置txt（当 START_ADDRESS_USE_CATEGORY=False 时使用）
USE_FILE_PREFIX: bool = True      # 是否在路径前加 #FILE#（当 START_ADDRESS_USE_CATEGORY=False 时生效）
START_ADDRESS_USE_CATEGORY: bool = False  # True: <StartAddress> 使用分类名；False: 使用 txt 绝对路径
DRY_RUN: bool = False              # 预览/不落库
# ====================================================


def load_categories(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"分类文件不存在: {path}")
    cats: List[str] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            cats.append(line)
    if not cats:
        raise ValueError("分类文件为空")
    return cats


def _natural_key(name: str):
    # 自然排序键：数字按数值比较、字母大小写不敏感
    return [int(s) if s.isdigit() else s.lower() for s in re.split(r"(\d+)", name)]


def list_txt_files(dir_path: str) -> List[str]:
    if not os.path.isdir(dir_path):
        raise NotADirectoryError(f"目录不存在: {dir_path}")
    names = [f for f in os.listdir(dir_path) if f.lower().endswith('.txt')]
    names.sort(key=_natural_key)  # 与资源管理器“名称 递增”一致
    files = [os.path.join(dir_path, f) for f in names]
    if not files:
        raise ValueError(f"目录 {dir_path} 中未找到任何 .txt 文件")
    return files


def _cat_to_filename_base(cat: str) -> str:
    """将分类名转换为期望的文件名（不含扩展名）的基名。
    规则：将 '|||' 替换为 '___'。
    """
    return cat.replace('|||', '___')


def _build_category_to_txt_map(categories: List[str], dir_path: str) -> dict:
    """将分类名与同名（或按规则转换后的同名）的 txt 文件一一匹配。
    匹配优先级：
      1) 文件名(不含扩展名) == 分类名
      2) 文件名(不含扩展名) == 将分类名中的 '|||' 替换为 '___'
    均为区分大小写的精确匹配。
    """
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
        raise ValueError(
            "以下分类未找到匹配的 txt 文件（需与分类同名或将 '|||' 替换为 '___'）：\n" + hint
        )

    return mapping


def fetch_job_by_id(conn: sqlite3.Connection, job_id: int) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT JobId, JobName, XmlData FROM Job WHERE JobId = ?", (job_id,))
    return cur.fetchone()


def update_job_record(conn: sqlite3.Connection, job_id: int, new_job_name: str, new_xml: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE Job SET JobName = ?, XmlData = ? WHERE JobId = ?", (new_job_name, new_xml, job_id))
    conn.commit()


def update_xml_fields(template_xml: str, new_job_name: str, txt_abs_path: str, *, use_file_prefix: bool, use_category_for_start: bool) -> str:
    try:
        root = ET.fromstring(template_xml)
    except ET.ParseError as e:
        raise ValueError(f"XML解析失败: {e}")

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


def main():
    if START_JOB_ID > END_JOB_ID:
        raise ValueError("START_JOB_ID 不能大于 END_JOB_ID")

    job_count = END_JOB_ID - START_JOB_ID + 1
    categories = load_categories(CATEGORY_FILE)

    # 当使用 txt 作为 <StartAddress> 时，采用“同名或'|||'->'___'”的映射
    category_to_txt = {}
    if not START_ADDRESS_USE_CATEGORY:
        category_to_txt = _build_category_to_txt_map(categories, LINKS_DIR)

    if len(categories) < job_count:
        raise ValueError(f"分类数量不足：需要 {job_count} 行，实际 {len(categories)} 行")
    if not START_ADDRESS_USE_CATEGORY and len(category_to_txt) < job_count:
        raise ValueError(f"匹配到的 txt 文件数量不足：需要 {job_count} 个，实际 {len(category_to_txt)} 个")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    processed = 0
    for index, job_id in enumerate(range(START_JOB_ID, END_JOB_ID + 1)):
        category = categories[index]
        if START_ADDRESS_USE_CATEGORY:
            txt_abs = ''
            start_addr_display = category
        else:
            txt_abs = category_to_txt[category]
            start_addr_display = (("#FILE#" if USE_FILE_PREFIX else '') + txt_abs)

        row = fetch_job_by_id(conn, job_id)
        if row is None:
            print(f"警告：JobId={job_id} 不存在，跳过")
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
            print(f"JobId={job_id} 更新XML失败：{e}")
            continue

        if DRY_RUN:
            print(f"[DRY-RUN] JobId={job_id}")
            print(f"  原JobName: {row['JobName']}  -> 新JobName: {category}")
            print(f"  StartAddress: {start_addr_display}")
            processed += 1
            continue

        update_job_record(conn, job_id, category, new_xml)
        processed += 1
        print(f"已更新 JobId={job_id}，JobName='{category}'，StartAddress= {start_addr_display}")

    print(f"\n完成：共处理 {processed} 条（目标 {job_count} 条）")


if __name__ == "__main__":
    main()
