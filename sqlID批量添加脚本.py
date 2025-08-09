import os
import sqlite3
import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple

# ================ 固定配置（按需修改）================
DB_PATH: str = r"D:\火车采集器V10.28\Configuration\config.db3"
START_JOB_ID: int = 913
END_JOB_ID: int = 1002
CATEGORY_FILE: str = r"D:\project\us.lounge\分类\output.txt"   # 每行一个分类/任务名
LINKS_DIR: str = r"D:\project\us.lounge\txt链接"  # 目录内放置txt
USE_FILE_PREFIX: bool = True      # 是否在路径前加 #FILE#
DRY_RUN: bool = False             # 预览/不落库
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


def list_txt_files(dir_path: str) -> List[str]:
    if not os.path.isdir(dir_path):
        raise NotADirectoryError(f"目录不存在: {dir_path}")
    files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.lower().endswith('.txt')]
    files.sort(key=lambda p: os.path.basename(p).lower())
    if not files:
        raise ValueError(f"目录 {dir_path} 中未找到任何 .txt 文件")
    return files


def fetch_job_by_id(conn: sqlite3.Connection, job_id: int) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT JobId, JobName, XmlData FROM Job WHERE JobId = ?", (job_id,))
    return cur.fetchone()


def update_job_record(conn: sqlite3.Connection, job_id: int, new_job_name: str, new_xml: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE Job SET JobName = ?, XmlData = ? WHERE JobId = ?", (new_job_name, new_xml, job_id))
    conn.commit()


def update_xml_fields(template_xml: str, new_job_name: str, txt_abs_path: str, use_file_prefix: bool) -> str:
    try:
        root = ET.fromstring(template_xml)
    except ET.ParseError as e:
        raise ValueError(f"XML解析失败: {e}")

    # 1) 仅修改根节点 JobName 属性
    root.set('JobName', new_job_name)

    # 2) 仅修改 <StartAddress> 文本
    start_node = root.find('StartAddress')
    if start_node is None:
        start_node = ET.Element('StartAddress')
        # 插入到首位，符合常见模板结构
        root.insert(0, start_node)
    start_node.text = ("#FILE#" + txt_abs_path) if use_file_prefix else txt_abs_path

    return ET.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')


def main():
    if START_JOB_ID > END_JOB_ID:
        raise ValueError("START_JOB_ID 不能大于 END_JOB_ID")

    job_count = END_JOB_ID - START_JOB_ID + 1
    categories = load_categories(CATEGORY_FILE)
    txt_files = list_txt_files(LINKS_DIR)

    if len(categories) < job_count:
        raise ValueError(f"分类数量不足：需要 {job_count} 行，实际 {len(categories)} 行")
    if len(txt_files) < job_count:
        raise ValueError(f"txt文件数量不足：需要 {job_count} 个，实际 {len(txt_files)} 个")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    processed = 0
    for index, job_id in enumerate(range(START_JOB_ID, END_JOB_ID + 1)):
        category = categories[index]
        txt_file = txt_files[index]
        txt_abs = os.path.abspath(txt_file)

        row = fetch_job_by_id(conn, job_id)
        if row is None:
            print(f"警告：JobId={job_id} 不存在，跳过")
            continue

        try:
            new_xml = update_xml_fields(row['XmlData'] or "<root></root>", category, txt_abs, USE_FILE_PREFIX)
        except Exception as e:
            print(f"JobId={job_id} 更新XML失败：{e}")
            continue

        if DRY_RUN:
            print(f"[DRY-RUN] JobId={job_id}")
            print(f"  原JobName: {row['JobName']}  -> 新JobName: {category}")
            print(f"  StartAddress: {(('#FILE#' if USE_FILE_PREFIX else '') + txt_abs)}")
            processed += 1
            continue

        update_job_record(conn, job_id, category, new_xml)
        processed += 1
        print(f"已更新 JobId={job_id}，JobName='{category}'，StartAddress= {(('#FILE#' if USE_FILE_PREFIX else '') + txt_abs)}")

    print(f"\n完成：共处理 {processed} 条（目标 {job_count} 条）")


if __name__ == "__main__":
    main()
