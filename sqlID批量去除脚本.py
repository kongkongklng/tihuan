import os
import shutil
import sqlite3
from typing import Tuple

# =============== 固定配置（按需修改）================
DB_PATH: str = r"D:\火车采集器V10.28\Configuration\config.db3"
START_JOB_ID: int = 610
END_JOB_ID: int = 809
DRY_RUN: bool = False              # 预览，不真正删除
BACKUP_BEFORE_DELETE: bool = False  # 删除前备份数据库
BACKUP_PATH: str = DB_PATH + ".bak"
# 关联清理（建议开启）：删除前先清理引用 JobId 的相关表
CLEAN_RELATED: bool = True
# ====================================================


def validate_range(start_id: int, end_id: int) -> Tuple[int, int]:
    if start_id <= 0 or end_id <= 0:
        raise ValueError("JobId 必须为正整数")
    if start_id > end_id:
        raise ValueError("START_JOB_ID 不能大于 END_JOB_ID")
    return start_id, end_id


def ensure_backup(db_path: str, backup_path: str) -> None:
    if not BACKUP_BEFORE_DELETE:
        return
    os.makedirs(os.path.dirname(backup_path), exist_ok=True) if os.path.dirname(backup_path) else None
    shutil.copy2(db_path, backup_path)
    print(f"已备份数据库到: {backup_path}")


def count_rows(conn: sqlite3.Connection, sql: str, params: Tuple) -> int:
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def delete_range(conn: sqlite3.Connection, table: str, where: str, params: Tuple) -> int:
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table} WHERE {where}", params)
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def main():
    start_id, end_id = validate_range(START_JOB_ID, END_JOB_ID)

    if not os.path.exists(DB_PATH):
        print(f"数据库不存在: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    # 统计将受影响的记录数
    to_delete_job = count_rows(conn, "SELECT COUNT(*) FROM Job WHERE JobId BETWEEN ? AND ?", (start_id, end_id))
    to_delete_jobdb = count_rows(conn, "SELECT COUNT(*) FROM JobDatabase WHERE JobId BETWEEN ? AND ?", (start_id, end_id))
    to_delete_jobweb = count_rows(conn, "SELECT COUNT(*) FROM JobWebPost WHERE JobId BETWEEN ? AND ?", (start_id, end_id))

    print(f"待删除范围: JobId {start_id}..{end_id}")
    print(f"Job            待删: {to_delete_job}")
    print(f"JobDatabase    待删: {to_delete_jobdb}")
    print(f"JobWebPost     待删: {to_delete_jobweb}")

    if DRY_RUN:
        print("DRY-RUN 预览结束：未执行删除")
        return

    ensure_backup(DB_PATH, BACKUP_PATH)

    total_deleted = 0
    # 先删除关联表记录（若开启）
    if CLEAN_RELATED:
        d_jobweb = delete_range(conn, "JobWebPost", "JobId BETWEEN ? AND ?", (start_id, end_id))
        d_jobdb = delete_range(conn, "JobDatabase", "JobId BETWEEN ? AND ?", (start_id, end_id))
        print(f"已删除 JobWebPost: {d_jobweb}")
        print(f"已删除 JobDatabase: {d_jobdb}")
        total_deleted += d_jobweb + d_jobdb

    # 删除主表 Job
    d_job = delete_range(conn, "Job", "JobId BETWEEN ? AND ?", (start_id, end_id))
    print(f"已删除 Job: {d_job}")
    total_deleted += d_job

    # 可选：VACUUM 回收空间（不必须）
    # conn.execute("VACUUM")
    # print("已执行 VACUUM")

    print(f"完成。总删除记录数（含关联表）: {total_deleted}")


if __name__ == "__main__":
    main()
