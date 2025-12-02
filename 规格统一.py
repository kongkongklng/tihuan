# 插入数据库生成统一的商品规格



# -*- coding: utf-8 -*-
import os
import sqlite3

# 固定规格目标（仅替换规格，不处理其他字段）
STANDARD_SPECS = "XS|||S|||M|||L|||XL"


def update_database(db_path):
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return False

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 确认 Content 表存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
        if cursor.fetchone() is None:
            print(f"数据库 {db_path} 缺少 Content 表")
            conn.close()
            return False

        # 检查是否存在 规格 列，不存在则添加
        cursor.execute("PRAGMA table_info(Content)")
        columns = [col[1] for col in cursor.fetchall()]
        if "规格" not in columns:
            cursor.execute("ALTER TABLE Content ADD COLUMN 规格 TEXT DEFAULT ''")
            conn.commit()

        # 仅更新 规格 列，不读取或依赖其他列
        cursor.execute("SELECT rowid FROM Content")
        rows = cursor.fetchall()

        updated = 0
        for (rowid,) in rows:
            cursor.execute(
                "UPDATE Content SET 规格 = ? WHERE rowid = ?",
                (STANDARD_SPECS, rowid),
            )
            updated += 1

        conn.commit()
        conn.close()
        print(f"已更新数据库: {db_path} (更新 {updated} 条)")
        return True

    except Exception as e:
        print(f"处理数据库 {db_path} 出错: {e}")
        return False


def batch_update(base_dir, start_num, end_num):
    print(f"开始处理目录: {base_dir}")
    print(f"处理范围: {start_num} - {end_num}")
    print(f"统一规格: {STANDARD_SPECS}")
    print("=" * 50)

    total = 0
    success = 0

    for folder in os.listdir(base_dir):
        if not folder.isdigit():
            continue
        folder_num = int(folder)
        if start_num <= folder_num <= end_num:
            total += 1
            db_path = os.path.join(base_dir, folder, "SpiderResult.db3")
            if update_database(db_path):
                success += 1

    print("=" * 50)
    print(f"处理完成: 成功 {success}/{total}")


if __name__ == "__main__":
    # 根据需要修改以下参数
    base_dir = r"D:\火车采集器V10.28\Data"
    start_num = 5140
    end_num = 5207

    batch_update(base_dir, start_num, end_num)
