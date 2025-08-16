import os
import sqlite3

base_dir = r"D:\火车采集器V10.28\Data"

for folder in os.listdir(base_dir):
    if folder.isdigit():
        folder_num = int(folder)
        if 1283 <= folder_num <= 1452:
            folder_path = os.path.join(base_dir, folder)
            db_path = os.path.join(folder_path, "SpiderResult.db3")

            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
                    if cursor.fetchone():
                        cursor.execute("""
                            UPDATE Content
                            SET 销售价 = printf('%.2f', CAST(销售价 AS REAL) * 0.05),
                                折扣价 = printf('%.2f', CAST(销售价 AS REAL) * 0.05 * 0.3)
                            WHERE 销售价 GLOB '[0-9]*';
                        """)
                        conn.commit()
                        print(f"✅ 已更新销售价和折扣价：{db_path}")
                    else:
                        print(f"⚠️ 跳过（无 Content 表）：{db_path}")

                    conn.close()
                except Exception as e:
                    print(f"❌ 出错：{db_path}，原因：{e}")
