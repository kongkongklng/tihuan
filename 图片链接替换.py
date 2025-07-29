import os
import sqlite3

# 设置你的根目录路径
base_dir = r"D:\火车采集器V10.28\Data"  # ← 修改成你的路径

# 遍历所有子文件夹
for folder in os.listdir(base_dir):
    # 确保是数字命名的文件夹
    if folder.isdigit():
        folder_num = int(folder)
        if 238 <= folder_num <= 367:
            folder_path = os.path.join(base_dir, folder)
            db_path = os.path.join(folder_path, "SpiderResult.db3")

            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    # 检查 Content 表是否存在
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
                    if cursor.fetchone():
                        cursor.execute("""
                            UPDATE Content
                            SET 图片 = REPLACE(图片, 'https:////', 'https://')
                            WHERE 图片 LIKE '%https:////%';
                        """)
                        conn.commit()
                        print(f"✅ 成功修改：{db_path}")
                    else:
                        print(f"⚠️ 跳过（无 Content 表）：{db_path}")

                    conn.close()
                except Exception as e:
                    print(f"❌ 处理失败：{db_path}，原因：{e}")
