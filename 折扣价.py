import os
import sqlite3

# 设置根目录路径（替换成你自己的路径）
base_dir = r"D:\火车采集器V10.28\Data"

# 遍历文件夹 236 到 237
for folder in os.listdir(base_dir):
    if folder.isdigit():
        folder_num = int(folder)
        if 449 <= folder_num <= 491:
            folder_path = os.path.join(base_dir, folder)
            db_path = os.path.join(folder_path, "SpiderResult.db3")

            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    # 检查是否有 Content 表和 销售价/折扣价字段
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
                    if cursor.fetchone():
                        # 将 折扣价 更新为 销售价 × 0.35（转换为数字再计算）
                        cursor.execute("""
                            UPDATE Content
                            SET 折扣价 = printf('%.2f', CAST(销售价 AS REAL) * 0.3)
                            WHERE 销售价 GLOB '[0-9]*';  -- 只处理纯数字价格
                        """)
                        conn.commit()
                        print(f"✅ 已更新折扣价：{db_path}")
                    else:
                        print(f"⚠️ 跳过（无 Content 表）：{db_path}")

                    conn.close()
                except Exception as e:
                    print(f"❌ 出错：{db_path}，原因：{e}")
