import os
import sqlite3
import shutil

# 设置总目录路径
base_dir = r"D:\火车采集器V10.28\Data"  # <<< 替换为你的路径

# 设置文件夹数字范围（含）
start_num = 517
end_num = 606

# 遍历文件夹
for folder_name in os.listdir(base_dir):
    if folder_name.isdigit():
        folder_num = int(folder_name)
        if start_num <= folder_num <= end_num:
            folder_path = os.path.join(base_dir, folder_name)
            db_path = os.path.join(folder_path, "SpiderResult.db3")

            if os.path.isfile(db_path):
                print(f"\n📂 正在处理：{folder_name}/SpiderResult.db3")
                backup_path = os.path.join(folder_path, "SpiderResult_backup.db3")

                # 备份数据库文件
                if not os.path.exists(backup_path):
                    shutil.copyfile(db_path, backup_path)
                    print("✅ 已备份为 SpiderResult_backup.db3")
                else:
                    print("⚠️ 备份已存在，跳过备份")

                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    # 备份 Content 表
                    cursor.execute("DROP TABLE IF EXISTS Content_backup")
                    cursor.execute("CREATE TABLE Content_backup AS SELECT * FROM Content")
                    print("✅ 已创建 Content_backup 表")

                    # 删除重复 SKU，仅保留 ID 最小的那条
                    cursor.execute("""
                        DELETE FROM Content
                        WHERE ID NOT IN (
                            SELECT MIN(ID)
                            FROM Content
                            GROUP BY SKU
                        )
                    """)
                    conn.commit()
                    conn.close()
                    print("✅ 去重完成")
                except Exception as e:
                    print(f"❌ 处理失败：{e}")
            else:
                print(f"❌ 未找到文件：{db_path}")
