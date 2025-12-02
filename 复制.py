import os
import sqlite3

# 设置根目录路径（替换成你自己的路径）
base_dir = r"D:\火车采集器V10.28\Data"

# 设置文件夹范围
start_num = 2835
end_num = 2903

# 表名（修改成你的实际表名）
table_name = "Content"

for folder in os.listdir(base_dir):
    if folder.isdigit():  # 确保是数字文件夹
        folder_num = int(folder)
        if start_num <= folder_num <= end_num:
            db_path = os.path.join(base_dir, folder, "SpiderResult.db3")
            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    # 更新：颜色1 → 颜色
                    cursor.execute(f"""
                        UPDATE {table_name}
                        SET 颜色 = 颜色1
                        WHERE 颜色1 IS NOT NULL AND 颜色1 != '';
                    """)
                    
                    conn.commit()
                    conn.close()
                    print(f"[✔] 已更新: {db_path}")
                except Exception as e:
                    print(f"[✘] 处理失败 {db_path}: {e}")
