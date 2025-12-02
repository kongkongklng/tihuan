import os
import sqlite3
import re

# ==============================
# 改进的颜色/规格处理函数
# ==============================
size_pattern = re.compile(r"(?:^|[\s/])([0-9]+X|X{1,3}S?|S|M|L|OS)(?:$|[\s|])", re.IGNORECASE)

def process_colors(color_str):
    if not color_str:
        return "", ""

    parts = [p.strip() for p in color_str.split("|||") if p.strip()]

    colors = []
    specs = []

    for part in parts:
        match = size_pattern.search(part)
        if match:
            specs.append(match.group(1).upper())
            # 去掉尺寸后，保留纯颜色
            color = size_pattern.sub("", part).strip(" /")
        else:
            color = part

        if color:
            colors.append(color)

    # 去重（保持顺序）
    colors = list(dict.fromkeys(colors))
    specs = list(dict.fromkeys(specs))

    return "|||".join(colors), "|||".join(specs)


# ==============================
# 主程序：批量更新数据库
# ==============================
base_dir = r"D:\火车采集器V10.28\Data"  # <<< 修改为你的目录
start_num = 2835  # <<< 修改开始文件夹号
end_num = 2903    # <<< 修改结束文件夹号

for folder in os.listdir(base_dir):
    if folder.isdigit():
        folder_num = int(folder)
        if start_num <= folder_num <= end_num:
            db_path = os.path.join(base_dir, folder, "SpiderResult.db3")

            if not os.path.exists(db_path):
                print(f"❌ 数据库不存在: {db_path}")
                continue

            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()  

                # 检查字段是否存在
                cursor.execute("PRAGMA table_info(Content)")
                columns = [col[1] for col in cursor.fetchall()]
                if not {"颜色1", "颜色", "规格"}.issubset(columns):
                    print(f"⚠️ 数据库 {db_path} 缺少必要字段")
                    conn.close()
                    continue

                # 读取并处理数据
                cursor.execute("SELECT rowid, 颜色1 FROM Content")
                rows = cursor.fetchall()

                for rowid, color1 in rows:
                    new_color, new_spec = process_colors(color1)
                    cursor.execute(
                        "UPDATE Content SET 颜色 = ?, 规格 = ? WHERE rowid = ?",
                        (new_color, new_spec, rowid)
                    )

                conn.commit()
                conn.close()
                print(f"✅ 已更新数据库: {db_path}")

            except Exception as e:
                print(f"❌ 处理数据库 {db_path} 出错: {e}")
