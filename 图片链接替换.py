import os
import sqlite3
import shutil

# 设置你的根目录路径
base_dir = r"D:\火车采集器V10.28\Data"  # ← 修改成你的路径

def fix_image_field(img_str: str) -> str:
    """修复图片字段，给非 http 开头的部分加前缀"""
    if not img_str:
        return img_str
    parts = img_str.split("|||")
    fixed_parts = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if not p.startswith("http"):
            p = "https://www.amirl.top/" + p
        fixed_parts.append(p)
    return "|||".join(fixed_parts)
    
# 遍历所有子文件夹
for folder in os.listdir(base_dir):
    if folder.isdigit():
        folder_num = int(folder)
        if 3664 <= folder_num <= 3763:
            folder_path = os.path.join(base_dir, folder)
            db_path = os.path.join(folder_path, "SpiderResult.db3")

            if os.path.exists(db_path):
                try:
                    # 先备份
                    backup_path = db_path + ".bak"
                    if not os.path.exists(backup_path):
                        shutil.copy2(db_path, backup_path)
                        print(f"🛡 已备份：{backup_path}")
                    else:
                        print(f"⚠️ 已存在备份，跳过备份：{backup_path}")

                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    # 检查 Content 表是否存在
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content'")
                    if cursor.fetchone():
                        print(f"📂 数据库：{db_path}")

                        # 取出所有图片字段
                        cursor.execute("SELECT rowid, 图片 FROM Content")
                        rows = cursor.fetchall()

                        preview_count = 0
                        for rowid, img in rows:
                            new_img = fix_image_field(img)
                            if new_img != img:
                                cursor.execute("UPDATE Content SET 图片 = ? WHERE rowid = ?", (new_img, rowid))
                                if preview_count < 5:  # 预览前 5 条
                                    print(f"🔍 {img}  →  {new_img}")
                                    preview_count += 1

                        conn.commit()
                        print(f"✅ 成功修改：{db_path}")
                    else:
                        print(f"⚠️ 跳过（无 Content 表）：{db_path}")

                    conn.close()
                except Exception as e:
                    print(f"❌ 处理失败：{db_path}，原因：{e}")
