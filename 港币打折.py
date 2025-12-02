import os
import sqlite3
import random

# 设置目录路径（替换成你自己的路径）
base_dir = r"D:\火车采集器V10.28\Data"

# JPY -> USD 汇率（默认取 1 JPY = 0.00658236 USD）
# 如果你有更精确的实时汇率，请在这里替换
JPY_TO_USD = 1

for folder in os.listdir(base_dir):
    if folder.isdigit():
        folder_num = int(folder)
        if 3114 <= folder_num <= 3197:
            folder_path = os.path.join(base_dir, folder)
            db_path = os.path.join(folder_path, "SpiderResult.db3")

            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    # 检查 Content 表是否存在
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='Content'"
                    )
                    if cursor.fetchone():
                        # 读取所有有销售价的记录（假设 销售价 存的是日元）
                        cursor.execute("SELECT rowid, 销售价 FROM Content WHERE 销售价 IS NOT NULL")
                        rows = cursor.fetchall()

                        updated = 0
                        for rowid, price in rows:
                            try:
                                # 把日元价格转换为美元（保留两位小数）
                                price_jpy = float(price)
                                price_usd = price_jpy * JPY_TO_USD
                                price_usd_str = f"{price_usd:.2f}"

                                # 折扣价为销售价的 0.3 倍
                                discount_price = price_usd * 0.3
                                discount_price_str = f"{discount_price:.2f}"

                                # 更新 销售价（改为美元）与 折扣价（美元固定值）
                                cursor.execute(
                                    "UPDATE Content SET 销售价 = ?, 折扣价 = ? WHERE rowid = ?",
                                    (price_usd_str, discount_price_str, rowid),
                                )
                                updated += 1
                            except Exception as e:
                                # 某行解析失败则跳过该行并打印警告
                                print(f"⚠️ 跳过 rowid={rowid}，解析/更新出错：{e}")

                        conn.commit()
                        print(f"✅ 已更新 {db_path}，共 {updated} 行记录（销售价已换算为美元，折扣价为 0.3 倍）")

                        # 打印前 5 行检查
                        cursor.execute("SELECT 销售价, 折扣价 FROM Content LIMIT 5")
                        print(cursor.fetchall())
                    else:
                        print(f"⚠️ 跳过（无 Content 表）：{db_path}")

                    conn.close()
                except Exception as e:
                    print(f"❌ 出错：{db_path}，原因：{e}")
