#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批量从各子文件夹里的 SpiderResult.db3 (Content 表) 发布商品到 WooCommerce 的脚本
支持字段开关、图片本地上传或远程 URL、分类层级创建、发布后回写已发 和 PageUrl
"""

import os
import sqlite3
import requests
from requests.auth import HTTPBasicAuth
import time
from multiprocessing import Pool

# ========== 配置区（请根据实际修改） ==========
ROOT_DIR = r"D:\火车采集器V10.28\Data"  # 顶层文件夹，里面包含 0001/ 0002/ ...

# 可选：限制处理的子文件夹范围（包含边界）。
# 例如：START_FOLDER = "0003"，END_FOLDER = "0010" 只处理 0003~0010。
# 设为 None 表示不限制。
START_FOLDER = 5211
END_FOLDER = 5522

# 是否在处理前清空每条记录的“已发”状态（慎用：会导致所有记录重新上传）
RESET_SENT_BEFORE_UPLOAD = True

# 是否启用多进程按子文件夹并行上传
USE_MULTIPROCESS = True
# 进程数量（建议 2~4，根据服务器性能调整）
MAX_PROCESSES = 4
DOMAIN = "https://www.ottarbox.club"   # 你的站点
KEY = "ck_9aabc242379aa59212bd389dd98defad0fd9fc19"
SECRET = "cs_67adb16ea2138e00bf0f173bb189ba0a1cb33477"

# 是否把本地图片上传到 WP 媒体库（True：上传；False：直接把本地路径当成 URL，通常会失败）
UPLOAD_LOCAL_IMAGES = True

# SQLite 表信息
DB_FILENAME = "SpiderResult.db3"
TABLE_NAME = "Content"

# 字段分隔符（你已指定）
IMG_SEPARATOR = "|||"
CAT_SEPARATOR = "|||"

# WooCommerce API endpoints
API_PRODUCTS = f"{DOMAIN}/wp-json/wc/v3/products"
API_CATEGORIES = f"{DOMAIN}/wp-json/wc/v3/products/categories"
API_MEDIA = f"{DOMAIN}/wp-json/wp/v2/media"  # 用于上传本地图片到媒体库

# 字段开关（True=发送到 WooCommerce，False=跳过）
FIELD_CONFIG = {
    "标题": True,
    "内容": True,
    "分类": True,
    "图片": True,
    "销售价": True,
    "折扣价": True,
    "SKU": True,
    "简介": True,
    "标签": False,
    "颜色": True,
    "规格": False,
    "品牌": False,
    "库存": True,
    "重量": False,
    "PageUrl": True,  # 是否回写 PageUrl（上传成功后会写回 permalink）
    # 你可以根据需要增减字段
}

# 使用 ||| 作为通用多值分隔符的字段集合
MULTI_VALUE_FIELDS = {
    "分类",   # 注意：这里主要用于通用解析；真正的层级仍由 create_category_hierarchy 处理
    "图片",
    "标签",
    "颜色",
    "规格",
}


def parse_field(raw, field_name: str):
    """通用字段解析：

    - 对于 MULTI_VALUE_FIELDS 中的字段：按 ||| 分割为列表
    - 其他字段：按 ||| 分割后只取第一个非空片段（避免意外多段）
    """
    separator = "|||"

    if raw is None:
        return [] if field_name in MULTI_VALUE_FIELDS else ""

    text = str(raw).strip()
    if not text:
        return [] if field_name in MULTI_VALUE_FIELDS else ""

    parts = [p.strip() for p in text.split(separator) if p.strip()]

    if field_name in MULTI_VALUE_FIELDS:
        return parts
    # 单值字段：只取第一个
    return parts[0] if parts else ""

# 重试与节流设置（避免对服务器并发太大）
SLEEP_BETWEEN_UPLOADS = 0.6  # 秒，上传每个商品后休眠
MAX_RETRIES = 3

# ========== End 配置区 ==========

auth = (KEY, SECRET)


# ---------- SQLite 帮助函数 ----------
def open_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # 以字典形式访问列名（支持中文列名）
    return conn


def update_sent_flag(conn, record_id, product_url=None):
    """把已发字段写成1，并可写回 PageUrl"""
    cur = conn.cursor()
    if FIELD_CONFIG.get("PageUrl", True) and product_url:
        cur.execute(f'UPDATE "{TABLE_NAME}" SET "已发" = 1, "PageUrl" = ? WHERE "ID" = ?', (product_url, record_id))
    else:
        cur.execute(f'UPDATE "{TABLE_NAME}" SET "已发" = 1 WHERE "ID" = ?', (record_id,))
    conn.commit()


# ---------- 分类相关（尝试复用现有分类或创建新分类） ----------
# 我们按“父->子”层级创建分类，返回最底层分类 ID 列表（供商品使用）
category_cache = {}  # 缓存 (name, parent_id) -> id


def find_category_by_name(name, parent=None):
    """尝试在已缓存或 API 中查找分类（返回 id 或 None）"""
    # 先在缓存中找
    key = (name, parent)
    if key in category_cache:
        return category_cache[key]

    # 使用 API 搜索 name（由于 WC API 没有按 name 精确查找的很好办法，尝试按 per_page 搜全部再匹配）
    page = 1
    while True:
        res = requests.get(API_CATEGORIES, auth=auth, params={"per_page": 100, "page": page})
        if res.status_code >= 400:
            break
        data = res.json()
        if not data:
            break
        for c in data:
            if c.get("name") == name and (parent is None or int(c.get("parent", 0)) == int(parent)):
                category_cache[key] = c["id"]
                return c["id"]
        page += 1

    return None


def create_category_hierarchy(cat_string):
    """接收 '父|||子|||孙' 字符串，按层级创建并返回最底层分类的 id"""
    if not cat_string:
        return []
    parts = [p.strip() for p in cat_string.split(CAT_SEPARATOR) if p.strip()]
    parent_id = None
    for part in parts:
        # 先查找是否存在
        existed = find_category_by_name(part, parent=parent_id)
        if existed:
            parent_id = existed
            continue
        # 创建新分类
        payload = {"name": part}
        if parent_id:
            payload["parent"] = parent_id
        res = requests.post(API_CATEGORIES, auth=auth, json=payload)
        if res.status_code >= 400:
            print(f"创建分类失败：{part} parent={parent_id} -> {res.text}")
            # 如果失败则尝试跳过或继续；这里选择跳过创建（但 parent_id 不变）
            parent_id = parent_id
        else:
            new_id = res.json()["id"]
            category_cache[(part, parent_id)] = new_id
            parent_id = new_id
            print(f"创建/使用分类：{part} => {new_id}")
    return [parent_id] if parent_id else []


# ---------- 图片上传相关 ----------
def upload_local_image_get_url(image_path):
    """把本地图片上传到 WP 媒体库，返回上传后的 URL（permalink 或 source_url）"""
    if not os.path.exists(image_path):
        print(f"图片不存在：{image_path}")
        return None

    filename = os.path.basename(image_path)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with open(image_path, "rb") as f:
                files = {'file': (filename, f, 'image/jpeg')}
                headers = {
                    # WordPress 要求 Content-Disposition (requests 会自动处理 multipart)
                }
                res = requests.post(API_MEDIA, auth=auth, files=files, headers=headers)
            if res.status_code < 400:
                data = res.json()
                # WordPress v2 media 返回 source_url 字段
                url = data.get("source_url") or data.get("guid", {}).get("rendered")
                return url
            else:
                print(f"上传媒体失败（{attempt}）：{res.status_code} {res.text}")
        except Exception as e:
            print("上传媒体异常：", e)
        time.sleep(0.8 * attempt)
    return None


def prepare_images_list(image_field_value, folder_base):
    """
    image_field_value: 来自 SQLite 的图片字段，使用 IMG_SEPARATOR 分割
    folder_base: 当前商品文件夹，用于定位本地文件
    返回 WooCommerce 格式的 images 列表（字典数组）
    """
    if not FIELD_CONFIG.get("图片", True):
        return None

    if not image_field_value:
        return None

    parts = [p.strip() for p in image_field_value.split(IMG_SEPARATOR) if p.strip()]
    if not parts:
        return None

    images = []
    for idx, p in enumerate(parts):
        # 判断是 URL 还是本地相对路径
        if p.lower().startswith("http://") or p.lower().startswith("https://"):
            # 远程图片 URL，直接交给 WooCommerce
            images.append({"src": p, "position": idx})
        else:
            # 认为是本地文件名或相对路径
            local_path = os.path.join(folder_base, p)
            # 也尝试 images 子目录
            if not os.path.exists(local_path):
                alt_path = os.path.join(folder_base, "images", p)
                if os.path.exists(alt_path):
                    local_path = alt_path

            if os.path.exists(local_path) and UPLOAD_LOCAL_IMAGES:
                url = upload_local_image_get_url(local_path)
                if url:
                    images.append({"src": url, "position": idx})
                else:
                    print(f"本地图片上传失败，尝试把路径当 URL 使用：{local_path}")
                    images.append({"src": local_path, "position": idx})
            else:
                # 没有文件，还是把原始值当 URL 填充（有可能是可访问的 URL）
                images.append({"src": p, "position": idx})

    return images


# ---------- 变体（Variations）相关 ----------
def create_variations_for_product(product_id, row):
    """根据颜色(Color) 和 规格(Size) 创建变体，价格/库存沿用主商品。

    - 颜色来自字段 "颜色"，多值用 ||| 分割
    - 尺寸来自字段 "规格"，多值用 ||| 分割
    - 若只有颜色或只有规格，则按单一维度创建变体
    - 价格/库存从当前行再次解析，和主商品保持一致
    """
    # 如果颜色/规格字段未启用，直接返回
    if not (FIELD_CONFIG.get("颜色", True) or FIELD_CONFIG.get("规格", True)):
        return

    colors = []
    sizes = []

    if FIELD_CONFIG.get("颜色", True) and "颜色" in row.keys() and row["颜色"]:
        parsed_colors = parse_field(row["颜色"], "颜色")
        if isinstance(parsed_colors, list):
            colors = parsed_colors

    if FIELD_CONFIG.get("规格", True) and "规格" in row.keys() and row["规格"]:
        parsed_sizes = parse_field(row["规格"], "规格")
        if isinstance(parsed_sizes, list):
            sizes = parsed_sizes

    # 如果两者都为空，就没必要创建变体
    if not colors and not sizes:
        return

    # 从行中解析价格和库存，沿用主商品的逻辑
    regular_price = None
    sale_price = None
    stock_quantity = None
    manage_stock = False

    if FIELD_CONFIG.get("销售价", True):
        raw_sale_price = row["销售价"] if "销售价" in row.keys() else None
        val = parse_field(raw_sale_price, "销售价")
        if val is not None and str(val).strip() != "":
            regular_price = str(val)

    if FIELD_CONFIG.get("折扣价", True):
        raw_disc = row["折扣价"] if "折扣价" in row.keys() else None
        val = parse_field(raw_disc, "折扣价")
        if val is not None and str(val).strip() != "":
            sale_price = str(val)

    if FIELD_CONFIG.get("库存", True):
        raw_qty = row["库存"] if "库存" in row.keys() else None
        qty = parse_field(raw_qty, "库存")
        if qty is not None and str(qty).strip() != "":
            try:
                stock_quantity = int(qty)
                manage_stock = True
            except Exception:
                pass

    # 生成属性组合
    combinations = []
    if colors and sizes:
        for c in colors:
            for s in sizes:
                combinations.append({
                    "attributes": [
                        {"name": "Color", "option": c},
                        {"name": "Size", "option": s},
                    ]
                })
    elif colors:
        for c in colors:
            combinations.append({
                "attributes": [
                    {"name": "Color", "option": c},
                ]
            })
    elif sizes:
        for s in sizes:
            combinations.append({
                "attributes": [
                    {"name": "Size", "option": s},
                ]
            })

    if not combinations:
        return

    variations_endpoint = f"{API_PRODUCTS}/{product_id}/variations"

    for combo in combinations:
        payload = {
            "attributes": combo["attributes"],
        }

        if regular_price is not None:
            payload["regular_price"] = regular_price
        if sale_price is not None:
            payload["sale_price"] = sale_price
        if manage_stock and stock_quantity is not None:
            payload["manage_stock"] = True
            payload["stock_quantity"] = stock_quantity

        try:
            res = requests.post(variations_endpoint, auth=auth, json=payload)
            if res.status_code >= 400:
                print(f"创建变体失败 product_id={product_id}, attrs={combo['attributes']} -> {res.status_code} {res.text}")
            else:
                vdata = res.json()
                vid = vdata.get("id")
                print(f"  ↳ 创建变体成功 ID={vid}, attrs={combo['attributes']}")
        except Exception as e:
            print(f"创建变体异常 product_id={product_id}, attrs={combo['attributes']}: {e}")


# ---------- 构建商品 JSON ----------
def build_product_payload(row, folder_base):
    """
    row: sqlite3.Row
    folder_base: 当前商品所在目录（用于本地图片定位）
    """
    p = {}

    # 标题
    if FIELD_CONFIG.get("标题", True):
        raw_title = row["标题"] if "标题" in row.keys() else row.get("title")
        title = parse_field(raw_title, "标题") or "无标题"
        p["name"] = title

    # 内容 / 描述
    if FIELD_CONFIG.get("内容", True):
        raw_desc = row["内容"] if "内容" in row.keys() else row.get("description", "")
        desc = parse_field(raw_desc, "内容")
        p["description"] = desc

    # 简介 / short_description
    if FIELD_CONFIG.get("简介", True):
        raw_short = row["简介"] if "简介" in row.keys() else ""
        short_desc = parse_field(raw_short, "简介")
        p["short_description"] = short_desc

    # 价格
    if FIELD_CONFIG.get("销售价", True):
        raw_sale_price = row["销售价"] if "销售价" in row.keys() else None
        sale_price = parse_field(raw_sale_price, "销售价")
        if sale_price is not None and str(sale_price).strip() != "":
            p["regular_price"] = str(sale_price)

    if FIELD_CONFIG.get("折扣价", True):
        raw_disc = row["折扣价"] if "折扣价" in row.keys() else None
        disc = parse_field(raw_disc, "折扣价")
        if disc is not None and str(disc).strip() != "":
            p["sale_price"] = str(disc)

    # SKU
    if FIELD_CONFIG.get("SKU", True):
        raw_sku = row["SKU"] if "SKU" in row.keys() else None
        sku = parse_field(raw_sku, "SKU")
        if sku:
            p["sku"] = str(sku)

    # 库存
    if FIELD_CONFIG.get("库存", True):
        try:
            raw_qty = row["库存"] if "库存" in row.keys() else None
            qty = parse_field(raw_qty, "库存")
            if qty is not None and str(qty).strip() != "":
                p["manage_stock"] = True
                p["stock_quantity"] = int(qty)
        except Exception:
            pass

    # 重量
    if FIELD_CONFIG.get("重量", True):
        raw_weight = row["重量"] if "重量" in row.keys() else None
        weight = parse_field(raw_weight, "重量")
        if weight is not None and str(weight).strip() != "":
            p["weight"] = str(weight)

    # 品牌、颜色、规格处理为 attributes 或 meta
    attributes = []
    # 品牌：非变量属性，仅用作展示
    if FIELD_CONFIG.get("品牌", True) and "品牌" in row.keys() and row["品牌"]:
        brand = parse_field(row["品牌"], "品牌")
        if brand:
            attributes.append({
                "name": "Brand",
                "visible": True,
                "variation": False,
                "options": [str(brand)],
            })
    # 颜色：变体属性 Color
    if FIELD_CONFIG.get("颜色", True) and "颜色" in row.keys() and row["颜色"]:
        colors = parse_field(row["颜色"], "颜色")
        if isinstance(colors, list) and colors:
            attributes.append({
                "name": "Color",
                "visible": True,
                "variation": True,
                "options": colors,
            })
    # 规格：变体属性 Size
    if FIELD_CONFIG.get("规格", True) and "规格" in row.keys() and row["规格"]:
        specs = parse_field(row["规格"], "规格")
        if isinstance(specs, list) and specs:
            attributes.append({
                "name": "Size",
                "visible": True,
                "variation": True,
                "options": specs,
            })

    if attributes:
        p["attributes"] = attributes

    # 标签
    if FIELD_CONFIG.get("标签", True) and "标签" in row.keys() and row["标签"]:
        tags = parse_field(row["标签"], "标签")
        if isinstance(tags, list) and tags:
            p["tags"] = [{"name": t} for t in tags]

    # 分类（如果字段开启，会尝试创建/查找并绑定最底层分类）
    if FIELD_CONFIG.get("分类", True) and "分类" in row.keys() and row["分类"]:
        cat_ids = create_category_hierarchy(row["分类"])
        if cat_ids:
            p["categories"] = [{"id": cid} for cid in cat_ids]

    # 图片
    imgs = None
    if FIELD_CONFIG.get("图片", True) and "图片" in row.keys() and row["图片"]:
        imgs = prepare_images_list(row["图片"], folder_base)
        if imgs:
            p["images"] = imgs

    # PageUrl 字段可以作为外链或引用，也可以放进 meta_data
    if "PageUrl" in row.keys() and row["PageUrl"]:
        # 把原始来源作为 meta 写入（非必须）
        p.setdefault("meta_data", []).append({"key": "source_page", "value": row["PageUrl"]})

    # 其他自定义字段，你可以在这里继续添加映射

    # 默认可变商品
    p.setdefault("type", "variable")
    return p


# ---------- 检查是否已经发布（通过 SKU 或 PageUrl） ----------
def product_exists_by_sku(sku):
    if not sku:
        return None
    res = requests.get(API_PRODUCTS, auth=auth, params={"sku": sku})
    if res.status_code >= 400:
        return None
    data = res.json()
    if isinstance(data, list) and data:
        return data[0]  # 返回第一个商品对象
    return None


# ---------- 主流程 ----------
def process_folder(folder_path):
    db_path = os.path.join(folder_path, DB_FILENAME)
    if not os.path.exists(db_path):
        print(f"未找到数据库：{db_path}, 跳过")
        return

    conn = open_db(db_path)
    cur = conn.cursor()

    # 可选：在本目录下重置所有记录的“已发”状态
    if RESET_SENT_BEFORE_UPLOAD:
        try:
            cur.execute(f'UPDATE "{TABLE_NAME}" SET "已发" = 0')
            conn.commit()
            print(f"已重置 {folder_path} 中所有记录的已发状态为 0")
        except Exception as e:
            print(f"重置已发状态失败（{folder_path}）：{e}")

    # 读取所有行（你可以加 WHERE "已发" = 0）
    try:
        cur.execute(f'SELECT * FROM "{TABLE_NAME}"')
    except Exception as e:
        print("读取表失败，请确认表名和列名：", e)
        conn.close()
        return

    rows = cur.fetchall()
    total_rows = len(rows)
    print(f"目录：{folder_path}")
    print(f"发现 {total_rows} 条记录，开始逐条处理。")

    uploaded = 0
    skipped = 0
    failed = 0

    for idx, row in enumerate(rows, start=1):
        try:
            record_id = row["ID"] if "ID" in row.keys() else None
            already_sent = row["已发"] if "已发" in row.keys() else 0
            if already_sent and int(already_sent) != 0:
                print(f"记录 {record_id} 已标记为已发，跳过。")
                skipped += 1
                remaining = total_rows - idx
                print(f"[进度] 已处理 {idx}/{total_rows} | 已上传 {uploaded} | 跳过 {skipped} | 失败 {failed} | 剩余 {remaining}")
                continue

            # 若开启 SKU 检查，避免重复上传
            if FIELD_CONFIG.get("SKU", True) and "SKU" in row.keys() and row["SKU"]:
                existing = product_exists_by_sku(row["SKU"])
                if existing:
                    print(f"记录 {record_id} 检测到相同 SKU 的商品已存在（ID={existing.get('id')}），标记为已发并跳过。")
                    update_sent_flag(conn, record_id, existing.get("permalink"))
                    skipped += 1
                    remaining = total_rows - idx
                    print(f"[进度] 已处理 {idx}/{total_rows} | 已上传 {uploaded} | 跳过 {skipped} | 失败 {failed} | 剩余 {remaining}")
                    continue

            payload = build_product_payload(row, folder_base=folder_path)
            if not payload:
                print(f"记录 {record_id} 构建 payload 为空，跳过。")
                skipped += 1
                remaining = total_rows - idx
                print(f"[进度] 已处理 {idx}/{total_rows} | 已上传 {uploaded} | 跳过 {skipped} | 失败 {failed} | 剩余 {remaining}")
                continue

            # 上传商品
            print(f"发布商品：{payload.get('name')}")
            res = requests.post(API_PRODUCTS, auth=auth, json=payload)
            if res.status_code >= 400:
                print(f"上传失败：{res.status_code} {res.text}")
                # 可选：重试机制
                continue

            result = res.json()
            product_url = result.get("permalink") or result.get("link") or None
            product_id = result.get("id")
            print(f"✔ 上传成功：{payload.get('name')} -> product_id={product_id}")
            uploaded += 1

            # 创建可变商品的变体（Color/Size）
            try:
                create_variations_for_product(product_id, row)
            except Exception as e:
                print(f"创建变体时发生异常 product_id={product_id}: {e}")

            # 回写 SQLite 的 已发 和 PageUrl
            try:
                update_sent_flag(conn, record_id, product_url)
            except Exception as e:
                print("回写数据库失败：", e)

            time.sleep(SLEEP_BETWEEN_UPLOADS)

        except Exception as e:
            print("处理单条记录异常：", e)
            failed += 1
        finally:
            remaining = total_rows - idx
            print(f"[进度] 已处理 {idx}/{total_rows} | 已上传 {uploaded} | 跳过 {skipped} | 失败 {failed} | 剩余 {remaining}")
            continue

    conn.close()


def main():
    # 按文件夹排序（数字排序）
    subfolders = [f for f in os.listdir(ROOT_DIR) if os.path.isdir(os.path.join(ROOT_DIR, f))]

    # 尝试按数字排序，如果不是纯数字则按名称排序
    is_numeric_sort = False
    try:
        subfolders.sort(key=lambda x: int(x))
        is_numeric_sort = True
    except Exception:
        subfolders.sort()

    # 如果配置了开始/结束文件夹，则做范围筛选（包含边界）
    def in_range(name: str) -> bool:
        if START_FOLDER is None and END_FOLDER is None:
            return True

        if is_numeric_sort:
            try:
                n = int(name)
                if START_FOLDER is not None:
                    if n < int(START_FOLDER):
                        return False
                if END_FOLDER is not None:
                    if n > int(END_FOLDER):
                        return False
                return True
            except Exception:
                # 如果转换失败，退回到字符串比较
                pass

        # 字符串比较（用于非纯数字文件夹名）
        if START_FOLDER is not None and name < str(START_FOLDER):
            return False
        if END_FOLDER is not None and name > str(END_FOLDER):
            return False
        return True

    subfolders = [f for f in subfolders if in_range(f)]

    total_folders = len(subfolders)
    print(f"将处理 {total_folders} 个文件夹。多进程: {USE_MULTIPROCESS} (进程数: {MAX_PROCESSES})")

    if not subfolders:
        print("未找到需要处理的子文件夹，结束。")
        return

    if USE_MULTIPROCESS:
        # 多进程按子文件夹并行处理
        folder_paths = [os.path.join(ROOT_DIR, sub) for sub in subfolders]
        for idx, fp in enumerate(folder_paths, start=1):
            print(f"[调度] {idx}/{total_folders}: {fp}")

        with Pool(processes=MAX_PROCESSES) as pool:
            pool.map(process_folder, folder_paths)
    else:
        # 单进程顺序处理
        for idx, sub in enumerate(subfolders, start=1):
            folder_path = os.path.join(ROOT_DIR, sub)
            print("\n-------------------------------")
            print(f"开始处理文件夹 ({idx}/{total_folders})：{folder_path}")
            process_folder(folder_path)

    print("\n全部处理完毕。")


if __name__ == "__main__":
    main()
