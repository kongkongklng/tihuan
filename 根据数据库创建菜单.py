#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
根据已上传商品所属分类，自动在指定菜单中创建对应的分类菜单项。

逻辑：
1. 遍历 ROOT_DIR 下的子文件夹，查找 SpiderResult.db3 (Content 表)。
2. 读取其中已发布的商品记录（已发 != 0），抽取非空的 "分类" 字段，去重，得到分类路径集合。
3. 使用 WordPress REST API：
   - 确保每条分类路径对应的 product_cat 分类存在（按层级父子创建）。
   - 在指定菜单 (TARGET_MENU_ID) 下按层级创建菜单项，菜单项类型为 taxonomy/product_cat。

注意：
- 只有真正出现在数据库、且已发标记不为 0 的分类才会参与菜单创建。
- 不依赖分类.txt 文件。
"""

import os
import sqlite3
import json
from typing import Dict, Any, List, Optional, Set

import requests
from requests.auth import HTTPBasicAuth

# ================== 数据源配置（与上传脚本保持一致） ==================
ROOT_DIR = r"D:\火车采集器V10.28\Data"  # 与上传脚本一致
DB_FILENAME = "SpiderResult.db3"
TABLE_NAME = "Content"

# 子文件夹范围（可按需要调整，含边界；若都为 None 则不限制）
START_FOLDER = 5211
END_FOLDER = 5522

# ================== WordPress 菜单 / 分类配置 ==================
DOMAIN = "https://www.ottarbox.club"

# WP 用户 + Application Password（用于菜单与 taxonomy 操作）
USER = "admin"
APP_PASSWORD = "w0Iy7DI2owAIejElTMoFMIU9"

# 目标菜单 ID
TARGET_MENU_ID = 2097

# 使用的 taxonomy（WooCommerce 商品分类）
TAXONOMY = "product_cat"

SEPARATOR = "|||"  # 分类层级分隔符

# ================== API 端点 ==================
API_MENU_ITEMS = f"{DOMAIN}/wp-json/wp/v2/menu-items"
API_PRODUCT_CAT = f"{DOMAIN}/wp-json/wp/v2/{TAXONOMY}"

auth = HTTPBasicAuth(USER, APP_PASSWORD)


class MenuNode:
    def __init__(self, name: str, full_path: str) -> None:
        self.name = name
        self.full_path = full_path  # 完整路径，如 "Accessories|||Accessories for MagSafe"
        self.children: Dict[str, "MenuNode"] = {}


# ================== 第一步：从 SQLite 收集已上传分类路径 ==================

def in_range(name: str, start: Optional[str], end: Optional[str]) -> bool:
    """根据 START_FOLDER/END_FOLDER 判断子文件夹是否在范围内。"""
    if start is None and end is None:
        return True

    # 先尝试按数字比较
    try:
        n = int(name)
        if start is not None and n < int(start):
            return False
        if end is not None and n > int(end):
            return False
        return True
    except Exception:
        # 回退到字符串比较
        if start is not None and name < str(start):
            return False
        if end is not None and name > str(end):
            return False
        return True


def collect_used_category_paths() -> Set[str]:
    """遍历 ROOT_DIR 下的各个子文件夹，从 SQLite 中收集已发商品的分类路径。"""
    used_paths: Set[str] = set()

    if not os.path.isdir(ROOT_DIR):
        print(f"ROOT_DIR 不存在或不是目录: {ROOT_DIR}")
        return used_paths

    subfolders = [f for f in os.listdir(ROOT_DIR) if os.path.isdir(os.path.join(ROOT_DIR, f))]

    # 排序，尽量与上传脚本行为一致
    try:
        subfolders.sort(key=lambda x: int(x))
    except Exception:
        subfolders.sort()

    subfolders = [f for f in subfolders if in_range(f, START_FOLDER, END_FOLDER)]

    print(f"将在以下 {len(subfolders)} 个子文件夹中收集已发分类：")
    for idx, sub in enumerate(subfolders, start=1):
        folder_path = os.path.join(ROOT_DIR, sub)
        print(f"  [{idx}/{len(subfolders)}] {folder_path}")

    for sub in subfolders:
        folder_path = os.path.join(ROOT_DIR, sub)
        db_path = os.path.join(folder_path, DB_FILENAME)
        if not os.path.exists(db_path):
            continue

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
        except Exception as e:
            print(f"  打开数据库失败: {db_path} -> {e}")
            continue

        try:
            # 只取 已发 != 0 且 分类 非空的记录
            sql = (
                f'SELECT DISTINCT "分类" FROM "{TABLE_NAME}" '
                'WHERE "已发" IS NOT NULL AND "已发" != 0 '
                'AND "分类" IS NOT NULL AND TRIM("分类") != ""'
            )
            cur.execute(sql)
            rows = cur.fetchall()
            for row in rows:
                cat = row["分类"]
                if isinstance(cat, str):
                    cat = cat.strip()
                if not cat:
                    continue
                used_paths.add(cat)
        except Exception as e:
            print(f"  读取分类失败: {db_path} -> {e}")
        finally:
            conn.close()

    print(f"\n共收集到 {len(used_paths)} 条已发分类路径。")
    return used_paths


# ================== 第二步：菜单 & 分类创建逻辑 ==================

# 缓存 (name, parent_term_id) -> term_id
term_cache: Dict[tuple, int] = {}


def load_existing_terms() -> None:
    """预加载已有的 product_cat term，填充 term_cache。"""
    page = 1
    while True:
        resp = requests.get(API_PRODUCT_CAT, auth=auth, params={"per_page": 100, "page": page})
        if resp.status_code >= 400:
            break
        data = resp.json()
        if not data:
            break
        for term in data:
            name = term.get("name")
            parent = int(term.get("parent", 0) or 0)
            term_id = int(term.get("id"))
            term_cache[(name, parent)] = term_id
        page += 1


def ensure_term_for_path(path: str) -> Optional[int]:
    """确保给定 full_path 对应的 taxonomy term 存在，返回最底层 term_id。"""
    parts = [p.strip() for p in path.split(SEPARATOR) if p.strip()]
    if not parts:
        return None

    parent_term_id = 0  # 顶级 parent 为 0

    for name in parts:
        key = (name, parent_term_id)
        if key in term_cache:
            parent_term_id = term_cache[key]
            continue

        payload = {"name": name}
        if parent_term_id:
            payload["parent"] = parent_term_id

        print(f"  创建分类 term: name='{name}', parent_term_id={parent_term_id}")
        resp = requests.post(API_PRODUCT_CAT, auth=auth, json=payload)

        # 尝试解析响应 JSON
        try:
            data = resp.json()
        except Exception:
            data = None

        if resp.status_code >= 400:
            # 特判：term 已存在，复用已有 term_id
            if isinstance(data, dict) and data.get("code") == "term_exists":
                existing_id = None
                # 优先从 data.data.term_id 取
                if isinstance(data.get("data"), dict) and "term_id" in data["data"]:
                    existing_id = int(data["data"]["term_id"])
                # 退而求其次，从 additional_data[0] 取
                elif isinstance(data.get("additional_data"), list) and data["additional_data"]:
                    existing_id = int(data["additional_data"][0])

                if existing_id:
                    print(f"    ⚠️ term 已存在，复用 term_id={existing_id}")
                    term_cache[key] = existing_id
                    parent_term_id = existing_id
                    continue  # 继续处理下一级

            # 其它错误仍然视为失败
            print("    ❌ 创建 term 失败:")
            if data is not None:
                print(json.dumps(data, ensure_ascii=False, indent=2))
            else:
                print(resp.text)
            return None

        # 正常新建 term 的情况
        term_id = int(data.get("id"))
        print(f"    ✅ 创建 term 成功，term_id={term_id}")
        term_cache[key] = term_id
        parent_term_id = term_id

    return parent_term_id


def create_menu_item_for_term(title: str, term_id: int, menu_id: int, parent_item_id: int = 0, order: int = 1) -> Optional[int]:
    """为指定 taxonomy term 创建菜单项（type=taxonomy, object=product_cat）。"""
    payload = {
        "title": title,
        "status": "publish",
        "menu_order": order,
        "menus": menu_id,
        "parent": parent_item_id,
        "type": "taxonomy",
        "object": TAXONOMY,
        "object_id": int(term_id),
    }

    print(f"  创建菜单项: title='{title}', term_id={term_id}, parent={parent_item_id}, order={order}")
    resp = requests.post(API_MENU_ITEMS, auth=auth, json=payload)
    print("    状态码:", resp.status_code)

    try:
        data = resp.json()
    except Exception:
        print("    响应内容:")
        print(resp.text)
        return None

    if resp.status_code >= 400:
        print("    ❌ 创建失败:")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return None

    item_id = data.get("id")
    print(f"    ✅ 创建成功，菜单项 ID: {item_id}")
    return item_id


def build_menu_tree_from_paths(paths: List[str]) -> Dict[str, MenuNode]:
    """根据若干 full_path 字符串构建菜单树。"""
    roots: Dict[str, MenuNode] = {}

    for line in paths:
        parts = [p.strip() for p in line.split(SEPARATOR) if p.strip()]
        if not parts:
            continue

        root_name = parts[0]
        if root_name not in roots:
            roots[root_name] = MenuNode(root_name, root_name)
        current = roots[root_name]

        for part in parts[1:]:
            next_full_path = current.full_path + SEPARATOR + part
            if part not in current.children:
                current.children[part] = MenuNode(part, next_full_path)
            current = current.children[part]

    return roots


def create_menus_for_used_categories(used_paths: Set[str]) -> None:
    if not used_paths:
        print("没有任何已发分类路径，跳过菜单创建。")
        return

    # 转成列表，便于排序和构建树
    path_list = sorted(used_paths)

    print("\n🌲 根据已发分类构建菜单树……")
    roots = build_menu_tree_from_paths(path_list)
    print(f"📁 顶级菜单数量: {len(roots)}")

    print("📥 预加载已有 product_cat 分类……")
    load_existing_terms()
    print(f"📥 已加载分类数量: {len(term_cache)}")

    created_count = 0
    failed_count = 0

    path_to_item_id: Dict[str, int] = {}

    print("🚀 开始向菜单 ID =", TARGET_MENU_ID, "创建菜单项……")

    def create_nodes(nodes: Dict[str, MenuNode], parent_item_id: int, level: int) -> None:
        nonlocal created_count, failed_count

        order = 1
        for name in sorted(nodes.keys()):
            node = nodes[name]
            indent = "  " * level
            full_path = node.full_path

            print(f"{indent}• 处理分类: {full_path}")
            term_id = ensure_term_for_path(full_path)
            if term_id is None:
                print(f"{indent}  ⚠️ 无法为该路径创建/获取 term，跳过对应菜单项")
                failed_count += 1
                continue

            item_id = create_menu_item_for_term(node.name, term_id, TARGET_MENU_ID, parent_item_id, order)
            order += 1

            if item_id is None:
                failed_count += 1
            else:
                created_count += 1
                path_to_item_id[full_path] = item_id
                if node.children:
                    create_nodes(node.children, item_id, level + 1)

    create_nodes(roots, parent_item_id=0, level=0)

    print("\n✅ 菜单创建完成。")
    print(f"总共创建菜单项: {created_count}，失败: {failed_count}")


def main() -> None:
    print("开始收集已发商品分类……")
    used_paths = collect_used_category_paths()
    print("\n准备为以下分类创建菜单（仅展示前 20 条）：")
    for i, p in enumerate(sorted(used_paths), start=1):
        if i > 20:
            print("  ……（更多分类省略）")
            break
        print("  ", p)

    create_menus_for_used_categories(used_paths)


if __name__ == "__main__":
    main()
