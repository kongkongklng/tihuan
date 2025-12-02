# 针对存在的分类.txt自动生成WP菜单


import os
import json
from typing import Dict, Any, List, Optional

import requests
from requests.auth import HTTPBasicAuth

# ================= 配置区 =================
DOMAIN = "https://www.ottarbox.club"

# 与 test_menus_api.py 保持一致：后台用户名 + Application Password
USER = "admin"
APP_PASSWORD = "w0Iy7DI2owAIejElTMoFMIU9"

# 目标菜单 ID（从 /wp-json/wp/v2/menu-locations 返回的 "menu" 字段获得）
TARGET_MENU_ID = 151

# 使用的 taxonomy（WooCommerce 商品分类）
TAXONOMY = "product_cat"

# 分类文件
CATEGORY_FILE = r"D:\project\otterbox\分类\分类.txt"
SEPARATOR = "|||"

# ================= API 端点 =================
API_MENU_ITEMS = f"{DOMAIN}/wp-json/wp/v2/menu-items"
API_PRODUCT_CAT = f"{DOMAIN}/wp-json/wp/v2/{TAXONOMY}"

auth = HTTPBasicAuth(USER, APP_PASSWORD)


class MenuNode:
    def __init__(self, name: str, full_path: str) -> None:
        self.name = name
        self.full_path = full_path  # 完整路径，如 "Accessories|||Accessories for MagSafe"
        self.children: Dict[str, "MenuNode"] = {}


def read_categories(base_dir: str) -> List[str]:
    path = os.path.join(base_dir, CATEGORY_FILE)
    if not os.path.exists(path):
        raise FileNotFoundError(f"未找到分类文件: {path}")

    lines: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            lines.append(line)
    return lines


def build_menu_tree(categories: List[str]) -> Dict[str, MenuNode]:
    roots: Dict[str, MenuNode] = {}

    for line in categories:
        parts = [p.strip() for p in line.split(SEPARATOR) if p.strip()]
        if not parts:
            continue

        full_path = SEPARATOR.join(parts)
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


def create_menu_item_for_term(title: str, term_id: int, menu_id: int, parent_item_id: int = 0, order: int = 1) -> Optional[int]:
    """为指定 taxonomy term 创建菜单项（type=taxonomy, object=product_cat）。"""
    payload = {
        "title": title,
        "status": "publish",
        "menu_order": order,
        # 将菜单项关联到指定菜单
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


# ========== product_cat 分类同步/创建 ==========

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
    """确保给定 full_path 对应的 taxonomy term 存在，返回最底层 term_id。

    按路径分段，逐级检查/创建：
    - (name, parent_term_id) 在缓存中则复用
    - 否则 POST /product_cat 创建新 term
    """
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
        if resp.status_code >= 400:
            print("    ❌ 创建 term 失败:")
            try:
                print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
            except Exception:
                print(resp.text)
            return None

        data = resp.json()
        term_id = int(data.get("id"))
        print(f"    ✅ 创建 term 成功，term_id={term_id}")
        term_cache[key] = term_id
        parent_term_id = term_id

    return parent_term_id


def build_and_create_menu(base_dir: str) -> None:
    print("📂 读取分类文件……")
    categories = read_categories(base_dir)
    print(f"📊 共 {len(categories)} 条分类路径")

    print("🌲 构建菜单树……")
    roots = build_menu_tree(categories)
    print(f"📁 顶级菜单数量: {len(roots)}")

    print("📥 预加载已有 product_cat 分类……")
    load_existing_terms()
    print(f"📥 已加载分类数量: {len(term_cache)}")

    created_count = 0
    failed_count = 0

    # 记录 full_path -> 菜单项 ID，方便为子节点设置 parent
    path_to_item_id: Dict[str, int] = {}

    print("🚀 开始向菜单 ID =", TARGET_MENU_ID, "创建菜单项……")

    # 递归创建
    def create_nodes(nodes: Dict[str, MenuNode], parent_item_id: int, level: int) -> None:
        nonlocal created_count, failed_count

        order = 1
        # 为了稳定顺序，按名称排序
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
                # 递归创建子节点
                if node.children:
                    create_nodes(node.children, item_id, level + 1)

    # 从顶级开始创建
    create_nodes(roots, parent_item_id=0, level=0)

    print("\n✅ 菜单创建完成。")
    print(f"总共创建菜单项: {created_count}，失败: {failed_count}")


def main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    build_and_create_menu(base_dir)


if __name__ == "__main__":
    main()
