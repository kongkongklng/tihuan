import pandas as pd
import os
import re

# ====== 这里自定义路径和字段名 ======
base_path = r'D:\火车采集器V10.28\Data'    # 基础文件夹路径，可改为绝对路径
csv_name = 'Content.csv'     # 文件名
# ====================================

def read_csv_file(folder_path, csv_name):
    """读取CSV文件"""
    csv_path = os.path.join(folder_path, csv_name)
    if not os.path.exists(csv_path):
        print(f"文件不存在: {csv_path}")
        return None, None
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"成功读取文件: {csv_path}")
        print(f"数据行数: {len(df)}")
        print(f"列名: {list(df.columns)}")
        return df, csv_path
    except Exception as e:
        print(f"读取文件时出现错误: {e}")
        return None, None

def save_csv_file(df, csv_path, folder_path, csv_name, operation_name):
    """保存CSV文件"""
    try:
        # 先删除原文件（如果存在）
        if os.path.exists(csv_path):
            os.remove(csv_path)
            print(f"已删除原文件: {csv_path}")
        
        # 保存新文件
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"{operation_name}完成！已覆盖原文件: {csv_path}")
        print(f"新的列名: {list(df.columns)}")
        
    except PermissionError:
        print(f"权限错误：无法覆盖文件 {csv_path}")
        print("请确保文件没有被其他程序打开，或者尝试以管理员身份运行")
        print("尝试保存为临时文件...")
        
        # 如果无法覆盖，保存为临时文件
        temp_path = os.path.join(folder_path, f"temp_{csv_name}")
        df.to_csv(temp_path, index=False, encoding='utf-8-sig')
        print(f"已保存为临时文件: {temp_path}")
        print("请手动替换原文件")

def convert_db_to_csv():
    """将多个文件夹中的SpiderResult.db3转换为CSV，并删除颜色重复列"""
    # 查找所有包含SpiderResult.db3的文件夹
    db_files = []
    
    # 设置文件夹范围
    start_folder = 449
    end_folder = 491
    
    print(f"搜索范围：文件夹 {start_folder} 到 {end_folder}")
    
    # 递归搜索所有SpiderResult.db3文件
    for root, dirs, files in os.walk(base_path):
        if 'SpiderResult.db3' in files:
            # 检查当前文件夹名是否为数字且在指定范围内
            current_folder = os.path.basename(root)
            try:
                folder_num = int(current_folder)
                if start_folder <= folder_num <= end_folder:
                    db_path = os.path.join(root, 'SpiderResult.db3')
                    db_files.append(db_path)
                    print(f"找到符合条件的文件夹：{current_folder}")
            except ValueError:
                # 如果文件夹名不是数字，跳过
                continue
    
    if not db_files:
        print(f"错误：在 {base_path} 及其子文件夹中未找到SpiderResult.db3文件")
        return
    
    print(f"找到 {len(db_files)} 个SpiderResult.db3文件：")
    for db_path in db_files:
        print(f"  - {db_path}")
    print()
    
    for i, db_path in enumerate(db_files, 1):
        print(f"处理第 {i}/{len(db_files)} 个数据库文件：{db_path}")
        
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            
            # 只处理Content表
            table_name = 'Content'
            
            # 检查表是否存在
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Content';")
            if not cursor.fetchone():
                print(f"  错误：数据库中不存在 {table_name} 表，跳过")
                conn.close()
                continue
            
            # 读取表数据
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            
            print(f"  原始列名：{list(df.columns)}")
            print(f"  数据行数：{len(df)}")
            
            # 查找并删除颜色重复列
            columns_to_drop = []
            for col in df.columns:
                if '颜色重复' in col or 'color_duplicate' in col.lower() or 'duplicate' in col.lower():
                    columns_to_drop.append(col)
            
            if columns_to_drop:
                print(f"  删除列：{columns_to_drop}")
                df = df.drop(columns=columns_to_drop)
            
            # 保存为CSV（保存在数据库文件所在的文件夹中）
            db_folder = os.path.dirname(db_path)
            csv_filename = os.path.join(db_folder, f"{table_name}.csv")
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"  已保存为：{csv_filename}")
            print(f"  最终列名：{list(df.columns)}")
            
            conn.close()
            print(f"  数据库转换完成！\n")
            
        except Exception as e:
            print(f"  转换过程中出现错误：{e}\n")
    
    print("所有数据库文件处理完成！")

def format_category_field(df, field_name='分类'):
    """格式化分类字段为嵌套格式"""
    def nested_format(cat):
        if pd.isna(cat):
            return ''
        parts = [p.strip() for p in str(cat).split('|||') if p.strip()]
        result = []
        # 生成所有嵌套路径
        for i in range(1, len(parts)):
            result.append(' > '.join(parts[:i+1]))
        if parts:
            result.append(parts[0])  # 单独加第一级
        # 去重并保持顺序
        result = list(dict.fromkeys(result))
        return ', '.join(result)
    
    df[field_name] = df[field_name].apply(nested_format)
    return df

def convert_title_to_name(df, old_field_name='标题', new_field_name='名称'):
    """将标题字段转换为名称字段"""
    if old_field_name not in df.columns:
        print(f"错误：字段 '{old_field_name}' 不存在")
        return df
    
    df = df.rename(columns={old_field_name: new_field_name})
    return df

def convert_image_urls(df, field_name='图片'):
    """转换图片链接格式"""
    def convert_url(url_string):
        if pd.isna(url_string) or url_string == '':
            return ''
        
        # 按|||分割多个URL
        urls = url_string.split('|||')
        converted_urls = []
        
        for url in urls:
            url = url.strip()
            if url:
                # 转换URL格式
                # 从: http://municipal.com/cdn/shop/files/xxx.jpg?v=123456
                # 到: https://www.municipals.top/wp-content/uploads/2025/08/xxx-2.jpg
                
                # 提取文件名部分
                match = re.search(r'/files/([^?]+)', url)
                if match:
                    filename = match.group(1)
                    # 添加-2后缀到文件名（在扩展名之前）
                    name_parts = filename.rsplit('.', 1)
                    if len(name_parts) == 2:
                        new_filename = f"{name_parts[0]}-2.{name_parts[1]}"
                    else:
                        new_filename = f"{filename}-2"
                    
                    # 构建新URL
                    new_url = f"https://www.municipals.top/wp-content/uploads/2025/08/{new_filename}"
                    converted_urls.append(new_url)
                else:
                    # 如果无法解析，保持原URL
                    converted_urls.append(url)
        
        # 用逗号和空格连接
        return ', '.join(converted_urls)
    
    df[field_name] = df[field_name].apply(convert_url)
    return df

def convert_color_attributes(df, old_field_name='颜色'):
    """将颜色字段转换为属性字段"""
    if old_field_name not in df.columns:
        print(f"错误：字段 '{old_field_name}' 不存在")
        return df
    
    def convert_color_values(color_string):
        if pd.isna(color_string) or color_string == '':
            return ''
        
        # 按|||分割多个颜色值
        colors = color_string.split('|||')
        # 去除空白并过滤空值
        colors = [color.strip() for color in colors if color.strip()]
        # 用逗号和空格连接
        return ', '.join(colors)
    
    # 创建新的属性字段
    df['属性 1 名称'] = 'Color'
    df['属性 1 值'] = df[old_field_name].apply(convert_color_values)
    df['属性 1 可见'] = 1
    df['属性 1  的全局'] = 0
    
    # 删除原颜色字段
    df = df.drop(columns=[old_field_name])
    return df

def convert_size_attributes(df, old_field_name='规格'):
    """将规格字段转换为属性字段"""
    if old_field_name not in df.columns:
        print(f"错误：字段 '{old_field_name}' 不存在")
        return df
    
    def convert_size_values(size_string):
        if pd.isna(size_string) or size_string == '':
            return ''
        
        # 按|||分割多个规格值
        sizes = size_string.split('|||')
        cleaned_sizes = []
        
        for size in sizes:
            size = size.strip()
            if size:
                # 清理HTML标签和多余字符
                # 移除HTML标签
                size = re.sub(r'<[^>]+>', '', size)
                
                # 移除多余的空白字符
                size = re.sub(r'\s+', ' ', size)
                
                # 移除特殊字符和多余文本
                size = re.sub(r'disabled', '', size)
                size = re.sub(r'value=\'[^\']*\'', '', size)
                size = re.sub(r'data-option=\'[^\']*\'', '', size)
                size = re.sub(r'删除', '', size)
                size = re.sub(r']\'', '', size)
                size = re.sub(r']', '', size)
                size = re.sub(r'\'', '', size)
                
                # 移除方括号和引号
                size = re.sub(r'\[[^\]]*\]', '', size)
                size = re.sub(r'\]', '', size)
                size = re.sub(r'\[', '', size)
                
                # 移除多余的空白字符
                size = re.sub(r'\s+', ' ', size)
                size = size.strip()
                
                if size:
                    # 提取颜色/规格组合（如 "Blue Sage / L"）
                    color_size_match = re.search(r'([^/]+)\s*/\s*([A-Z0-9]+)', size)
                    if color_size_match:
                        color = color_size_match.group(1).strip()
                        size_code = color_size_match.group(2).strip()
                        # 确保颜色和规格代码都不为空
                        if color and size_code:
                            cleaned_sizes.append(f"{color} / {size_code}")
                    else:
                        # 如果没有颜色/规格组合，只保留规格代码
                        size_code_match = re.search(r'([A-Z0-9]+)', size)
                        if size_code_match:
                            cleaned_sizes.append(size_code_match.group(1))
                        else:
                            # 如果都不匹配，保留清理后的原始文本（但排除不完整的组合）
                            cleaned_size = size.strip()
                            # 排除以 "/" 结尾的不完整组合
                            if cleaned_size and len(cleaned_size) > 1 and not cleaned_size.endswith('/'):
                                cleaned_sizes.append(cleaned_size)
        
        # 去重并排序
        cleaned_sizes = list(set(cleaned_sizes))
        cleaned_sizes.sort()
        
        # 用逗号和空格连接
        return ', '.join(cleaned_sizes)
    
    # 创建新的属性字段
    df['属性 2 名称'] = 'Size'
    df['属性 2 值'] = df[old_field_name].apply(convert_size_values)
    df['属性 2 可见'] = 1
    df['属性 2  的全局'] = 0
    
    # 删除原规格字段
    df = df.drop(columns=[old_field_name])
    return df

def convert_price_fields(df):
    """转换价格字段名称"""
    fields_to_rename = {
        '销售价': '常规售价',
        '折扣价': '促销价格'
    }
    
    missing_fields = []
    for old_field, new_field in fields_to_rename.items():
        if old_field not in df.columns:
            missing_fields.append(old_field)
    
    if missing_fields:
        print(f"错误：以下字段不存在: {missing_fields}")
        return df
    
    # 重命名字段
    df = df.rename(columns=fields_to_rename)
    return df

def main():
    """主函数 - 执行所有转换操作"""
    print("=== CSV数据处理工具 ===\n")
    
    # 1. 数据库转换
    print("1. 执行数据库转换...")
    convert_db_to_csv()
    print()
    
    # 2. 读取CSV文件（使用第一个找到的CSV文件）
    print("2. 读取CSV文件...")
    
    # 查找指定范围内的第一个CSV文件
    start_folder = 449
    end_folder = 491
    csv_found = False
    
    for root, dirs, files in os.walk(base_path):
        if 'Content.csv' in files:
            current_folder = os.path.basename(root)
            try:
                folder_num = int(current_folder)
                if start_folder <= folder_num <= end_folder:
                    csv_path = os.path.join(root, 'Content.csv')
                    df, csv_path = read_csv_file(root, 'Content.csv')
                    csv_found = True
                    break
            except ValueError:
                continue
    
    if not csv_found:
        print("错误：在指定范围内未找到Content.csv文件")
        return
    print()
    
    # 3. 分类字段格式化
    print("3. 执行分类字段格式化...")
    if '分类' in df.columns:
        df = format_category_field(df, '分类')
        print("分类字段格式化完成")
    else:
        print("分类字段不存在，跳过")
    print()
    
    # 4. 标题转名称
    print("4. 执行标题转名称...")
    if '标题' in df.columns:
        df = convert_title_to_name(df, '标题', '名称')
        print("标题转名称完成")
    else:
        print("标题字段不存在，跳过")
    print()
    
    # 5. 图片链接转换
    print("5. 执行图片链接转换...")
    if '图片' in df.columns:
        df = convert_image_urls(df, '图片')
        print("图片链接转换完成")
    else:
        print("图片字段不存在，跳过")
    print()
    
    # 6. 颜色属性转换
    print("6. 执行颜色属性转换...")
    if '颜色' in df.columns:
        df = convert_color_attributes(df, '颜色')
        print("颜色属性转换完成")
    else:
        print("颜色字段不存在，跳过")
    print()
    
    # 7. 规格属性转换
    print("7. 执行规格属性转换...")
    if '规格' in df.columns:
        df = convert_size_attributes(df, '规格')
        print("规格属性转换完成")
    else:
        print("规格字段不存在，跳过")
    print()
    
    # 8. 价格字段转换
    print("8. 执行价格字段转换...")
    if '销售价' in df.columns or '折扣价' in df.columns:
        df = convert_price_fields(df)
        print("价格字段转换完成")
    else:
        print("价格字段不存在，跳过")
    print()
    
    # 9. 保存文件
    print("9. 保存处理后的文件...")
    folder_path = os.path.dirname(csv_path)
    save_csv_file(df, csv_path, folder_path, csv_name, "所有数据处理")
    print()
    
    print("=== 所有处理完成 ===")

if __name__ == "__main__":
    main() 