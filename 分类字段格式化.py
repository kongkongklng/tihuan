import pandas as pd
import os

# ====== 这里自定义路径和字段名 ======
folder_path = r'D:\project\municipal\产品csv'  # 文件夹路径，可改为绝对路径
field_name = 'Categories'           # 字段名
# ====================================

def format_category_field(csv_path, field_name='分类'):
    if not os.path.exists(csv_path):
        print(f"文件不存在: {csv_path}")
        return
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
    except Exception as e:
        print(f"读取失败: {csv_path}, 错误: {e}")
        return

    if field_name not in df.columns:
        print(f"字段不存在: {field_name}，文件: {csv_path}")
        return

    def nested_format(cat):
        if pd.isna(cat):
            return ''
        parts = [p.strip() for p in str(cat).split('|||') if p.strip()]
        result = []
        for i in range(1, len(parts)):
            result.append(' > '.join(parts[:i+1]))
        if parts:
            result.append(parts[0])  # 单独加第一级
        result = list(dict.fromkeys(result))  # 去重保持顺序
        return ', '.join(result)

    df[field_name] = df[field_name].apply(nested_format)
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"{csv_path} 分类字段已格式化完成！")

if __name__ == "__main__":
    for file in os.listdir(folder_path):
        if file.lower().endswith('.csv'):
            csv_path = os.path.join(folder_path, file)
            format_category_field(csv_path, field_name)
