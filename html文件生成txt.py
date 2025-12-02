import os
from bs4 import BeautifulSoup

# 设置 HTML 文件所在目录
# 获取所有 html 文件
input_folder = 'D:\project\municipal\分类html\men'
output_folder = 'D:\project\municipal\产品链接\men'

# 确保输出目录存在
os.makedirs(output_folder, exist_ok=True)

# 获取所有 html 文件
html_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.html')]

# 设置前缀
base_url = 'https://www.fredericks.com'

# 遍历处理每个文件
for filename in html_files:
    input_path = os.path.join(input_folder, filename)
    
    # 读取 HTML 内容
    with open(input_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
        links = [a['href'] for a in soup.find_all('a', href=True)]
        unique_links = sorted(set(base_url + link for link in links))

    # 输出文件名：保留原文件名但修改扩展名为 .txt
    base_name = os.path.splitext(filename)[0]
    output_filename = f"{base_name}.txt"
    output_path = os.path.join(output_folder, output_filename)

    # 写入链接
    with open(output_path, 'w', encoding='utf-8') as f:
        for link in unique_links:
            f.write(link + '\n')

    print(f'✅ 处理完成：{filename} → {output_filename}')
