import os

# 📁 设置包含所有 .txt 文件的目录（源目录）
txt_folder = r'D:\project\municipal\分类'

# 📁 设置所有 HTML 输出的总目录（你想放输出文件的地方）
output_base_dir = r'D:\project\municipal\分类html'
os.makedirs(output_base_dir, exist_ok=True)

# 遍历 txt 文件夹中的所有 .txt 文件
for file in os.listdir(txt_folder):
    if file.lower().endswith('.txt'):
        txt_path = os.path.join(txt_folder, file)
        folder_name = os.path.splitext(file)[0]  # 不含扩展名
        output_dir = os.path.join(output_base_dir, folder_name)
        os.makedirs(output_dir, exist_ok=True)

        with open(txt_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        count = 0
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # 替换非法字符
            file_name = line.replace('|||', '_') + '.html'
            file_name = file_name.replace('/', '-').replace('\\', '-').replace(':', '-')
            file_name = file_name.replace('*', '-').replace('?', '-').replace('"', "'")
            file_name = file_name.replace('<', '-').replace('>', '-').replace('|', '-')

            html_path = os.path.join(output_dir, file_name)

            with open(html_path, 'w', encoding='utf-8') as html_file:
                html_file.write('')  # 你可以写模板内容
            count += 1

        print(f'✅ {file} → 生成 {count} 个 HTML 文件到目录：{output_dir}')
