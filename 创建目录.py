import os

def create_folders_from_file(input_file, base_path='.'):
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('|||')
            if len(parts) < 2:
                print(f"警告：该行目录层级不足两级，跳过：{line}")
                continue
            level1 = parts[0].strip()
            level2 = parts[1].strip()

            folder_path = os.path.join(base_path, level1, level2)
            os.makedirs(folder_path, exist_ok=True)
            print(f"[{os.path.basename(input_file)}] 已创建文件夹: {folder_path}")

def batch_process_folder(input_folder, output_base_path='.'):
    for filename in os.listdir(input_folder):
        if filename.endswith('.txt'):
            full_path = os.path.join(input_folder, filename)
            create_folders_from_file(full_path, output_base_path)

if __name__ == '__main__':
    input_folder = 'D:\project\municipal\分类'   # 这里改成你的txt文件所在文件夹路径
    output_base_path = 'D:\project\municipal\产品链接'     # 这里改成你想创建文件夹的根目录
    batch_process_folder(input_folder, output_base_path)
