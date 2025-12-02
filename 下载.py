import os
import requests
from urllib.parse import urlparse

def download_image(url, save_dir="."):
    try:
        # 解析 URL 获取文件名
        filename = os.path.basename(urlparse(url).path)
        save_path = os.path.join(save_dir, filename)

        # 发送 GET 请求
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

        # 保存图片
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)

        print(f"✅ 已保存: {save_path}")
    except Exception as e:
        print(f"❌ 下载失败: {e}")


if __name__ == "__main__":
    url = "https://m.media-amazon.com/images/S/gestalt-seller-images-prod-us-east-1/ATVPDKIKX0DER/A3BI9ERHVFO7XC/c4c528e2b9ea4a40fcd35ed38d751438.png"
    download_image(url, save_dir=".")
