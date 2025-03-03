import csv
import requests
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# specify cloud platform
specify_cloud_platform = 'azure'


# framework
framework = 'cis'
new_csv_field_name = f'{framework}{specify_cloud_platform}Standard'


# 定义API的URL和认证令牌
api_url = 'https://cloud.fastgpt.cn/api/v1/chat/completions'
auth_token_file = f'{framework}_app_key'

# 定义CSV文件路径
test_csv_file_path = 'plugin_azure_failed.csv'
out_csv_file_path = f'plugin_with_{framework}_{specify_cloud_platform}_deepseek_reasoner.csv'

concurrent_num = 20
max_retries = 3  # 最大重试次数

def read_token_file(file_path):
    """从指定的文件中读取内容并返回。"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read().strip()  # 去掉多余的空格和换行
        return content
    except FileNotFoundError:
        return f"文件未找到: {file_path}"
    except IOError:
        return f"读取文件时发生错误: {file_path}"

def send_chat_request(name,api_url, auth_token, chat_id, user_message, stream=False, detail=False):
    """发送聊天请求到指定的API接口。"""
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json;  charset=utf-8'
    }

    data = {
        "chatId": chat_id,
        "stream": stream,
        "detail": detail,
        "messages": [
            {
                "role": "user",
                "content": user_message
            }
        ]
    }

    for attempt in range(max_retries):
        try:
            print(f"开始请求 {attempt} {name}")
            response = requests.post(api_url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"请求失败 {name} (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt + 1 == max_retries:  # 如果是最后一次尝试，返回错误信息
                return {"error": str(e)}
            time.sleep(2 ** attempt)  # 指数退避，增加重试间隔


def sanitize_input(text):
    """Remove or replace problematic characters from the input."""
    return text.encode('utf-8', 'replace').decode('utf-8')

def process_row(row, auth_token):
    """处理每一行数据并返回处理结果。"""

    name = row['name']
    scan_item = row['扫描项']
    rules = row['rules']
    cloud_platform = row['云平台']
    scan_type = row['扫描类型']
    content_description = row['内容描述']
    description = row['description']

    print(f"process row name:{name}")

    # 判断 cloud_platform 是否等于指定的平台，不区分大小写
    if cloud_platform.lower() != specify_cloud_platform.lower():
        print(f"跳过处理: {name}，因为云平台不匹配")
        return name, "云平台不匹配"
    
    prompt = sanitize_input(
        f"云服务检测项内容为：{cloud_platform} {name} {rules} {description},"
        #f"最匹配哪个CIS aws benchmark v3的recomandation."
        f"最匹配哪个CIS microsoft azure benchmark v3的recomandation. 要求recomandation所属的云产品必须是{scan_type}，没有匹配的返回'无对应云服务产品'."
    )

    #print(f"Sending prompt: {prompt}")  # Log the prompt   
    chat_id = str(int(time.time()))
    result = send_chat_request(name,api_url, auth_token, chat_id, prompt)

    if "error" in result:
        return name, "请求失败"
    
    try:
        content = result['choices'][0]['message']['content']
        return name, content.replace("'", '"')  # 直接返回响应内容
    except (KeyError, json.JSONDecodeError) as e:
        return name, "解析失败"

def read_file(csv_file_path, auth_token):
    """读取CSV文件并处理数据。"""
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=',')
        fieldnames = csv_reader.fieldnames + [new_csv_field_name]

        with open(out_csv_file_path, mode='w', newline='', encoding='utf-8') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=fieldnames, delimiter=',')
            writer.writeheader()

            rows = []
            for row in csv_reader:
                rows.append(row)
                if len(rows) == concurrent_num:  # 每次处理20行
                    with ThreadPoolExecutor(max_workers=concurrent_num) as executor:
                        futures = {executor.submit(process_row, r, auth_token): r for r in rows}
                        for future in as_completed(futures):
                            name, result = future.result()
                            print(f"处理完成: {name} - 结果: {result}")
                            row_result = futures[future]
                            row_result[new_csv_field_name] = result
                            writer.writerow(row_result)
                    rows = []  # 清空已处理的行

            # 处理剩余的行
            if rows:
                with ThreadPoolExecutor(max_workers=concurrent_num) as executor:
                    futures = {executor.submit(process_row, r, auth_token): r for r in rows}
                    for future in as_completed(futures):
                        name, result = future.result()
                        print(f"处理完成: {name} - 结果: {result}")
                        row_result = futures[future]
                        row_result[new_csv_field_name] = result
                        writer.writerow(row_result)

# 示例调用
if __name__ == "__main__":
    auth_token = read_token_file(auth_token_file)
    print(f"auth toke: {auth_token}")
    read_file(test_csv_file_path, auth_token)

