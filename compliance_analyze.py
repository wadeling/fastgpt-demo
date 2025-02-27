import csv
import requests
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# 定义API的URL和认证令牌
api_url = 'https://cloud.fastgpt.cn/api/v1/chat/completions'
auth_token_file = './pci_app_key'

# 定义CSV文件路径
test_csv_file_path = 'plugin.csv'
out_csv_file_path = 'plugin_with_pci_qwen_2-5.csv'

concurrent_num = 20

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

def send_chat_request(api_url, auth_token, chat_id, user_message, stream=False, detail=False):
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

    try:
        response = requests.post(api_url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def sanitize_input(text):
    """Remove or replace problematic characters from the input."""
    return text.encode('utf-8', 'replace').decode('utf-8')

def process_row(row, auth_token):
    """处理每一行数据并返回处理结果。"""
    print(f"process row:{row}")

    name = row['name']
    scan_item = row['扫描项']
    rules = row['rules']
    cloud_platform = row['云平台']
    scan_type = row['扫描类型']
    content_description = row['内容描述']
    description = row['description']

    prompt = sanitize_input(
        f"一个检测项内容为：{name} {rules} {cloud_platform} {scan_type} {description} {content_description},"
        f"请参考知识库，判断这个云服务配置检测项属于哪些 PCI 标准（格式为：PCI DSS v a.b-x.y.z）,按这个格式返回: 所属标准 - 理由."
    )

    #print(f"Sending prompt: {prompt}")  # Log the prompt   
    chat_id = str(int(time.time()))
    result = send_chat_request(api_url, auth_token, chat_id, prompt)

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
        fieldnames = csv_reader.fieldnames + ['pciStandard']

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
                            row_result['pciStandard'] = result
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
                        row_result['pciStandard'] = result
                        writer.writerow(row_result)

# 示例调用
if __name__ == "__main__":
    auth_token = read_token_file(auth_token_file)
    print(f"auth toke: {auth_token}")
    read_file(test_csv_file_path, auth_token)

