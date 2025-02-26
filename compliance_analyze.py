import csv
import requests
import time
import json

# 定义API的URL和认证令牌
api_url = 'https://cloud.fastgpt.cn/api/v1/chat/completions'
auth_token = ''

# 定义CSV文件路径
test_csv_file_path = 'plugin.csv'

out_csv_file_path = 'plugin_with_iso_qwen_turbo.csv'

def send_chat_request(api_url, auth_token, chat_id, user_message, stream=False, detail=False):
    """
    发送聊天请求到指定的API接口。

    :param api_url: API的URL
    :param auth_token: 认证令牌（Bearer Token）
    :param chat_id: 聊天会话ID
    :param user_message: 用户发送的消息
    :param stream: 是否启用流式响应（默认True）
    :param detail: 是否返回详细信息（默认True）
    :return: 响应内容（JSON格式）或错误信息
    """
    # 定义请求头
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }

    # 定义请求体（JSON数据）
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

    response = None  # Initialize response variable
    try:
        # 发送POST请求
        response = requests.post(api_url, headers=headers, json=data)
        response.raise_for_status()  # 检查响应状态码，如果不是200会抛出异常
        return response.json()  # 返回JSON格式的响应内容
    except requests.exceptions.RequestException as e:
        # 处理请求异常
        return {"error": str(e), "status_code": response.status_code if response else None}

def read_file(csv_file_path):
    # 打开输入CSV文件并读取内容
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        # 创建CSV读取器
        csv_reader = csv.DictReader(file, delimiter=',')

        # 定义输出文件的表头
        fieldnames = csv_reader.fieldnames + ['isoStandard']

        # 打开输出CSV文件并写入数据
        with open(out_csv_file_path, mode='w', newline='', encoding='utf-8') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=fieldnames, delimiter=',')
            writer.writeheader()  # 写入表头

            # 遍历每一行
            for row in csv_reader:
                # 读取所需的列
                name = row['name']
                scan_item = row['扫描项']
                rules = row['rules']
                cloud_platform = row['云平台']
                scan_type = row['扫描类型']
                content_description = row['内容描述']
                description = row['description']

                prompt = (
                    f"一个检测项内容为：{name} {rules} {cloud_platform} {scan_type} {description} {content_description},"
                    #f"请参考知识库，判断这个云服务配置检测项属于哪些 iso 标准（格式为：ISO/IEC 270xx:xxxx-x.y.z）。按这个格式返回：[{{'isoStandard':'', 'reason':''}}]."
                    f"请参考知识库，判断这个云服务配置检测项属于哪些 iso 标准（格式为：ISO/IEC 270xx:xxxx-x.y.z）,按这个格式返回: 所属标准 - 理由."
                )

                print(f"start deal: {name}")

                # 调用函数发送请求
                chat_id = str(int(time.time()))
                result = send_chat_request(api_url, auth_token, chat_id, prompt)

                # 处理响应结果
                if "error" in result:
                    print(f"请求失败: {result['error']}")
                    iso_reason = "请求失败"
                else:
                    print("请求成功！")
                    print("响应内容:", result)

                    # 解析 choices.message.content 中的 JSON 数据
                    try:
                        content = result['choices'][0]['message']['content']
                        content = content.replace("'", '"')
                        #iso_list = json.loads(content)  # 将字符串解析为 Python 对象
                        #iso_reason = "\n".join([f"{item['isoStandard']} - {item['reason']}" for item in iso_list])
                        iso_reason = content
                    except (KeyError, json.JSONDecodeError) as e:
                        print(f"解析响应内容失败: {e}")
                        iso_reason = "解析失败"

                print(f"end deal: {name}")

                # 将结果写入新行
                row['isoStandard'] = iso_reason
                writer.writerow(row)

                #break  # 调试时只处理一行数据，完成后可以去掉此行

# 示例调用
if __name__ == "__main__":

    read_file(test_csv_file_path)

    
