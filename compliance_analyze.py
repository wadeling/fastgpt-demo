import csv
import requests
import time
import json
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential

# 配置参数
SPECIFY_CLOUD_PLATFORM = 'ali'
FRAMEWORK = 'cis'
NEW_CSV_FIELD_NAME = f'{FRAMEWORK}{SPECIFY_CLOUD_PLATFORM}Standard'
API_URL = 'https://cloud.fastgpt.cn/api/v1/chat/completions'
AUTH_TOKEN_FILE = f'{FRAMEWORK}_app_key'
INPUT_CSV = 'plugin.csv'
OUTPUT_CSV = f'plugin_with_{FRAMEWORK}_{SPECIFY_CLOUD_PLATFORM}_deepseek_reasoner.csv'
CONCURRENT_NUM = 20
MAX_RETRIES = 3
REQUEST_TIMEOUT = 600
REQUIRED_FIELDS = ['name', '扫描项', 'rules', '云平台', '扫描类型', '内容描述', 'description']

class APIRequestError(Exception):
    """自定义API请求异常"""
    pass

def read_token_file(file_path: str) -> str:
    """安全读取认证令牌文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            token = f.read().strip()
            if not token:
                raise ValueError("Token文件内容为空")
            return token
    except FileNotFoundError:
        raise FileNotFoundError(f"Token文件未找到: {file_path}")
    except Exception as e:
        raise RuntimeError(f"读取Token文件失败: {str(e)}")

def sanitize_input(text: str) -> str:
    """规范化输入文本"""
    return unicodedata.normalize('NFKC', text).strip()

@retry(stop=stop_after_attempt(MAX_RETRIES), 
       wait=wait_exponential(multiplier=1, max=10))
def send_chat_request(name: str, auth_token: str, prompt: str) -> dict:
    """发送带重试机制的API请求"""
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json; charset=utf-8'
    }
    
    payload = {
        "chatId": str(int(time.time())),
        "stream": False,
        "detail": False,
        "messages": [{"role": "user", "content": prompt}]
    }

    try:
        response = requests.post(
            API_URL,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        err_msg = f"{type(e).__name__}: {str(e)}"
        print(f"API请求失败 [{name}]: {err_msg}")
        raise APIRequestError(err_msg)

def validate_row(row: dict):
    """验证CSV行数据完整性"""
    missing_fields = [f for f in REQUIRED_FIELDS if f not in row]
    if missing_fields:
        raise ValueError(f"缺少必要字段: {missing_fields}")
    
    if row['云平台'].lower() != SPECIFY_CLOUD_PLATFORM.lower():
        raise ValueError("云平台不匹配")

def process_row(row: dict, auth_token: str) -> tuple:
    """处理单行数据"""
    try:
        print(f"正在处理: {row['name']}")
        validate_row(row)
        
        prompt = sanitize_input(
            f"云服务检测项内容: {row['云平台']} {row['name']} "
            f"{row['rules']} {row['description']}\n"
            f"最匹配哪个CIS_Alibaba_Cloud_Foundation_Benchmark v1的推荐项？"
            f"要求云产品必须属于{row['扫描类型']}，无匹配则返回'无对应云服务产品'"
        )
        
        response = send_chat_request(row['name'], auth_token, prompt)
        
        if not isinstance(response, dict) or 'choices' not in response:
            return row['name'], "无效API响应结构"
        
        if len(response['choices']) == 0:
            return row['name'], "无返回结果"
            
        content = response['choices'][0].get('message', {}).get('content', '')
        return row['name'], content.replace("'", '"').strip()
    
    except Exception as e:
        print(f"处理失败 [{row['name']}]: {str(e)}")
        return row['name'], f"处理错误: {type(e).__name__}"

def process_batch(batch: list, auth_token: str) -> list:
    """并发处理批次数据并保持顺序"""
    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENT_NUM) as executor:
        future_to_row = {executor.submit(process_row, row, auth_token): row for row in batch}
        for future in future_to_row:
            try:
                results.append(future.result())
            except Exception as e:
                row_name = future_to_row[future]['name']
                results.append((row_name, f"执行错误: {str(e)}"))
    return results

def main():
    try:
        auth_token = read_token_file(AUTH_TOKEN_FILE)
        print("认证令牌验证成功 {auth_token}")

        with open(INPUT_CSV, 'r', encoding='utf-8') as f_in, \
             open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f_out:

            reader = csv.DictReader(f_in)
            if not all(field in reader.fieldnames for field in REQUIRED_FIELDS):
                missing = [f for f in REQUIRED_FIELDS if f not in reader.fieldnames]
                raise ValueError(f"CSV文件缺少必要字段: {missing}")

            writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames + [NEW_CSV_FIELD_NAME])
            writer.writeheader()

            batch = []
            for row in reader:
                # 创建新字典保留原始顺序
                processed_row = {k: v for k, v in row.items()}
                batch.append(processed_row)
                
                if len(batch) >= CONCURRENT_NUM:
                    batch_results = process_batch(batch, auth_token)
                    for (name, result), row in zip(batch_results, batch):
                        row[NEW_CSV_FIELD_NAME] = result
                        writer.writerow(row)
                    batch = []

            # 处理剩余批次
            if batch:
                batch_results = process_batch(batch, auth_token)
                for (name, result), row in zip(batch_results, batch):
                    row[NEW_CSV_FIELD_NAME] = result
                    writer.writerow(row)

        print("处理完成，结果已保存至:", OUTPUT_CSV)

    except Exception as e:
        print(f"程序执行失败: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
