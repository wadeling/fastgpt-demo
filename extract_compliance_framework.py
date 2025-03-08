import pandas as pd
import numpy as np  # 如果需要使用np.nan，可以保留这一行

# 框架名称
framework = 'hipaa'

# 读取CSV文件
input_file = 'prisma_cloud_simple_policies.csv'  # 输入文件名
output_file = f'{framework}_output.csv'  # 输出文件名

# 加载数据
df = pd.read_csv(input_file)

# 创建一个新的列 'cisStandard' 用于存储提取的结果
df[f'{framework}Standard'] = df['Compliance Standard'].apply(
    lambda x: ', '.join([
        item.strip() for item in x.split(',') 
        if item.strip().lower().startswith(framework)  
    ]) if any(item.strip().lower().startswith(framework) for item in x.split(',')) else np.nan
)

# 过滤掉没有以 'cis' 开头的标准的行
df_filtered = df[df[f'{framework}Standard'].notna()]

# 将结果写入新的CSV文件，包含原有的所有表头
df_filtered.to_csv(output_file, index=False)

