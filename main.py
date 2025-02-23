import os
import pandas as pd
import re
import concurrent.futures
import fitz  # PyMuPDF
import logging
from src.text_cleaner import clean_text
from src.keyword_extractor import extract_keywords

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()


# 使用PyMuPDF进行PDF文本提取
def extract_text_from_pdf(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = ''
        for page in doc:
            text += page.get_text("text")  # 提取文本
        return text
    except Exception as e:
        logger.error(f"Error extracting text from {pdf_path}: {e}")
        return ""


# 处理单个PDF文件
def process_single_pdf(pdf_filename, pdf_folder, processed_files):
    # 使用正则表达式提取股票代码、公司名称和年份，兼容特殊字符和多个下划线
    match = re.match(r'(\d{6})_(.+?)_(\d{4})年.*?年度报告.*\.pdf', pdf_filename)
    if match:
        stock_code = match.group(1)  # 股票代码
        company_name = match.group(2)  # 公司名称
        year = match.group(3)  # 年份
    else:
        logger.warning(f"无法解析文件名: {pdf_filename}")
        return None

    # 获取PDF文件路径
    pdf_path = os.path.join(pdf_folder, pdf_filename)

    # Step 1: 提取PDF文本
    pdf_text = extract_text_from_pdf(pdf_path)

    # Step 2: 清理文本
    cleaned_text = clean_text(pdf_text)

    # Step 3: 提取关键词
    keywords = extract_keywords(cleaned_text)

    return stock_code, company_name, year, keywords


def process_pdf_files(pdf_folder, output_folder):
    all_keywords = []
    stock_codes = []  # 存储股票代码
    company_names = []  # 存储公司名
    years = []  # 存储年份

    # 获取所有PDF文件
    pdf_filenames = [f for f in os.listdir(pdf_folder) if f.endswith('.pdf')]
    total_files = len(pdf_filenames)

    logger.info(f"Total files to process: {total_files}")

    # 使用ThreadPoolExecutor来并行处理文件
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for pdf_filename in pdf_filenames:
            futures.append(executor.submit(process_single_pdf, pdf_filename, pdf_folder, None))

        # 收集处理结果
        processed_count = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            processed_count += 1
            if result:
                stock_code, company_name, year, keywords = result
                stock_codes.append(stock_code)
                company_names.append(company_name)
                years.append(year)
                all_keywords.append(keywords)

            # 输出进度日志
            logger.info(
                f"Processed {processed_count}/{total_files} files... Remaining: {total_files - processed_count}")

    # 将关键词字典转换为DataFrame，方便导出
    keyword_df = pd.DataFrame(all_keywords).fillna(0)

    # 确保所有频率为整数
    keyword_df = keyword_df.astype(int)

    # 添加股票代码、公司名和年份列
    keyword_df['Stock Code'] = stock_codes
    keyword_df['Company'] = company_names
    keyword_df['Year'] = years

    # 调整列顺序，将股票代码、公司名和年份放在前面
    keyword_df = keyword_df[['Stock Code', 'Company', 'Year'] + [col for col in keyword_df.columns if
                                                                 col not in ['Stock Code', 'Company', 'Year']]]

    # 删除重复的股票代码和年份，只保留第一个出现的记录
    keyword_df = keyword_df.drop_duplicates(subset=['Stock Code', 'Year'], keep='first')

    # 保存DataFrame到CSV
    output_path = os.path.join(output_folder, 'keywords_frequency.csv')
    keyword_df.to_csv(output_path, index=False)
    logger.info(f"Keywords data pool saved to: {output_path}")


# 设置目录
pdf_folder = 'data/pdf_files'
output_folder = 'data/output'

# 运行处理函数
process_pdf_files(pdf_folder, output_folder)
