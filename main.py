import os
import pandas as pd
import re
import concurrent.futures
import fitz  # PyMuPDF
from src.text_cleaner import clean_text
from src.keyword_extractor import extract_keywords

# 使用PyMuPDF进行PDF文本提取
def extract_text_from_pdf(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = ''
        for page in doc:
            text += page.get_text("text")  # 提取文本
        return text
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return ""

def process_single_pdf(pdf_filename, pdf_folder):
    # 使用正则表达式提取股票代码、公司名称和年份，兼容特殊字符和多个下划线
    match = re.match(r'(\d{6})_(.+?)_(\d{4})年.*?年度报告.*\.pdf', pdf_filename)
    if match:
        stock_code = match.group(1)  # 股票代码
        company_name = match.group(2)  # 公司名称
        year = match.group(3)  # 年份
    else:
        print(f"无法解析文件名: {pdf_filename}")
        return None

    # 获取PDF文件路径
    pdf_path = os.path.join(pdf_folder, pdf_filename)

    # Step 1: Extract text from PDF
    pdf_text = extract_text_from_pdf(pdf_path)

    # Step 2: Clean the extracted text
    cleaned_text = clean_text(pdf_text)

    # Step 3: Extract keywords
    keywords = extract_keywords(cleaned_text)

    return stock_code, company_name, year, keywords

def process_pdf_files(pdf_folder, output_folder):
    all_keywords = []
    stock_codes = []  # 存储股票代码
    company_names = []  # 存储公司名
    years = []  # 存储年份

    # 获取所有PDF文件
    pdf_filenames = [f for f in os.listdir(pdf_folder) if f.endswith('.pdf')]

    # 使用ThreadPoolExecutor来并行处理文件
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for pdf_filename in pdf_filenames:
            futures.append(executor.submit(process_single_pdf, pdf_filename, pdf_folder))

        # 收集处理结果
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                stock_code, company_name, year, keywords = result
                stock_codes.append(stock_code)
                company_names.append(company_name)
                years.append(year)
                all_keywords.append(keywords)

    # Convert list of keyword dictionaries to DataFrame for easy export
    keyword_df = pd.DataFrame(all_keywords).fillna(0)

    # Ensure all frequencies are integers
    keyword_df = keyword_df.astype(int)

    # Add Stock Code, Company, and Year columns
    keyword_df['Stock Code'] = stock_codes
    keyword_df['Company'] = company_names
    keyword_df['Year'] = years

    # Reorder the columns to place Stock Code, Company, and Year in the first three columns
    keyword_df = keyword_df[['Stock Code', 'Company', 'Year'] + [col for col in keyword_df.columns if col not in ['Stock Code', 'Company', 'Year']]]

    # Save DataFrame to CSV
    output_path = os.path.join(output_folder, 'keywords_frequency.csv')
    keyword_df.to_csv(output_path, index=False)
    print(f"Keywords data pool saved to: {output_path}")

# Set your directories
pdf_folder = 'data/pdf_files'
output_folder = 'data/output'

# Run the processing function
process_pdf_files(pdf_folder, output_folder)
