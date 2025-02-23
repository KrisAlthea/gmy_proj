import os
import pandas as pd
import re
from src.pdf_text_extractor import extract_text_from_pdf
from src.text_cleaner import clean_text
from src.keyword_extractor import extract_keywords


def process_pdf_files(pdf_folder, output_folder):
    all_keywords = []
    stock_codes = []  # 存储股票代码
    company_names = []  # 存储公司名
    years = []  # 存储年份

    for pdf_filename in os.listdir(pdf_folder):
        if pdf_filename.endswith('.pdf'):
            # 使用正则表达式提取股票代码、公司名称和年份，兼容特殊字符和多个下划线
            match = re.match(r'(\d{6})_(.+?)_(\d{4})年.*?年度报告.*\.pdf', pdf_filename)
            if match:
                stock_code = match.group(1)  # 股票代码
                company_name = match.group(2)  # 公司名称
                year = match.group(3)  # 年份
            else:
                print(f"无法解析文件名: {pdf_filename}")
                continue

            pdf_path = os.path.join(pdf_folder, pdf_filename)
            print(f"Processing: {pdf_filename}")

            # Step 1: Extract text from PDF
            pdf_text = extract_text_from_pdf(pdf_path)

            # Step 2: Clean the extracted text
            cleaned_text = clean_text(pdf_text)

            # Step 3: Extract keywords
            keywords = extract_keywords(cleaned_text)

            # Step 4: Store keywords along with stock code, company name, and year
            stock_codes.append(stock_code)
            company_names.append(company_name)
            years.append(year)
            all_keywords.append(keywords)

    # Convert list of keyword dictionaries to DataFrame for easy export
    # Flatten the nested dictionary to ensure every keyword has its own column
    keyword_df = pd.DataFrame(all_keywords).fillna(0)

    # Ensure all frequencies are integers
    keyword_df = keyword_df.astype(int)

    # Add Stock Code, Company, and Year columns
    keyword_df['Stock Code'] = stock_codes
    keyword_df['Company'] = company_names
    keyword_df['Year'] = years

    # Reorder the columns to place Stock Code, Company, and Year in the first three columns
    keyword_df = keyword_df[['Stock Code', 'Company', 'Year'] + [col for col in keyword_df.columns if
                                                                 col not in ['Stock Code', 'Company', 'Year']]]

    # Save DataFrame to CSV
    output_path = os.path.join(output_folder, 'keywords_frequency.csv')
    keyword_df.to_csv(output_path, index=False)
    print(f"Keywords data pool saved to: {output_path}")


# Set your directories
pdf_folder = 'data/pdf_files'
output_folder = 'data/output'

# Run the processing function
process_pdf_files(pdf_folder, output_folder)
