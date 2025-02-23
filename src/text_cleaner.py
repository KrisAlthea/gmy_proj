import re

def clean_text(text):
    cleaned_text = re.sub(r'\s+', ' ', text)  # 替换多余的空白字符为一个空格
    cleaned_text = cleaned_text.strip()  # 去掉前后空格
    return cleaned_text
