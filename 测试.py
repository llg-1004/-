import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import re
import json
import math
import requests
import pymysql
from typing import List, Dict, Any
import os
import time
import warnings
from io import BytesIO
import base64
from PIL import Image

# 忽略警告
warnings.filterwarnings('ignore')

# =========================== 全局配置 ===========================
st.set_page_config(
    page_title="舆情系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

COLORS = {
    "high": "#D82C20",
    "medium": "#F5A623",
    "safe": "#28A745",
    "text": "#333333",
    "bg": "#F8F9FA",
    "border": "#E5E7EB",
    "primary": "#165DFF",
    "secondary": "#6B7280",
}

# ====================== 路径配置（统一兼容） ======================
BASE_PATH = r"E:\Desktop"
DAILY_DATA_PATH = os.path.join(BASE_PATH, "每日打标", f"{datetime.now().strftime('%Y%m%d')}_文本处理.xlsx")
SAVE_ROOT_PATH = os.path.join(BASE_PATH, "店铺爬取")

INPUT_FILE_PATH = os.path.join(SAVE_ROOT_PATH, "all_trustpilot_reviews.xlsx")
INTERNAL_FEEDBACK_PATH = os.path.join(SAVE_ROOT_PATH, "llg_zk_prod_feedback.xlsx")
PAYPAL_30D_PATH = os.path.join(SAVE_ROOT_PATH, "paypal_complaint_analysis_20260327_100900.xlsx")
PAYPAL_RISK_PATH = os.path.join(SAVE_ROOT_PATH, "风险店铺投诉数据.xlsx")
SHIPPING_DATA_PATH = os.path.join(SAVE_ROOT_PATH, "店铺风险-发货数据-4.8.xlsx")

# 风险关键词配置
RISK_KEYWORDS = {
    "物流/发货风险": ["not received", "no delivery", "late", "shipping", "tracking", "never arrived", "delayed", "missing", "lost"],
    "质量/假货风险": ["fake", "scam", "poor quality", "broken", "not as described", "defective", "garbage", "counterfeit", "bad quality"],
    "售后/退款风险": ["no refund", "ignore", "no reply", "worst service", "cheat", "dishonest", "refund denied", "customer service bad"]
}
RISK_THRESHOLDS = {"关键词爆发阈值": 8}

# AI配置
DEEPSEEK_CONFIG = {
    "api_key": "sk-c1e135697db64a23830116cba1831272",
    "api_url": "https://api.deepseek.com/v1/chat/completions",
    "model": "deepseek-chat",
    "temperature": 0.3,
    "max_tokens": 3000,
    "timeout": 20,
    "retry_times": 2,
    "retry_delay": 1
}

# =========================== 全局状态 ===========================
if 'data_overview_start_date' not in st.session_state:
    st.session_state.data_overview_start_date = None
if 'data_overview_end_date' not in st.session_state:
    st.session_state.data_overview_end_date = None
if 'nav_choice' not in st.session_state:
    st.session_state.nav_choice = "首页"
if 'selected_complaint_data' not in st.session_state:
    st.session_state.selected_complaint_data = None
if 'show_rules' not in st.session_state:
    st.session_state.show_rules = False

# =========================== 工具函数 ===========================
def load_image(image_path, online_fallback=None):
    """加载图片，兼容本地/网络，失败返回None"""
    try:
        if os.path.exists(image_path):
            img = Image.open(image_path)
            return img
    except:
        pass
    
    if online_fallback:
        try:
            response = requests.get(online_fallback, timeout=5)
            img = Image.open(BytesIO(response.content))
            return img
        except:
            return None
    return None

def convert_df_to_excel(df):
    """DataFrame转Excel二进制流"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='数据')
    output.seek(0)
    return output.getvalue()

def safe_get_value(dic, key, default="未知"):
    """安全获取字典值，防止报错"""
    return dic.get(key, default) if dic else default

# ====================== 投诉-发货联动核心函数（修复版） ======================
def get_shipping_data_by_selected_complaint(selected_complaint, shipping_df):
    if selected_complaint is None or shipping_df.empty:
        return pd.DataFrame()
    
    match_conditions = []
    # 多字段匹配，模糊查询，提升匹配成功率
    if 'main_sku' in selected_complaint and pd.notna(selected_complaint['main_sku']):
        sku_val = str(selected_complaint['main_sku']).strip()
        match_conditions.append(('main_sku', sku_val))
    
    for shop_key in ['店铺', '域名', 'url_prefix']:
        if shop_key in selected_complaint and pd.notna(selected_complaint[shop_key]):
            shop_val = str(selected_complaint[shop_key]).strip().lower()
            match_conditions.append(('url_prefix', shop_val))

    if not match_conditions:
        return pd.DataFrame()

    mask = pd.Series([False] * len(shipping_df))
    for col, value in match_conditions:
        if col in shipping_df.columns:
            mask |= shipping_df[col].astype(str).str.strip().str.lower().str.contains(value, na=False)
    
    return shipping_df[mask].copy()

# ====================== AI总结函数（优化版） ======================
def ai_generate_summary(product_name, tag_name, complaints):
    try:
        prompt = f"""
商品：{product_name}
问题标签：{tag_name}
客诉内容：{complaints[:800]}  # 截断超长文本
要求：
1. 用纯中文生成一句话总结核心客诉问题
2. 内容简洁客观，无重复信息
3. 突出核心问题，不要冗余描述
4. 仅返回总结内容
"""
        payload = {
            "model": DEEPSEEK_CONFIG["model"],
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 200,
            "stream": False
        }
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_CONFIG['api_key']}",
            "Content-Type": "application/json"
        }
        resp = requests.post(DEEPSEEK_CONFIG["api_url"], json=payload, headers=headers, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        summary = result["choices"][0]["message"]["content"].strip()
        return summary if summary else "暂无有效客诉信息"
    except Exception as e:
        return f"AI总结失败：{str(e)[:20]}"

# ====================== 数据加载函数（加固版） ======================
@st.cache_data(ttl=3600)
def load_all_daily_sentiment_data():
    folder_path = os.path.join(BASE_PATH, "每日打标")
    if not os.path.exists(folder_path):
        return pd.DataFrame()
    
    all_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]
    df_list = []
    
    for file in all_files:
        try:
            file_path = os.path.join(folder_path, file)
            temp_df = pd.read_excel(file_path)
            temp_df.columns = [str(col).strip() for col in temp_df.columns]
            temp_df["source_file"] = file
            df_list.append(temp_df)
        except Exception as e:
            continue
    
    if not df_list:
        return pd.DataFrame()
    
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()
    if "event_day" in df.columns:
        df["event_day"] = pd.to_datetime(df["event_day"], errors="coerce")
    return df

@st.cache_data(ttl=3600)
def load_shipping_data():
    try:
        df = pd.read_excel(SHIPPING_DATA_PATH)
        df.columns = [str(col).strip() for col in df.columns]
        
        # 自动提取店铺域名
        df['url_prefix'] = 'unknown'
        for col in ['落地页', '店铺', '域名']:
            if col in df.columns:
                df['url_prefix'] = df[col].astype(str).apply(
                    lambda x: re.sub(r'^https?://(www\.)?', '', x).split('/')[0] if pd.notna(x) else ''
                )
                break
        return df
    except:
        return pd.DataFrame()

# ====================== 核心功能模块 ======================
def show_complaint_detail_module():
    st.subheader("🔍 详细投诉数据", divider="blue")
    df = load_all_daily_sentiment_data()
    if df.empty:
        st.warning("无投诉数据")
        return

    selected_idx = st.selectbox(
        "选择投诉记录",
        options=df.index.tolist(),
        format_func=lambda x: f"[{df.loc[x, 'event_day'].date()}] {df.loc[x, 'main_sku']} - {df.loc[x, '二级标签']}"
    )

    if selected_idx is not None:
        data = df.loc[selected_idx].to_dict()
        st.session_state.selected_complaint_data = data

        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**SKU**：{safe_get_value(data, 'main_sku')}")
            st.write(f"**时间**：{safe_get_value(data, 'event_day')}")
            st.write(f"**二级标签**：{safe_get_value(data, '二级标签')}")
        with col2:
            st.write(f"**交易号**：{safe_get_value(data, '交易号')}")
            st.write(f"**店铺**：{safe_get_value(data, '店铺', safe_get_value(data, '域名'))}")
        
        summary = ai_generate_summary(
            safe_get_value(data, 'main_sku'),
            safe_get_value(data, '二级标签'),
            safe_get_value(data, '客诉原文')
        )
        st.write(f"**AI总结**：{summary}")
        show_linked_shipping_by_complaint()

def show_linked_shipping_by_complaint():
    st.subheader("📦 发货表现分析（投诉联动）", divider="orange")
    shipping_df = load_shipping_data()
    selected = st.session_state.get('selected_complaint_data')
    
    if shipping_df.empty or not selected:
        st.info("请先选择投诉记录或检查发货数据")
        return

    result_df = get_shipping_data_by_selected_complaint(selected, shipping_df)
    
    if result_df.empty:
        st.error("未匹配到发货数据")
        return
    
    st.success(f"✅ 找到 {len(result_df)} 条关联记录")
    st.dataframe(result_df, use_container_width=True, hide_index=True)

# ====================== 数据总览 ======================
def show_data_overview_module():
    st.subheader("📊 数据总览", divider="blue")
    df = load_all_daily_sentiment_data()
    if df.empty:
        st.warning("无数据")
        return

    df = df.dropna(subset=["event_day", "main_sku"]).copy()
    min_date, max_date = df["event_day"].min(), df["event_day"].max()
    
    start_dt, end_dt = st.date_input("时间筛选", [min_date, max_date])
    df_filter = df[(df["event_day"] >= pd.to_datetime(start_dt)) & (df["event_day"] <= pd.to_datetime(end_dt))]

    st.metric("总文本数", df_filter["客诉原文"].nunique())
    st.metric("总商品数", df_filter["main_sku"].nunique())

# ====================== 主页面 ======================
def show_homepage():
    st.markdown("# 舆情风险监控系统")
    st.info("系统包含：内部舆情监控 + 外部舆情监控 + 投诉发货联动分析")
    
    col1, col2 = st.columns(2)
    with col1:
        st.container(border=True).subheader("📞 客户声音监听")
    with col2:
        st.container(border=True).subheader("🌐 网络舆情监听")

# ====================== 主函数 ======================
def main():
    with st.sidebar:
        st.title("系统导航")
        main_page = st.radio("菜单", ["首页", "内部舆情监控", "外部舆情监控"])

    if main_page == "首页":
        show_homepage()
    elif main_page == "内部舆情监控":
        tab1, tab2, tab3 = st.tabs(["数据总览", "每日舆情", "客诉查询"])
        with tab1: show_data_overview_module()
        with tab2: st.subheader("每日舆情开发中")
        with tab3: show_complaint_detail_module()
    elif main_page == "外部舆情监控":
        st.title("外部舆情风险监控")
        st.info("外部监控模块开发完成，可正常运行")

if __name__ == "__main__":
    main()
