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

# =========================== 修复：缺失的 load_image 函数 ===========================
from PIL import Image
import requests
from io import BytesIO

def load_image(image_path, online_fallback=None):
    """
    加载本地图片，失败则加载网络图片，兼容Streamlit展示
    """
    try:
        img = Image.open(image_path)
        return img
    except:
        if online_fallback:
            try:
                response = requests.get(online_fallback)
                img = Image.open(BytesIO(response.content))
                return img
            except:
                return None
        else:
            return None
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

# ====================== 每日舆情自动日期路径 ======================
# ====================== 【GitHub 云部署专用路径】复制我 ======================
import datetime
today_date = datetime.datetime.now().strftime("%Y%m%d")

# 1. 每日打标文件夹（GitHub 上的文件夹）
DAILY_DATA_PATH = f"./每日打标/{today_date}_文本处理.xlsx"

# 2. 店铺爬取文件夹（GitHub 上的文件夹）
SAVE_ROOT_PATH = "./店铺爬取/"

# 3. 店铺爬取里的所有文件（固定名字，不要改）
INPUT_FILE_PATH = SAVE_ROOT_PATH + "all_trustpilot_reviews.xlsx"
INTERNAL_FEEDBACK_PATH = SAVE_ROOT_PATH + "llg_zk_prod_feedback.xlsx"
PAYPAL_30D_PATH = SAVE_ROOT_PATH + "paypal_complaint_analysis_20260327_100900.xlsx"
PAYPAL_RISK_PATH = SAVE_ROOT_PATH + "风险店铺投诉数据.xlsx"
SHIPPING_DATA_PATH = SAVE_ROOT_PATH + "店铺风险-发货数据-4.8.xlsx"

RISK_KEYWORDS = {
    "物流/发货风险": ["not received", "no delivery", "late", "shipping", "tracking", "never arrived", "delayed",
                      "missing", "lost"],
    "质量/假货风险": ["fake", "scam", "poor quality", "broken", "not as described", "defective", "garbage",
                      "counterfeit", "bad quality"],
    "售后/退款风险": ["no refund", "ignore", "no reply", "worst service", "cheat", "dishonest", "refund denied",
                      "customer service bad"]
}
RISK_THRESHOLDS = {"关键词爆发阈值": 8}

DEEPSEEK_CONFIG = {
    "api_key": "sk-c1e135697db64a23830116cba1831272",
    "api_url": "https://api.deepseek.com/v1/chat/completions",
    "model": "deepseek-chat",
    "temperature": 0.3,
    "max_tokens": 3000,
    "timeout": 60,
    "retry_times": 3,
    "retry_delay": 2
}

# =========================== 全局变量 ===========================
if 'data_overview_start_date' not in st.session_state:
    st.session_state.data_overview_start_date = None
if 'data_overview_end_date' not in st.session_state:
    st.session_state.data_overview_end_date = None
if 'nav_choice' not in st.session_state:
    st.session_state.nav_choice = "首页"
if 'selected_complaint_data' not in st.session_state:
    st.session_state.selected_complaint_data = None
if 'selected_sku_teams' not in st.session_state:
    st.session_state.selected_sku_teams = []
if 'show_rules' not in st.session_state:
    st.session_state.show_rules = False

# =========================== 工具函数 ===========================
def convert_df_to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='数据')
    output.seek(0)
    return output.getvalue()

# ====================== 投诉-发货联动核心函数 ======================
def get_shipping_data_by_selected_complaint(selected_complaint, shipping_df):
    if selected_complaint is None or shipping_df.empty:
        return pd.DataFrame()
    match_keys = []
    if 'main_sku' in selected_complaint and pd.notna(selected_complaint['main_sku']):
        match_keys.append(('main_sku', selected_complaint['main_sku']))
    if '店铺' in selected_complaint and pd.notna(selected_complaint['店铺']):
        match_keys.append(('url_prefix', selected_complaint['店铺']))
    if '域名' in selected_complaint and pd.notna(selected_complaint['域名']):
        match_keys.append(('url_prefix', selected_complaint['域名']))
    if not match_keys:
        return pd.DataFrame()
    mask = pd.Series([False] * len(shipping_df))
    for col, value in match_keys:
        if col in shipping_df.columns:
            mask |= shipping_df[col].astype(str).str.contains(str(value), na=False, regex=False)
    return shipping_df[mask].copy()

# ====================== 详细投诉数据展示模块 ======================
def show_complaint_detail_module():
    st.subheader("🔍 详细投诉数据", divider="blue")
    df = load_all_daily_sentiment_data()
    if df.empty:
        st.warning("无投诉数据，请检查每日打标文件夹路径")
        return
    st.markdown("### 投诉数据列表")
    select_options = df.index.tolist()
    select_format = lambda x: f"[{df.loc[x, 'event_day'].date()}] {df.loc[x, 'main_sku']} - {df.loc[x, '二级标签']}"
    selected_idx = st.selectbox("选择投诉记录（联动发货表现分析）", options=select_options, format_func=select_format, key="complaint_select")
    if selected_idx is not None:
        selected_data = df.loc[selected_idx].to_dict()
        st.session_state.selected_complaint_data = selected_data
        st.markdown("### 选中的投诉详情")
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**商品SKU**：{selected_data.get('main_sku', '未知')}")
            st.write(f"**投诉时间**：{selected_data.get('event_day', '未知')}")
            st.write(f"**二级标签**：{selected_data.get('二级标签', '未知')}")
            st.write(f"**三级标签**：{selected_data.get('三级标签', '未知')}")
        with col2:
            st.write(f"**交易号**：{selected_data.get('交易号', '未知')}")
            st.write(f"**店铺/域名**：{selected_data.get('店铺', selected_data.get('域名', '未知'))}")
            st.write(f"**客诉原文**：{selected_data.get('客诉原文', '未知')[:200]}...")
        summary = ai_generate_summary(selected_data.get('main_sku', '未知'), selected_data.get('二级标签', '未知'), selected_data.get('客诉原文', '未知'))
        st.write(f"**AI核心总结**：{summary}")
        st.divider()
        show_linked_shipping_by_complaint()

# ====================== 投诉-SKU联动发货表现模块 ======================
def show_linked_shipping_by_complaint():
    st.subheader("📦 发货表现分析（投诉联动）", divider="orange")
    shipping_df = load_shipping_data()
    if shipping_df.empty:
        st.warning(f"未加载到发货数据，请检查路径：{SHIPPING_DATA_PATH}")
        return
    selected_complaint = st.session_state.get('selected_complaint_data')
    if not selected_complaint:
        st.info("👆 请先在上方【详细投诉数据】中选择一条记录")
        return
    source_sku = selected_complaint.get('main_sku', '') or selected_complaint.get('产品SKU', '')
    source_team = selected_complaint.get('运营团队', '') or selected_complaint.get('opera_team', '')
    with st.expander("🔍 点击查看匹配调试信息", expanded=True):
        st.write("**投诉数据中所有可用字段：**", list(selected_complaint.keys()))
        st.write(f"**提取的匹配值：** SKU = `{source_sku}` | 运营团队 = `{source_team}`")
        st.write("**发货表字段列表：**", shipping_df.columns.tolist())
    shipping_df.columns = [str(col).strip() for col in shipping_df.columns]
    mask = pd.Series([True] * len(shipping_df))
    match_log = []
    if pd.notna(source_sku) and str(source_sku).strip() != '':
        val_sku = str(source_sku).strip()
        sku_matched = False
        for col in ['产品SKU', 'main_sku', 'sku', '商品SKU']:
            if col in shipping_df.columns:
                mask &= shipping_df[col].astype(str).str.strip() == val_sku
                sku_matched = True
                match_log.append(f"✅ 使用字段 [{col}] 匹配 SKU: {val_sku}")
                break
        if not sku_matched:
            match_log.append("❌ 发货表中未找到 SKU 字段")
            mask &= False
    else:
        match_log.append("⚠️ 投诉数据中无 SKU")
        mask &= False
    if pd.notna(source_team) and str(source_team).strip() != '':
        val_team = str(source_team).strip()
        team_matched = False
        for col in ['运营团队', 'opera_team', 'team']:
            if col in shipping_df.columns:
                if not shipping_df[col].isna().all():
                    mask &= shipping_df[col].astype(str).str.strip() == val_team
                    match_log.append(f"✅ 使用字段 [{col}] 匹配 运营团队: {val_team}")
                    team_matched = True
                    break
        if not team_matched and '运营团队' in shipping_df.columns:
            match_log.append("ℹ️ 发货表有运营团队字段，但为空或不匹配，已忽略团队条件")
    result_df = shipping_df[mask].copy()
    with st.expander("📝 匹配步骤日志", expanded=False):
        for log in match_log:
            st.write(log)
    if result_df.empty:
        st.error("❌ 未找到匹配的发货数据。请查看上方调试信息核对字段名和值。")
    else:
        st.success(f"✅ 找到 {len(result_df)} 条关联发货记录")
        st.dataframe(result_df, use_container_width=True, hide_index=True)

# ====================== AI总结函数 ======================
def ai_generate_summary(product_name, tag_name, complaints):
    try:
        prompt = f"""
商品：{product_name}
问题标签：{tag_name}
客诉内容：{complaints[:1000]}
要求：
1. 用纯中文生成一句话总结核心客诉问题
2. 内容简洁客观，无重复信息
3. 突出核心问题，不要冗余描述
4. 仅返回总结内容，不要其他解释
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
        resp = requests.post(DEEPSEEK_CONFIG["api_url"], json=payload, headers=headers, timeout=15)
        result = resp.json()
        summary = result["choices"][0]["message"]["content"].strip()
        summary = re.sub(r'重复|多次|屡次|频繁等', '', summary)
        return summary if summary else "暂无有效客诉信息"
    except Exception as e:
        st.warning(f"AI总结生成失败：{str(e)}")
        return "AI总结生成失败"

# ====================== 数据总览模块 ======================
@st.cache_data(ttl=3600)
def load_all_daily_sentiment_data():
    import os
    import glob
    folder_path = r"E:\Desktop\每日打标"
    all_files = glob.glob(os.path.join(folder_path, "*.xlsx"))
    if not all_files:
        return pd.DataFrame()
    df_list = []
    for file in all_files:
        try:
            temp_df = pd.read_excel(file)
            temp_df.columns = [str(col).strip() for col in temp_df.columns]
            temp_df["source_file"] = os.path.basename(file)
            df_list.append(temp_df)
        except Exception as e:
            st.warning(f"读取文件失败：{os.path.basename(file)}，错误：{str(e)}")
            continue
    if not df_list:
        return pd.DataFrame()
    df = pd.concat(df_list, ignore_index=True)
    df = df.drop_duplicates()
    if "event_day" in df.columns:
        df["event_day"] = pd.to_datetime(df["event_day"], errors="coerce")
        df = df.sort_values("event_day").reset_index(drop=True)
    return df

def show_data_overview_module():
    st.subheader("📊 数据总览", divider="blue")
    df = load_all_daily_sentiment_data()
    if df.empty:
        st.warning("未读取到任何每日舆情数据")
        return
    required_cols = ["event_day", "二级标签", "三级标签", "客诉原文", "main_sku"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.warning(f"缺少关键字段：{', '.join(missing)}")
        return
    df = df.dropna(subset=["event_day", "main_sku"]).copy()
    df["event_day"] = pd.to_datetime(df["event_day"], errors="coerce")
    min_date = df["event_day"].min()
    max_date = df["event_day"].max()
    if st.session_state.data_overview_start_date is None:
        st.session_state.data_overview_start_date = min_date
    if st.session_state.data_overview_end_date is None:
        st.session_state.data_overview_end_date = max_date
    col1, _ = st.columns(2)
    with col1:
        start_dt, end_dt = st.date_input("⏰ 时间筛选", [st.session_state.data_overview_start_date, st.session_state.data_overview_end_date], min_value=min_date, max_value=max_date, key="data_overview_date_filter")
    st.session_state.data_overview_start_date = pd.to_datetime(start_dt)
    st.session_state.data_overview_end_date = pd.to_datetime(end_dt)
    start_dt = pd.to_datetime(start_dt)
    end_dt = pd.to_datetime(end_dt)
    df_filter = df[(df["event_day"] >= start_dt) & (df["event_day"] <= end_dt)].copy()
    if df_filter.empty:
        st.info("当前筛选条件下无数据")
        return
    total_text = df_filter["客诉原文"].nunique()
    total_product = df_filter["main_sku"].nunique()
    c1, c2 = st.columns(2)
    with c1:
        st.metric("总文本数", total_text)
    with c2:
        st.metric("总商品数", total_product)
    st.divider()
    col_q, col_l, col_o = st.columns(3)
    with col_q:
        st.markdown("### 🔴 产品质量问题 TOP10")
        quality_tags = ["功能问题", "尺寸问题", "质量问题"]
        df_q = df_filter[df_filter["二级标签"].isin(quality_tags)].copy()
        if not df_q.empty:
            top_q = df_q.groupby("main_sku").size().reset_index(name="标签数量")
            top_q = top_q.sort_values("标签数量", ascending=False).head(10)
            res_q = []
            for _, row in top_q.iterrows():
                sku = row["main_sku"]
                cnt = row["标签数量"]
                complaints = " | ".join(df_q[df_q["main_sku"] == sku]["客诉原文"].fillna("").astype(str).unique())
                summary = ai_generate_summary(sku, "产品质量问题", complaints)
                res_q.append({"商品名称": sku, "标签数量": cnt, "AI客诉总结": summary})
            st.dataframe(pd.DataFrame(res_q), use_container_width=True, hide_index=True)
        else:
            st.info("无产品质量数据")
    with col_l:
        st.markdown("### 🟠 未收到货问题 TOP10")
        logistics_tags = ["物流问题"]
        df_l = df_filter[df_filter["二级标签"].isin(logistics_tags)].copy()
        if not df_l.empty:
            top_l = df_l.groupby("main_sku").size().reset_index(name="标签数量")
            top_l = top_l.sort_values("标签数量", ascending=False).head(10)
            res_l = []
            for _, row in top_l.iterrows():
                sku = row["main_sku"]
                cnt = row["标签数量"]
                complaints = " | ".join(df_l[df_l["main_sku"] == sku]["客诉原文"].fillna("").astype(str).unique())
                summary = ai_generate_summary(sku, "物流未收到货", complaints)
                res_l.append({"商品名称": sku, "标签数量": cnt, "AI客诉总结": summary})
            st.dataframe(pd.DataFrame(res_l), use_container_width=True, hide_index=True)
        else:
            st.info("无物流问题数据")
    with col_o:
        st.markdown("### 🟡 其他问题 TOP10")
        other_tags = ["损坏", "少发/漏发", "外部因素"]
        df_o = df_filter[df_filter["二级标签"].isin(other_tags)].copy()
        if not df_o.empty:
            top_o = df_o.groupby("main_sku").size().reset_index(name="标签数量")
            top_o = top_o.sort_values("标签数量", ascending=False).head(10)
            res_o = []
            for _, row in top_o.iterrows():
                sku = row["main_sku"]
                cnt = row["标签数量"]
                complaints = " | ".join(df_o[df_o["main_sku"] == sku]["客诉原文"].fillna("").astype(str).unique())
                summary = ai_generate_summary(sku, "损坏/少发/外部问题", complaints)
                res_o.append({"商品名称": sku, "标签数量": cnt, "AI客诉总结": summary})
            st.dataframe(pd.DataFrame(res_o), use_container_width=True, hide_index=True)
        else:
            st.info("无其他问题数据")
    st.divider()

# ====================== 趋势统计模块 ======================
def get_image_as_base64(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

def show_daily_sentiment_module():
    st.subheader("📈 趋势统计模块")
    df = load_all_daily_sentiment_data()
    if df.empty:
        st.warning("未读取到任何每日舆情数据，请检查路径：E:\\Desktop\\每日打标")
        return
    required_cols = ["event_day", "二级标签", "三级标签", "交易号", "客诉原文", "main_sku"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.warning(f"缺少关键字段：{', '.join(missing)}，无法展示")
        return
    df = df.dropna(subset=["event_day", "二级标签"]).copy()
    if st.session_state.data_overview_start_date and st.session_state.data_overview_end_date:
        min_date = st.session_state.data_overview_start_date
        max_date = st.session_state.data_overview_end_date
    else:
        min_date = df["event_day"].min()
        max_date = df["event_day"].max()
    col1, col2 = st.columns(2)
    with col1:
        default_start = max(min_date, df["event_day"].min())
        default_end = min(max_date, df["event_day"].max())
        start_date, end_date = st.date_input("📅 时间范围（事件时间）", [default_start, default_end], min_value=df["event_day"].min(), max_value=df["event_day"].max())
    with col2:
        level2_list = ["全部"] + sorted(df["二级标签"].dropna().unique().tolist())
        selected_l2 = st.selectbox("🏷️ 二级标签筛选", level2_list)
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    df_filter = df[(df["event_day"] >= start_dt) & (df["event_day"] <= end_dt)].copy()
    if selected_l2 != "全部":
        df_filter = df_filter[df_filter["二级标签"] == selected_l2].copy()
    if df_filter.empty:
        st.info("当前筛选条件下无数据")
        return
    total_text = df_filter["客诉原文"].nunique()
    total_product = df_filter["main_sku"].nunique()
    c1, c2 = st.columns(2)
    with c1:
        st.metric("文本总数", total_text)
    with c2:
        st.metric("涉及商品数量", total_product)
    days_diff = (end_dt - start_dt).days
    if days_diff <= 15:
        df_filter["时间轴"] = df_filter["event_day"].dt.date
    else:
        df_filter["时间轴"] = df_filter["event_day"].dt.to_period("W").astype(str)
    trend_df = df_filter.groupby(["时间轴", "二级标签"]).size().reset_index(name="数量")
    total_by_time = trend_df.groupby("时间轴")["数量"].transform("sum")
    trend_df["比例"] = (trend_df["数量"] / total_by_time * 100).round(1).astype(str) + "%"
    tag_total = trend_df.groupby("二级标签")["数量"].sum().reset_index()
    tag_total = tag_total.sort_values("数量", ascending=False)
    sorted_tags = tag_total["二级标签"].tolist()
    trend_df["二级标签"] = pd.Categorical(trend_df["二级标签"], categories=sorted_tags, ordered=True)
    trend_df = trend_df.sort_values(["时间轴", "二级标签"])
    color_seq = px.colors.qualitative.Set3[:len(sorted_tags)]
    col_trend, col_stats = st.columns(2)
    with col_trend:
        st.markdown("### 📈 二级标签数量趋势")
        fig_trend = px.bar(trend_df, x="时间轴", y="数量", color="二级标签", text_auto=True, color_discrete_sequence=color_seq, category_orders={"二级标签": sorted_tags})
        fig_trend.update_traces(hovertemplate="<b>%{x}</b><br>%{fullData.name}<br>数量: %{y}<br>占比: %{customdata[0]}<extra></extra>", customdata=trend_df[["比例"]].values)
        fig_trend.update_traces(textposition="inside", texttemplate="%{customdata[0]}", customdata=trend_df[["比例"]].values)
        fig_trend.update_layout(height=550, xaxis_title="时间", yaxis_title="数量", legend_title="二级标签", barmode='stack', hovermode='x unified')
        st.plotly_chart(fig_trend, use_container_width=True)
    with col_stats:
        st.markdown("### 📊 标签统计")
        col_l2, col_l3 = st.columns(2)
        with col_l2:
            st.markdown("**二级标签统计**")
            df_filter["二级_交易"] = df_filter["二级标签"].astype(str) + "_" + df_filter["交易号"].astype(str)
            l2_stats = df_filter["二级_交易"].value_counts().reset_index()
            l2_stats.columns = ["二级_交易", "数量"]
            l2_stats["二级标签"] = l2_stats["二级_交易"].str.split("_").str[0]
            l2_stats = l2_stats.groupby("二级标签")["数量"].sum().reset_index()
            l2_stats = l2_stats.sort_values("数量", ascending=False)
            l2_stats["占比"] = (l2_stats["数量"] / l2_stats["数量"].sum() * 100).round(2).astype(str) + "%"
            st.dataframe(l2_stats, use_container_width=True, hide_index=True)
        with col_l3:
            st.markdown("**三级标签统计**")
            df_filter["三级_交易"] = df_filter["三级标签"].astype(str) + "_" + df_filter["交易号"].astype(str)
            l3_stats = df_filter["三级_交易"].value_counts().reset_index()
            l3_stats.columns = ["三级_交易", "数量"]
            l3_stats["三级标签"] = l3_stats["三级_交易"].str.split("_").str[0]
            l3_stats = l3_stats.groupby("三级标签")["数量"].sum().reset_index()
            l3_stats = l3_stats.sort_values("数量", ascending=False)
            st.dataframe(l3_stats, use_container_width=True, hide_index=True)
    st.divider()
    st.markdown("### ⚠️ 黑榜产品TOP10")
    available_l2 = df_filter["二级标签"].dropna().unique().tolist()
    if not available_l2:
        st.warning("无可用二级标签")
        return
    selected_top10_l2 = st.selectbox("选择二级标签查看TOP10", available_l2)
    df_top = df_filter[df_filter["二级标签"] == selected_top10_l2].copy()
    if df_top.empty:
        st.info("该标签下无数据")
        return
    top10 = df_top.groupby("main_sku").size().reset_index(name="投诉量")
    top10 = top10.sort_values("投诉量", ascending=False).head(10)
    result = []
    for _, row in top10.iterrows():
        sku = row["main_sku"]
        cnt = row["投诉量"]
        complaints = " | ".join(df_top[df_top["main_sku"] == sku]["客诉原文"].fillna("").tolist())
        summary = ai_generate_summary(sku, selected_top10_l2, complaints)
        result.append({"二级标签": selected_top10_l2, "商品SKU": sku, "投诉量": cnt, "标签原因总结（AI）": summary})
    df_result = pd.DataFrame(result)
    st.dataframe(df_result, use_container_width=True, hide_index=True)
    excel_data = convert_df_to_excel(df_result)
    st.download_button(label="📥 导出TOP10商品", data=excel_data, file_name=f"舆情TOP10商品_{selected_top10_l2}_{datetime.now().strftime('%Y%m%d')}.xlsx")
    st.divider()
    st.markdown("### 📁 导出完整每日舆情报表")
    excel_all = convert_df_to_excel(df)
    st.download_button(label="导出全部数据Excel", data=excel_all, file_name=f"每日舆情完整报表_{datetime.now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# =========================== 图表函数 ===========================
def create_risk_overview_chart(df_stores):
    risk_counts = df_stores['risk_level'].value_counts()
    risk_labels = {'high': '高危', 'medium': '关注', 'safe': '安全'}
    fig = go.Figure(data=[go.Pie(labels=[risk_labels.get(x, x) for x in risk_counts.index], values=risk_counts.values, marker=dict(colors=[COLORS['high'], COLORS['medium'], COLORS['safe']]), textinfo='label+percent', textposition='outside', hole=0.4)])
    fig.update_layout(title="🚨 店铺风险等级分布", height=450, margin=dict(l=20, r=20, t=60, b=20), showlegend=True)
    return fig

def create_paypal_complaint_chart(paypal_data, selected_year_month):
    if paypal_data is None or paypal_data.empty: return None
    df_filtered = paypal_data[paypal_data['年月'] == selected_year_month].copy()
    if df_filtered.empty: return None
    df_sku = df_filtered[['产品SKU', '与描述不符投诉数', '未收到货投诉数', '该月出单数']].copy()
    df_sku = df_sku.sort_values('与描述不符投诉数', ascending=False).head(10)
    fig = go.Figure()
    fig.add_trace(go.Bar(name='与描述不符投诉', x=df_sku['产品SKU'], y=df_sku['与描述不符投诉数'], marker_color=COLORS['medium'], text=df_sku['与描述不符投诉数'], textposition='outside'))
    fig.add_trace(go.Bar(name='未收到货投诉', x=df_sku['产品SKU'], y=df_sku['未收到货投诉数'], marker_color=COLORS['high'], text=df_sku['未收到货投诉数'], textposition='outside'))
    fig.update_layout(title=f'💳 {selected_year_month} PayPal投诉TOP10 SKU分析', barmode='group', height=500, xaxis_title="产品SKU", yaxis_title="投诉数量", xaxis_tickangle=-45, margin=dict(l=20, r=20, t=60, b=100))
    return fig

def create_risk_trend_chart(store_data):
    periods = []
    ratings = []
    ot_counts = []
    for p_num in sorted(store_data['period_data'].keys()):
        p_data = store_data['period_data'][p_num]
        periods.append(f"{p_data['period']} ({p_data['time_range']})")
        ratings.append(p_data['avg_rating'])
        ot_counts.append(p_data['ot'])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=periods, y=ratings, name='平均星级', mode='lines+markers', line=dict(color=COLORS['safe'], width=3), marker=dict(size=10), yaxis='y1', text=[f'{r:.1f}⭐' for r in ratings], textposition='top center'))
    fig.add_trace(go.Scatter(x=periods, y=ot_counts, name='差评数', mode='lines+markers', line=dict(color=COLORS['high'], width=3), marker=dict(size=10), yaxis='y2', text=[str(count) for count in ot_counts], textposition='top center'))
    fig.update_layout(title="📈 店铺28天风险趋势分析", yaxis=dict(title='平均星级', side='left', range=[0, 5], gridcolor='lightgray'), yaxis2=dict(title='差评数', side='right', overlaying='y', showgrid=False), height=500, margin=dict(l=20, r=20, t=60, b=80), hovermode='x unified')
    return fig

def create_complaint_rate_chart(paypal_data):
    if paypal_data is None or paypal_data.empty: return None
    df_trend = paypal_data.groupby('年月').agg({'与描述不符投诉数': 'sum', '未收到货投诉数': 'sum', '总投诉数': 'sum', '该月出单数': 'sum'}).reset_index()
    df_trend['与描述不符投诉率'] = (df_trend['与描述不符投诉数'] / df_trend['该月出单数'] * 100).round(2)
    df_trend['未收到货投诉率'] = (df_trend['未收到货投诉数'] / df_trend['该月出单数'] * 100).round(2)
    df_trend['总投诉率'] = (df_trend['总投诉数'] / df_trend['该月出单数'] * 100).round(2)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_trend['年月'], y=df_trend['总投诉率'], name='总投诉率', mode='lines+markers', line=dict(color=COLORS['high'], width=3), marker=dict(size=8), text=[f'{rate}%' for rate in df_trend['总投诉率']], textposition='top center'))
    fig.add_trace(go.Scatter(x=df_trend['年月'], y=df_trend['与描述不符投诉率'], name='与描述不符投诉率', mode='lines+markers', line=dict(color=COLORS['medium'], width=3, dash='dot'), marker=dict(size=6)))
    fig.add_trace(go.Scatter(x=df_trend['年月'], y=df_trend['未收到货投诉率'], name='未收到货投诉率', mode='lines+markers', line=dict(color=COLORS['primary'], width=3, dash='dash'), marker=dict(size=6)))
    fig.update_layout(title="📊 PayPal投诉率月度趋势", xaxis_title="统计年月", yaxis_title="投诉率(%)", height=450, margin=dict(l=20, r=20, t=60, b=80), hovermode='x unified')
    return fig

def create_internal_risk_distribution_chart(internal_data):
    if internal_data is None or internal_data.empty: return None
    team_stats = internal_data.groupby('opera_team').size().reset_index(name='风险数量')
    team_stats = team_stats.sort_values('风险数量', ascending=False).head(10)
    fig = go.Figure(data=[go.Bar(x=team_stats['opera_team'], y=team_stats['风险数量'], marker=dict(color=team_stats['风险数量'], colorscale='Reds', showscale=True, colorbar=dict(title="风险数量")), text=team_stats['风险数量'], textposition='outside')])
    fig.update_layout(title="⚠️ 内部风险看板 - 运营团队风险分布", xaxis_title="运营团队", yaxis_title="风险记录数", height=450, xaxis_tickangle=-45, margin=dict(l=20, r=20, t=60, b=100))
    return fig

@st.cache_data(ttl=3600)
def load_shipping_data():
    try:
        df = pd.read_excel(SHIPPING_DATA_PATH)
        df.columns = df.columns.str.strip()
        if '落地页' in df.columns:
            df['url_prefix'] = df['落地页'].astype(str).apply(lambda x: re.sub(r'^https?://(www\.)?', '', x).split('/')[0] if pd.notna(x) and x != 'nan' else '')
        elif '店铺' in df.columns:
            df['url_prefix'] = df['店铺'].astype(str).str.strip()
        elif '域名' in df.columns:
            df['url_prefix'] = df['域名'].astype(str).str.strip()
        else:
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['url', 'site', 'domain', 'store', 'shop']):
                    df['url_prefix'] = df[col].astype(str).str.strip()
                    break
        if 'url_prefix' not in df.columns:
            df['url_prefix'] = 'unknown'
        return df
    except Exception as e:
        st.warning(f"加载发货数据失败：{str(e)}")
        return pd.DataFrame()

def fig_to_base64(fig):
    img_bytes = fig.to_image(format="png", width=1400, height=700, scale=2)
    encoded = base64.b64encode(img_bytes).decode()
    return f"data:image/png;base64,{encoded}"

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>%(report_title)s</title>
    <style>
        body { font-family: 'Microsoft YaHei', 'PingFang SC', sans-serif; background: #f5f5f5; padding: 30px 20px; line-height: 1.6; }
        .container { max-width: 1400px; margin: 0 auto; background: white; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }
        .header { background: linear-gradient(135deg, #D32F2F 0%, #B71C1C 100%); color: white; padding: 40px; text-align: center; border-radius: 12px 12px 0 0; }
        .header h1 { font-size: 2.8em; margin-bottom: 15px; font-weight: 700; }
        .header .risk-badge { display: inline-block; background: white; color: #D32F2F; padding: 8px 20px; border-radius: 20px; font-weight: bold; margin-top: 15px; }
        .content { padding: 40px; }
        .section { margin-bottom: 50px; }
        .section-title { font-size: 2em; color: #D32F2F; margin-bottom: 25px; padding-bottom: 15px; border-bottom: 4px solid #D32F2F; font-weight: 700; display: flex; align-items: center; }
        .section-title .icon { margin-right: 15px; font-size: 1.2em; }
        .risk-alert { background: linear-gradient(135deg, #FFEBEE 0%, #FFCDD2 100%); border-left: 6px solid #D32F2F; padding: 25px; margin: 25px 0; border-radius: 8px; }
        .risk-alert h3 { color: #B71C1C; margin-top: 0; font-size: 1.5em; }
        .risk-alert .risk-level { font-size: 1.8em; font-weight: bold; color: #D32F2F; margin: 15px 0; }
        .ai-analysis { background: #FFF3E0; border-left: 5px solid #FF9800; padding: 25px; margin: 25px 0; border-radius: 8px; }
        .ai-analysis h4 { color: #F57C00; margin-bottom: 15px; font-size: 1.3em; }
        .ai-analysis p { color: #424242; line-height: 1.9; margin-bottom: 12px; font-size: 1.05em; }
        .metric-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 25px; margin: 30px 0; }
        .metric-card { padding: 25px; border-radius: 10px; text-align: center; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        .metric-card.risk-high { background: linear-gradient(135deg, #FF5252 0%, #D32F2F 100%); color: white; }
        .metric-card.risk-medium { background: linear-gradient(135deg, #FFA726 0%, #F57C00 100%); color: white; }
        .metric-card.risk-safe { background: linear-gradient(135deg, #66BB6A 0%, #43A047 100%); color: white; }
        .metric-card.neutral { background: linear-gradient(135deg, #42A5F5 0%, #1E88E5 100%); color: white; }
        .metric-card .value { font-size: 3em; font-weight: 700; margin-bottom: 10px; }
        .metric-card .label { font-size: 1.1em; opacity: 0.95; }
        .chart-container { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin: 25px 0; border: 1px solid #e0e0e0; }
        .chart-container img { width: 100%; height: auto; border-radius: 8px; }
        .chart-title { font-size: 1.4em; color: #424242; margin-bottom: 20px; font-weight: 600; text-align: center; }
        .data-table { width: 100%; border-collapse: collapse; margin: 25px 0; background: white; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; }
        .data-table th { background: linear-gradient(135deg, #424242 0%, #212121 100%); color: white; padding: 18px; text-align: left; font-weight: 600; font-size: 1.05em; }
        .data-table td { padding: 15px 18px; border-bottom: 1px solid #eee; }
        .data-table tr:hover { background: #FFF8E1; }
        .data-table tr:nth-child(even) { background: #FAFAFA; }
        .risk-high-light { color: #D32F2F; font-weight: bold; }
        .risk-medium-light { color: #F57C00; font-weight: bold; }
        .paypal-section { background: linear-gradient(135deg, #E3F2FD 0%, #BBDEFB 100%); padding: 25px; border-radius: 10px; margin: 25px 0; border-left: 5px solid #1976D2; }
        .paypal-section h3 { color: #1565C0; margin-top: 0; }
        .internal-feedback-section { background: linear-gradient(135deg, #E8F5E9 0%, #C8E6C9 100%); padding: 25px; border-radius: 10px; margin: 25px 0; border-left: 5px solid #388E3C; }
        .internal-feedback-section h3 { color: #2E7D32; margin-top: 0; }
        .shipping-section { background: linear-gradient(135deg, #FFF3E0 0%, #FFE0B2 100%); padding: 25px; border-radius: 10px; margin: 25px 0; border-left: 5px solid #FF9800; }
        .shipping-section h3 { color: #E65100; margin-top: 0; }
        .risk-summary-box { background: linear-gradient(135deg, #FFF8E1 0%, #FFECB3 100%); padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 5px solid #FFC107; }
        .risk-summary-box h4 { color: #FF6F00; margin-top: 0; }
        .footer { background: #424242; color: white; padding: 40px; text-align: center; font-size: 0.9em; }
        .footer p { margin: 10px 0; }
        @media print {
            body { background: white; padding: 20px; }
            .container { box-shadow: none; }
            .header { background: #D32F2F !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚨 %(report_title)s</h1>
            <div class="risk-badge">风险等级：<span id="risk_level">%(risk_level)s</span></div>
            <div style="margin-top: 20px; font-size: 1.1em; opacity: 0.9;">
                <p>📊 生成时间：%(generation_time)s</p>
                <p>🎯 分析周期：%(analysis_period)s</p>
                <p>📈 数据来源：Trustpilot + PayPal + 发货表现 + 内部反馈系统</p>
            </div>
        </div>
        <div class="content">
            %(content)s
        </div>
    </div>
</body>
</html>"""

def generate_ai_analysis(data_type: str, data: Any, title: str) -> str:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if data_type == "internal":
        total_records = len(data)
        product_count = data['商品'].nunique()
        team_count = data['opera_team'].nunique()
        sample_data = data.to_string(index=False, max_rows=30)
        prompt = f"""你是电商风险管控专家,请基于以下内部舆情数据生成专业的风险分析报告。
报告生成时间：{current_time}
报告核心目标：识别和暴露潜在风险
数据统计：
- 总记录数：{total_records}条
- 涉及商品数：{product_count}个
- 涉及运营团队数：{team_count}个
详细数据（前30条记录）：
{sample_data}
请生成一份专业的风险分析报告,重点突出：
1. ⚠️ 核心风险识别：明确列出主要风险点和问题类型
2. 📊 风险量化分析：用数据支撑风险等级判定
3. 🎯 高风险商品/团队：指出需要重点关注的商品和运营团队
4. 📈 风险趋势研判：分析风险的发展趋势和潜在恶化可能
报告要求：
- 以风险暴露为核心,突出严重性和紧急性
- 用具体数据支撑每一个观点
- 报告格式和结构由你自由决定,但必须突出风险
- 请勿提供任何建议、措施或应对方案"""
    elif data_type == "external":
        store_name, store_data, bad_reviews, all_stores_data, paypal_data, internal_feedback_data, shipping_data = data
        risk_level = {'high': '🔴 高危', 'medium': '🟡 关注', 'safe': '🟢 安全'}[store_data['risk_level']]
        risk_level_text = {'high': '高危', 'medium': '关注', 'safe': '安全'}[store_data['risk_level']]
        store_info = f"""
【店铺风险档案】
- 店铺名称：{store_name}
- 风险等级：{risk_level}
- 风险原因：{store_data['risk_reason']}
- 近7天差评数：{store_data['ot_7d']}条
- 近28天差评数：{store_data['ot_28d']}条
- 平均星级：{store_data['avg_rating_28d']}/5.0
- 1星占比：{store_data['one_star_ratio_28d']}%
- 差评环比增长：{store_data['ot_growth'] * 100:.1f}%
"""
        bad_reviews_text = ""
        if bad_reviews and len(bad_reviews) > 0:
            bad_reviews_text = "\n".join([f"{i + 1}. ⭐{str(review)[:350]}" for i, review in enumerate(bad_reviews[:20])])
        paypal_text = ""
        if paypal_data is not None and not paypal_data.empty:
            paypal_summary = f"""
【PayPal投诉风险分析】
- 统计年月：{', '.join(paypal_data['年月'].unique())}
- 总订单数：{paypal_data['该月出单数'].sum()}
- 总投诉数：{paypal_data['总投诉数'].sum()}
- 与描述不符投诉数：{paypal_data['与描述不符投诉数'].sum()}
- 未收到货投诉数：{paypal_data['未收到货投诉数'].sum()}
- 整体投诉率：{(paypal_data['总投诉数'].sum() / paypal_data['该月出单数'].sum() * 100) if paypal_data['该月出单数'].sum() > 0 else 0:.2f}%
"""
            paypal_detail = paypal_data.to_string(index=False, max_rows=25)
            paypal_text = paypal_summary + "\n\n【PayPal详细投诉数据】（前25条）\n" + paypal_detail
        internal_text = ""
        if internal_feedback_data is not None and not internal_feedback_data.empty:
            internal_summary = f"""
【内部反馈风险分析】
- 内部反馈记录数：{len(internal_feedback_data)}条
- 涉及商品数：{internal_feedback_data['商品'].nunique()}个
- 涉及运营团队数：{internal_feedback_data['opera_team'].nunique()}个
"""
            internal_detail = internal_feedback_data.to_string(index=False, max_rows=20)
            internal_text = internal_summary + "\n\n【内部反馈详细数据】（前20条）\n" + internal_detail
        shipping_text = ""
        if shipping_data is not None and not shipping_data.empty:
            shipping_summary = f"""
【发货表现风险分析】
- 发货数据记录数：{len(shipping_data)}条
- 涉及SKU/店铺数：{shipping_data.get('产品SKU', shipping_data.get('店铺', shipping_data.get('url_prefix', ['unknown']))).nunique() if '产品SKU' in shipping_data.columns or '店铺' in shipping_data.columns or 'url_prefix' in shipping_data.columns else len(shipping_data)}个
"""
            rate_columns = []
            for col in shipping_data.columns:
                if any(keyword in col.lower() for keyword in ['率', 'rate', 'ratio', 'percent', '%']):
                    rate_columns.append(col)
            if rate_columns:
                shipping_summary += "- 发货指标：" + "、".join(rate_columns)
            shipping_detail = shipping_data.to_string(index=False, max_rows=20)
            shipping_text = shipping_summary + "\n\n【发货表现详细数据】（前20条）\n" + shipping_detail
        prompt = f"""你是电商风险管控专家,请基于以下完整数据生成专业的店铺风险分析报告。
报告生成时间：{current_time}
报告核心目标：全方位暴露店铺风险,为决策提供数据支撑
{store_info}
【Trustpilot差评风险】（近28天1-2星评论,前20条）
{bad_reviews_text if bad_reviews_text else '暂无差评数据'}
{paypal_text if paypal_text else '暂无PayPal投诉数据'}
{shipping_text if shipping_text else '暂无发货表现数据'}
{internal_text if internal_text else '暂无内部反馈数据'}
请生成一份专业的风险分析报告,重点突出：
第一部分：🚨 风险总体评估
- 综合风险等级判定及依据
- 主要风险领域识别（Trustpilot、PayPal、发货表现、内部反馈）
- 风险严重程度量化评估
第二部分：⚠️ 核心风险点深度分析
- Trustpilot差评：主要风险类型、高频问题、典型案例
- PayPal投诉：投诉类型分析、投诉率对比、高风险SKU
- 发货表现：发货指标异常、发货延迟风险、物流问题
- 内部反馈：内部发现的问题、重复出现的问题
第三部分：📊 风险量化与趋势
- 各类风险指标的量化对比
- 风险变化趋势分析
- 与行业基准的对比（如有）
报告要求：
- 以风险暴露为核心,突出严重性和紧急性
- 每个观点都要有数据支撑
- 请勿提供任何建议、措施或应对方案
- 报告格式和结构由你自由决定,但必须突出风险"""
    else:
        return "不支持的数据类型"
    payload = {"model": DEEPSEEK_CONFIG["model"], "messages": [{"role": "user", "content": prompt}], "temperature": DEEPSEEK_CONFIG["temperature"], "max_tokens": DEEPSEEK_CONFIG["max_tokens"], "stream": False}
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {DEEPSEEK_CONFIG['api_key']}"}
    try:
        response = requests.post(DEEPSEEK_CONFIG["api_url"], headers=headers, json=payload, timeout=DEEPSEEK_CONFIG["timeout"])
        response.raise_for_status()
        result = response.json()
        if "choices" in result and len(result["choices"]) > 0:
            return result["choices"][0]["message"]["content"].strip()
        else:
            return "AI分析生成失败：API返回格式异常"
    except Exception as e:
        return f"AI分析生成失败：{str(e)}"

# ====================== 发货表现分析模块 ======================
def show_shipping_analysis_module():
    st.subheader("🚚 发货表现分析（投诉联动）", divider="green")
    shipping_df = load_shipping_data()
    if shipping_df.empty:
        st.warning("未加载到发货数据，请检查路径：{}".format(SHIPPING_DATA_PATH))
        return
    selected_complaint = st.session_state.selected_complaint_data
    if selected_complaint is None:
        st.info("请先在「详细投诉数据」模块选择一条投诉记录，自动联动展示关联发货表现")
        return
    linked_shipping_df = get_shipping_data_by_selected_complaint(selected_complaint, shipping_df)
    if linked_shipping_df.empty:
        st.warning(f"未匹配到关联发货数据\n匹配条件：SKU={selected_complaint.get('main_sku', '未知')} | 店铺/域名={selected_complaint.get('店铺', selected_complaint.get('域名', '未知'))}")
        return
    st.markdown("### 📌 联动匹配结果")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("匹配到发货记录数", len(linked_shipping_df))
    with col2:
        st.metric("关联SKU数", linked_shipping_df['main_sku'].nunique() if 'main_sku' in linked_shipping_df.columns else 0)
    with col3:
        st.metric("关联店铺数", linked_shipping_df['url_prefix'].nunique())
    st.markdown("### 📊 发货核心指标分析")
    shipping_metrics = {}
    if '发货时效' in linked_shipping_df.columns:
        shipping_metrics['平均发货时效'] = linked_shipping_df['发货时效'].mean().round(2)
        shipping_metrics['最晚发货时效'] = linked_shipping_df['发货时效'].max()
    if '物流妥投率' in linked_shipping_df.columns:
        shipping_metrics['平均妥投率'] = f"{linked_shipping_df['物流妥投率'].mean().round(2)}%"
    if '异常订单数' in linked_shipping_df.columns:
        shipping_metrics['异常订单数'] = linked_shipping_df['异常订单数'].sum()
    if '延迟发货数' in linked_shipping_df.columns:
        shipping_metrics['延迟发货数'] = linked_shipping_df['延迟发货数'].sum()
        shipping_metrics['延迟发货率'] = f"{(linked_shipping_df['延迟发货数'].sum() / len(linked_shipping_df) * 100).round(2)}%"
    metric_cols = st.columns(len(shipping_metrics))
    for idx, (metric_name, metric_value) in enumerate(shipping_metrics.items()):
        with metric_cols[idx]:
            st.metric(metric_name, metric_value)
    st.markdown("### 📋 关联发货数据详情")
    key_columns = ['url_prefix', 'main_sku', '发货时效', '物流妥投率', '异常订单数', '延迟发货数', '发货日期']
    display_columns = [col for col in key_columns if col in linked_shipping_df.columns]
    if not display_columns:
        display_columns = linked_shipping_df.columns.tolist()[:10]
    st.dataframe(linked_shipping_df[display_columns], use_container_width=True, hide_index=True)
    st.markdown("### 📈 发货异常趋势分析")
    if '发货日期' in linked_shipping_df.columns and '延迟发货数' in linked_shipping_df.columns:
        linked_shipping_df['发货日期'] = pd.to_datetime(linked_shipping_df['发货日期'], errors='coerce')
        trend_df = linked_shipping_df.groupby(pd.Grouper(key='发货日期', freq='D')).agg({'延迟发货数': 'sum', '异常订单数': 'sum' if '异常订单数' in linked_shipping_df.columns else 'count'}).reset_index()
        fig = px.bar(trend_df, x='发货日期', y=['延迟发货数', '异常订单数'] if '异常订单数' in linked_shipping_df.columns else ['延迟发货数'], barmode='group', title="每日发货异常统计", color_discrete_map={"延迟发货数": COLORS['medium'], "异常订单数": COLORS['high']})
        fig.update_layout(height=400, xaxis_title="日期", yaxis_title="数量")
        st.plotly_chart(fig, use_container_width=True)
    excel_data = convert_df_to_excel(linked_shipping_df)
    st.download_button(label="📥 导出联动发货数据", data=excel_data, file_name=f"投诉联动发货数据_{selected_complaint.get('main_sku', '未知')}_{datetime.now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def generate_html_report(analysis_content: str, title: str, data_stats: dict, report_type: str, charts_base64: Dict[str, str] = None, internal_feedback_data: pd.DataFrame = None, paypal_data: pd.DataFrame = None, store_data: dict = None, shipping_data: pd.DataFrame = None) -> str:
    summary = analysis_content[:400] + "..." if len(analysis_content) > 400 else analysis_content
    risk_level = "安全"
    if report_type == "external":
        if "高危" in title or "HIGH" in title.upper():
            risk_level = "🔴 高危"
        elif "关注" in title or "MEDIUM" in title.upper():
            risk_level = "🟡 关注"
        else:
            risk_level = "🟢 安全"
    stats_html = ""
    for stat in data_stats:
        risk_class = "risk-high" if stat.get('risk', '').lower() == 'high' else "risk-medium" if stat.get('risk', '').lower() == 'medium' else "risk-safe" if stat.get('risk', '').lower() == 'safe' else "neutral"
        stats_html += f"""
<div class="metric-card {risk_class}">
<div class="value">{stat['value']}</div>
<div class="label">{stat['label']}</div>
</div>
"""
    analysis_html = analysis_content.replace('\n\n', '</p><p>').replace('\n', '<br>')
    analysis_html = f'<p>{analysis_html}</p>'
    analysis_html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', analysis_html)
    analysis_html = re.sub(r'### (.*)', r'<h4>\1</h4>', analysis_html)
    analysis_html = re.sub(r'## (.*)', r'<h3>\1</h3>', analysis_html)
    analysis_html = re.sub(r'# (.*)', r'<h2>\1</h3>', analysis_html)
    lines = analysis_html.split('<p>')
    filtered_lines = []
    for line in lines:
        if '建议' not in line and '应对' not in line and '措施' not in line:
            filtered_lines.append(line)
    analysis_html = '<p>'.join(filtered_lines)
    analysis_html = re.sub(r'(高危|HIGH)', r'<span class="risk-high-light">\1</span>', analysis_html)
    analysis_html = re.sub(r'(关注|MEDIUM)', r'<span class="risk-medium-light">\1</span>', analysis_html)
    charts_html = ""
    if charts_base64:
        for chart_name, chart_base64 in charts_base64.items():
            charts_html += f"""
<div class="chart-container">
<div class="chart-title">{chart_name}</div>
<img src="{chart_base64}" alt="{chart_name}" />
</div>
"""
    paypal_analysis_html = ""
    if paypal_data is not None and not paypal_data.empty:
        selected_year_month = paypal_data['年月'].max()
        df_filtered = paypal_data[paypal_data['年月'] == selected_year_month]
        total_complaints = df_filtered['总投诉数'].sum()
        total_orders = df_filtered['该月出单数'].sum()
        desc_complaints = df_filtered['与描述不符投诉数'].sum()
        delivery_complaints = df_filtered['未收到货投诉数'].sum()
        complaint_rate = (total_complaints / total_orders * 100) if total_orders > 0 else 0
        top_skus = df_filtered.sort_values('总投诉数', ascending=False).head(5)[['产品SKU', '总投诉数', '与描述不符投诉数', '未收到货投诉数']]
        paypal_analysis_html = f"""
<div class="section">
<div class="section-title">
<span class="icon">💳</span>
PayPal投诉情况分析
</div>
<div class="paypal-section">
<h3>📊 投诉风险量化数据</h3>
<p><strong>统计年月：</strong>{selected_year_month}</p>
<p><strong>总订单数：</strong>{total_orders} 单</p>
<p><strong>总投诉数：</strong>{total_complaints} 件</p>
<p><strong>与描述不符投诉：</strong>{desc_complaints} 件（占比 {desc_complaints / total_complaints * 100:.1f}%）</p>
<p><strong>未收到货投诉：</strong>{delivery_complaints} 件（占比 {delivery_complaints / total_complaints * 100:.1f}%）</p>
<p><strong>整体投诉率：</strong>{complaint_rate:.2f}%</p>
<div class="risk-summary-box">
<h4>⚠️ 核心风险点识别</h4>
<p>投诉率{'高于' if complaint_rate > 3 else '低于'}行业平均水平（3%）</p>
<p>主要投诉类型：{'与描述不符' if desc_complaints > delivery_complaints else '未收到货'}问题占比最高</p>
</div>
<h4 style="margin-top: 20px;">🔥 投诉贡献TOP5 SKU</h4>
{generate_data_table_html(top_skus, "投诉集中度分析")}
</div>
</div>
"""
    shipping_analysis_html = ""
    if shipping_data is not None and not shipping_data.empty:
        rate_columns = []
        for col in shipping_data.columns:
            if any(keyword in col.lower() for keyword in ['率', 'rate', 'ratio', 'percent', '%']):
                rate_columns.append(col)
        shipping_stats = {}
        for col in rate_columns:
            if col in shipping_data.columns:
                avg_val = shipping_data[col].mean()
                if pd.notna(avg_val):
                    shipping_stats[col] = f"{avg_val:.2f}%"
        if '产品SKU' in shipping_data.columns:
            top_shipping = shipping_data.sort_values(rate_columns[0] if rate_columns else shipping_data.columns[0], ascending=False).head(5)
            shipping_analysis_html += f"""
<div class="shipping-section">
<h3>📦 发货表现TOP5 SKU</h3>
{generate_data_table_html(top_shipping, "发货表现分析")}
</div>
"""
        shipping_stats_html = ""
        if shipping_stats:
            shipping_stats_html = "<p><strong>整体发货指标：</strong>" + " | ".join([f"{k}: {v}" for k, v in shipping_stats.items()]) + "</p>"
        shipping_analysis_html = f"""
<div class="section">
<div class="section-title">
<span class="icon">📦</span>
发货表现分析
</div>
<div class="shipping-section">
<h3>📊 发货风险量化数据</h3>
<p><strong>数据记录数：</strong>{len(shipping_data)} 条</p>
{shipping_stats_html}
<div class="risk-summary-box">
<h4>⚠️ 发货风险点识别</h4>
<p>发货数据反映店铺的履约能力和物流管理效率</p>
</div>
</div>
{shipping_analysis_html if '产品SKU' in shipping_data.columns else ''}
</div>
"""
    internal_feedback_html = ""
    if internal_feedback_data is not None and not internal_feedback_data.empty:
        team_risk_stats = internal_feedback_data.groupby('opera_team').size().reset_index(name='风险记录数')
        team_risk_stats = team_risk_stats.sort_values('风险记录数', ascending=False)
        display_columns = ['年份', '月份', '商品', 'opera_team', '投诉原因总结']
        internal_feedback_html = f"""
<div class="section">
<div class="section-title">
<span class="icon">📄</span>
内部反馈风险数据
</div>
<div class="internal-feedback-section">
<h3>商品在内部风险看板体现</h3>
<p>该商品在内部反馈系统中共有 <strong>{len(internal_feedback_data)}</strong> 条记录,涉及 <strong>{internal_feedback_data['商品'].nunique()}</strong> 个商品,<strong>{internal_feedback_data['opera_team'].nunique()}</strong> 个运营团队。</p>
<div class="risk-summary-box">
<h4>⚠️ 高风险运营团队</h4>
<p>风险记录最多的团队：<strong>{team_risk_stats.iloc[0]['opera_team']}</strong>（{team_risk_stats.iloc[0]['风险记录数']}条）</p>
</div>
</div>
{generate_data_table_html(internal_feedback_data.head(20), "内部反馈详细数据（前20条）", display_columns)}
</div>
"""
    html_content = HTML_TEMPLATE % {
        'report_title': title,
        'risk_level': risk_level,
        'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'analysis_period': '近28天数据' if report_type == 'external' else '当前数据',
        'content': f"""
<div class="section">
<div class="section-title">
<span class="icon">🚨</span>
风险概览
</div>
<div class="risk-alert">
<h3>⚠️ 风险评估摘要</h3>
<div class="risk-level">{risk_level}</div>
<p>{summary}</p>
</div>
<h2 style="font-size: 1.5em; color: #424242; margin: 30px 0 20px 0;">📊 关键风险指标</h2>
<div class="metric-cards">
{stats_html}
</div>
</div>
<div class="section">
<div class="section-title">
<span class="icon">🤖</span>
AI智能风险分析
</div>
<div class="ai-analysis">
<h4>💡 深度风险洞察</h4>
{analysis_html}
</div>
</div>
{charts_html if charts_html else ''}
{paypal_analysis_html if paypal_analysis_html else ''}
{shipping_analysis_html if shipping_analysis_html else ''}
{internal_feedback_html}
"""
    }
    return html_content

def generate_data_table_html(df: pd.DataFrame, title: str = "数据详情", columns: list = None) -> str:
    table_html = f"<h3 style='color: #424242; margin-top: 0;'>{title}</h3>"
    table_html += "<table class='data-table'><thead><tr>"
    if columns:
        display_cols = [col for col in columns if col in df.columns]
    else:
        display_cols = df.columns.tolist()
    for col in display_cols:
        table_html += f"<th>{col}</th>"
    table_html += "</tr></thead><tbody>"
    for _, row in df.iterrows():
        table_html += "<tr>"
        for col in display_cols:
            table_html += f"<td>{str(row[col]) if pd.notna(row[col]) else ''}</td>"
        table_html += "</tr>"
    table_html += "</tbody></table>"
    return table_html

def add_report_export_ui(report_type: str, data: Any, title: str, data_stats: list, charts: Dict[str, go.Figure] = None, internal_feedback_data: pd.DataFrame = None, paypal_data: pd.DataFrame = None, store_data: dict = None, shipping_data: pd.DataFrame = None):
    html_content = ""
    analysis_content = ""
    report_bytes = None
    mime_type = ""
    file_ext = ""
    col1, col2 = st.columns([3, 1])
    with col1:
        report_format = st.selectbox("选择报告格式", ["HTML", "Markdown"], key=f"report_format_{report_type}")
    with col2:
        generate_clicked = st.button("🚨 生成风险分析报告", use_container_width=True, key=f"generate_report_{report_type}")
    if generate_clicked:
        with st.spinner("AI正在全面分析风险并生成报告（可能需要30-60秒）..."):
            analysis_content = generate_ai_analysis(report_type, data, title)
        if report_format == "HTML":
            charts_base64 = {}
            if charts:
                for chart_name, fig in charts.items():
                    try:
                        charts_base64[chart_name] = fig_to_base64(fig)
                    except Exception as e:
                        st.warning(f"图表转换失败：{chart_name} - {str(e)}")
            html_content = generate_html_report(analysis_content, title, data_stats, report_type, charts_base64, internal_feedback_data, paypal_data, store_data, shipping_data)
            report_bytes = html_content.encode('utf-8')
            mime_type = "text/html"
            file_ext = "html"
        else:
            report_bytes = analysis_content.encode('utf-8')
            mime_type = "text/markdown"
            file_ext = "md"
        st.download_button(label=f"📥 下载{report_format}风险报告", data=report_bytes, file_name=f"风险分析报告_{title.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_ext}", mime=mime_type, use_container_width=True)
    with st.expander("📋 查看风险报告预览", expanded=True):
        if not generate_clicked:
            st.info("请点击上方【生成风险分析报告】按钮后查看预览")
        else:
            if report_format == "HTML" and html_content:
                st.html(html_content)
            elif report_format == "Markdown" and analysis_content:
                st.markdown(analysis_content)
            else:
                st.warning("报告生成失败，请重试")

@st.cache_data(ttl=3600)
def load_internal_feedback_data():
    try:
        df = pd.read_excel(INTERNAL_FEEDBACK_PATH)
        if '年份' not in df.columns:
            df['年份'] = df.get('year', pd.NA)
        if '月份' not in df.columns:
            df['月份'] = df.get('month', pd.NA)
        if 'opera_team' not in df.columns:
            df['opera_team'] = df.get('运营团队', pd.NA)
        if '商品' not in df.columns:
            df['商品'] = df.get('product', pd.NA)
        if '文本内容' not in df.columns:
            df['文本内容'] = df.get('content', pd.NA)
        df['年份'] = pd.to_numeric(df['年份'], errors='coerce')
        df['月份'] = pd.to_numeric(df['月份'], errors='coerce')
        df['商品_清理'] = df['商品'].astype(str).str.strip()
        df['opera_team_清理'] = df['opera_team'].astype(str).str.strip()
        return df
    except Exception as e:
        st.error(f"加载内部舆情数据失败：{str(e)}")
        return pd.DataFrame()

def get_store_internal_feedback(store_name: str, product_skus: List[str] = None) -> pd.DataFrame:
    try:
        df_internal = load_internal_feedback_data()
        if df_internal.empty:
            return pd.DataFrame()
        df_filtered = df_internal.copy()
        if product_skus:
            df_filtered = df_filtered[df_filtered['商品_清理'].isin([str(sku).strip() for sku in product_skus])]
        return df_filtered
    except Exception as e:
        st.warning(f"获取内部反馈数据失败：{str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_paypal_30d_data():
    try:
        df = pd.read_excel(PAYPAL_30D_PATH)
        df.columns = df.columns.str.strip()
        if '落地页' in df.columns:
            df['url_prefix'] = df['落地页'].astype(str).apply(lambda x: re.sub(r'^https?://(www\.)?', '', x).split('/')[0] if pd.notna(x) else '')
        return df
    except Exception as e:
        st.warning(f"加载店铺近30天表现数据失败：{str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_paypal_risk_data():
    try:
        df = pd.read_excel(PAYPAL_RISK_PATH)
        df.columns = df.columns.str.strip()
        if '落地页' in df.columns:
            df['url_prefix'] = df['落地页'].astype(str).apply(lambda x: re.sub(r'^https?://(www\.)?', '', x).split('/')[0] if pd.notna(x) else '')
        df = _process_paypal_date_fields(df)
        df = _calculate_complaint_contribution(df)
        return df
    except Exception as e:
        st.warning(f"加载风险店铺投诉数据失败：{str(e)}")
        return pd.DataFrame()

def _process_paypal_date_fields(df):
    if '统计年月' in df.columns:
        df['年月'] = pd.to_datetime(df['统计年月'], errors='coerce').dt.strftime('%Y-%m')
    elif '投诉时间' in df.columns:
        df['年月'] = pd.to_datetime(df['投诉时间'], errors='coerce').dt.strftime('%Y-%m')
    elif '日期' in df.columns:
        df['年月'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m')
    if '年月' not in df.columns and '年份' in df.columns and '月份' in df.columns:
        df['年份'] = pd.to_numeric(df['年份'], errors='coerce').fillna(0).astype(int)
        df['月份'] = pd.to_numeric(df['月份'], errors='coerce').fillna(0).astype(int)
        df['年月'] = df.apply(lambda x: f"{x['年份']}-{x['月份']:02d}" if x['年份'] != 0 and x['月份'] != 0 else None, axis=1)
        df = df[df['年月'].notna()].copy()
    return df

def _calculate_complaint_contribution(df):
    necessary_cols = ['与描述不符投诉数', '未收到货投诉数', '总投诉数', '该月出单数', 'url_prefix', '年月']
    for col in necessary_cols:
        if col not in df.columns:
            df[col] = 0.0
    numeric_cols = ['与描述不符投诉数', '未收到货投诉数', '总投诉数', '该月出单数']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    store_month_totals = df.groupby(['url_prefix', '年月']).agg({'与描述不符投诉数': 'sum', '未收到货投诉数': 'sum', '总投诉数': 'sum', '该月出单数': 'sum'}).reset_index()
    store_month_totals.rename(columns={'与描述不符投诉数': '店铺月_与描述不符总数', '未收到货投诉数': '店铺月_未收到货总数', '总投诉数': '店铺月_总投诉总数', '该月出单数': '店铺月_总订单总数'}, inplace=True)
    df = pd.merge(df, store_month_totals, on=['url_prefix', '年月'], how='left')
    df['与描述不符投诉贡献'] = df.apply(lambda row: (row['与描述不符投诉数'] / row['店铺月_与描述不符总数']) * 100 if row['店铺月_与描述不符总数'] > 0 else 0.0, axis=1)
    df['未收到货投诉贡献'] = df.apply(lambda row: (row['未收到货投诉数'] / row['店铺月_未收到货总数']) * 100 if row['店铺月_未收到货总数'] > 0 else 0.0, axis=1)
    df['总投诉贡献'] = df.apply(lambda row: (row['总投诉数'] / row['店铺月_总投诉总数']) * 100 if row['店铺月_总投诉总数'] > 0 else 0.0, axis=1)
    df['总订单贡献'] = df.apply(lambda row: (row['该月出单数'] / row['店铺月_总订单总数']) * 100 if row['店铺月_总订单总数'] > 0 else 0.0, axis=1)
    df['与描述不符投诉率'] = df.apply(lambda row: (row['与描述不符投诉数'] / row['该月出单数']) * 100 if row['该月出单数'] > 0 else 0.0, axis=1)
    df['未收到货投诉率'] = df.apply(lambda row: (row['未收到货投诉数'] / row['该月出单数']) * 100 if row['该月出单数'] > 0 else 0.0, axis=1)
    df['总投诉率'] = df.apply(lambda row: (row['总投诉数'] / row['该月出单数']) * 100 if row['该月出单数'] > 0 else 0.0, axis=1)
    df.drop(columns=['店铺月_与描述不符总数', '店铺月_未收到货总数', '店铺月_总投诉总数', '店铺月_总订单总数'], inplace=True, errors='ignore')
    contribution_cols = ['与描述不符投诉贡献', '未收到货投诉贡献', '总投诉贡献', '总订单贡献']
    rate_cols = ['与描述不符投诉率', '未收到货投诉率', '总投诉率']
    for col in contribution_cols + rate_cols:
        df[col] = df[col].round(2)
    return df

def get_deepseek_label(review_text: str) -> Dict[str, Any]:
    if pd.isna(review_text) or review_text.strip() == "":
        return {"risk_type": "其他", "reason_analysis": "无评论内容", "severity": "低"}
    review_text = str(review_text)[:2000]
    prompt = f"""你是电商评论分析专家,请分析以下Trustpilot差评：
1. 风险类型（物流/发货风险、质量/假货风险、售后/退款风险、其他）
2. 核心原因（≤50字）
3. 严重程度（高、中、低）
差评内容：{review_text}
严格按JSON格式返回,不要添加任何额外内容：
{{"risk_type":"","reason_analysis":"","severity":""}}"""
    payload = {"model": DEEPSEEK_CONFIG["model"], "messages": [{"role": "user", "content": prompt}], "temperature": DEEPSEEK_CONFIG["temperature"], "max_tokens": DEEPSEEK_CONFIG["max_tokens"], "stream": False}
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {DEEPSEEK_CONFIG['api_key']}"}
    for retry in range(DEEPSEEK_CONFIG["retry_times"]):
        try:
            response = requests.post(DEEPSEEK_CONFIG["api_url"], headers=headers, json=payload, timeout=DEEPSEEK_CONFIG["timeout"])
            response.raise_for_status()
            result = response.json()
            if "choices" in result and len(result["choices"]) > 0:
                content = result["choices"][0]["message"]["content"].strip()
                content = re.sub(r'^```json|```$', '', content).strip()
                label_result = json.loads(content)
                risk_type = label_result.get("risk_type", "其他")
                reason_analysis = label_result.get("reason_analysis", "无法分析")[:50]
                severity = label_result.get("severity", "低")
                valid_risk_types = ["物流/发货风险", "质量/假货风险", "售后/退款风险", "其他"]
                valid_severities = ["高", "中", "低"]
                if risk_type not in valid_risk_types:
                    risk_type = "其他"
                if severity not in valid_severities:
                    severity = "低"
                return {"risk_type": risk_type, "reason_analysis": reason_analysis, "severity": severity}
            else:
                raise ValueError("API返回格式异常,无choices字段")
        except Exception as e:
            error_msg = str(e)
            if retry == DEEPSEEK_CONFIG["retry_times"] - 1:
                st.warning(f"DeepSeek API调用失败（重试{DEEPSEEK_CONFIG['retry_times']}次后）：{error_msg},使用关键词匹配")
                text = review_text.lower()
                risk_type = "其他"
                for rt, keywords in RISK_KEYWORDS.items():
                    if any(kw in text for kw in keywords):
                        risk_type = rt
                        break
                hit_count = sum(1 for kw_list in RISK_KEYWORDS.values() for kw in kw_list if kw in text)
                severity = "高" if hit_count >= 3 else "中" if hit_count >= 1 else "低"
                return {"risk_type": risk_type, "reason_analysis": f"API调用失败,关键词匹配到{hit_count}个风险词", "severity": severity}
            else:
                time.sleep(DEEPSEEK_CONFIG["retry_delay"] * (retry + 1))
                continue
    return {"risk_type": "其他", "reason_analysis": "API调用失败", "severity": "中"}

def summarize_store_risk(store_name: str, bad_reviews: List[str]) -> str:
    if not bad_reviews or all(pd.isna(r) or r.strip() == "" for r in bad_reviews):
        return "暂无差评,店铺运营良好"
    review_sample = "\n\n".join([f"{i + 1}. {str(review)[:500]}" for i, review in enumerate(bad_reviews[:10])])
    prompt = f"""你是电商舆情分析专家,请总结以下店铺的核心风险点：
店铺名称：{store_name}
差评样本（前10条）：
{review_sample}
总结要求：
1. 用3-5个核心要点总结,每个要点不超过20字
2. 语言简洁直观,突出核心问题
3. 按问题严重程度排序
4. 只返回总结内容,不要额外解释"""
    payload = {"model": DEEPSEEK_CONFIG["model"], "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 300, "stream": False}
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {DEEPSEEK_CONFIG['api_key']}"}
    try:
        response = requests.post(DEEPSEEK_CONFIG["api_url"], headers=headers, json=payload, timeout=DEEPSEEK_CONFIG["timeout"])
        response.raise_for_status()
        result = response.json()
        if "choices" in result and len(result["choices"]) > 0:
            summary = result["choices"][0]["message"]["content"].strip()
            summary_items = summary.split('\n')
            formatted_summary = []
            for item in summary_items:
                item = item.strip()
                if item and not item.startswith('###'):
                    item = re.sub(r'^\d+\.?\s*', '', item)
                    if item:
                        formatted_summary.append(item)
            if not formatted_summary:
                formatted_summary = summary.split('\n')[:3]
            formatted_summary = formatted_summary[:5]
            risk_points = [f"{i + 1}. {point}" for i, point in enumerate(formatted_summary) if point]
            return "\n".join(risk_points) if risk_points else "未识别到明显风险点"
        else:
            return "未识别到明显风险点"
    except Exception as e:
        st.warning(f"风险点总结失败：{str(e)}")
        risk_hits = {}
        all_text = " ".join([str(r).lower() for r in bad_reviews])
        for risk_type, keywords in RISK_KEYWORDS.items():
            hits = sum(1 for kw in keywords if kw in text)
            if hits > 0:
                risk_hits[risk_type] = hits
        if risk_hits:
            sorted_risks = sorted(risk_hits.items(), key=lambda x: x[1], reverse=True)
            risk_points = []
            for risk_type, hits in sorted_risks[:3]:
                if risk_type == "物流/发货风险":
                    risk_points.append("1. 物流延迟,大量用户反馈未收到货")
                elif risk_type == "质量/假货风险":
                    risk_points.append("2. 商品质量差,疑似假货/描述不符")
                elif risk_type == "售后/退款风险":
                    risk_points.append("3. 售后无响应,退款申请被拒绝")
            return "\n".join(risk_points)
        else:
            return "未识别到明显风险点"

@st.cache_data(ttl=3600)
def batch_label_reviews(reviews: List[str]) -> List[Dict[str, Any]]:
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    unique_reviews = list({review: idx for idx, review in enumerate(reviews)}.keys())
    unique_results = {}
    for i, review in enumerate(unique_reviews):
        progress = (i + 1) / len(reviews)
        progress_bar.progress(progress)
        status_text.text(f"正在分析第 {i + 1}/{len(reviews)} 条差评...")
        result = get_deepseek_label(review)
        unique_results[review] = result
        time.sleep(0.2)
    results = [unique_results[review] for review in reviews]
    progress_bar.empty()
    status_text.empty()
    return results

def get_store_data():
    try:
        df = pd.read_excel(INPUT_FILE_PATH)
        def parse_date(date_str):
            if pd.isna(date_str):
                return None
            date_formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d']
            for fmt in date_formats:
                try:
                    return datetime.strptime(str(date_str), fmt)
                except:
                    continue
            return pd.to_datetime(date_str, errors='coerce')
        df['published_datetime'] = df['published_date'].apply(parse_date)
        df = df[df['published_datetime'].notna()].copy()
        now = datetime.now()
        cutoff_28d = now - timedelta(days=28)
        cutoff_7d = now - timedelta(days=7)
        df_28d = df[df['published_datetime'] >= cutoff_28d].copy()
        def get_periods():
            periods = []
            for i in range(4):
                end = now - timedelta(days=i * 7)
                start = end - timedelta(days=6)
                if start < cutoff_28d:
                    start = cutoff_28d
                periods.append({'name': f"第{i + 1}周", 'start': start, 'end': end, 'num': i + 1, 'time_range': f"{start.strftime('%m-%d')} ~ {end.strftime('%m-%d')}"})
            return periods[::-1]
        periods = get_periods()
        store_metrics = []
        period_details = []
        for store in df['store_name'].unique():
            df_store = df[df['store_name'] == store].copy()
            df_store_28d = df_28d[df_28d['store_name'] == store].copy()
            total_reviews = len(df_store)
            reviews_28d = len(df_store_28d)
            df_store_7d = df_store_28d[df_store_28d['published_datetime'] >= cutoff_7d].copy()
            ot_7d = len(df_store_7d[df_store_7d['star_rating'] <= 2])
            period_data = {}
            total_28d_ot = 0
            for p in periods:
                mask = (df_store_28d['published_datetime'] >= p['start']) & (df_store_28d['published_datetime'] <= p['end'])
                df_p = df_store_28d[mask].copy()
                total = len(df_p)
                avg_rating = round(df_p['star_rating'].mean(), 2) if total else 0
                ot = len(df_p[df_p['star_rating'] <= 2])
                one_star = len(df_p[df_p['star_rating'] == 1])
                one_star_ratio = round(one_star / total * 100, 2) if total else 0
                risk_hits = {}
                for risk_type, keywords in RISK_KEYWORDS.items():
                    if total > 0:
                        txt_series = df_p[df_p['star_rating'] <= 2]['review_content'].fillna('').str.lower()
                        hits = sum(1 for txt in txt_series if any(kw in txt for kw in keywords))
                        risk_hits[risk_type] = hits
                    else:
                        risk_hits[risk_type] = 0
                period_data[p['num']] = {'period': p['name'], 'time_range': p['time_range'], 'total': total, 'avg_rating': avg_rating, 'ot': ot, 'one_star': one_star, 'one_star_ratio': one_star_ratio, 'risk_hits': risk_hits, 'start': p['start'], 'end': p['end']}
                total_28d_ot += ot
                period_details.append({'store': store, 'period': p['name'], 'time_range': p['time_range'], 'total_reviews': total, 'avg_rating': avg_rating, 'ot_count': ot, 'one_star_ratio': one_star_ratio})
            ot_growth = 0
            if 3 in period_data and period_data[3]['ot'] > 0:
                ot_growth = (period_data[4]['ot'] - period_data[3]['ot']) / period_data[3]['ot']
            total_ot = total_28d_ot
            four_period_over_3 = total_ot > 11
            two_period_rise = False
            if 2 in period_data and 3 in period_data and 4 in period_data:
                two_period_rise = (period_data[3]['ot'] > period_data[2]['ot']) and (period_data[4]['ot'] > period_data[3]['ot'])
            logi_fake = sum([period_data[p]['risk_hits']['物流/发货风险'] + period_data[p]['risk_hits']['质量/假货风险'] for p in period_data])
            keyword_outbreak = logi_fake >= RISK_THRESHOLDS["关键词爆发阈值"]
            risk_level = "safe"
            risk_reason = ""
            if ot_7d >= 5 or four_period_over_3 or (two_period_rise and (period_data[3]['ot'] + period_data[4]['ot'] > 5)):
                risk_level = "high"
                reasons = []
                if ot_7d >= 5:
                    reasons.append("近7天差评≥5条")
                if four_period_over_3:
                    reasons.append("28天差评>11条")
                if two_period_rise:
                    reasons.append("连续2周差评上升")
                if ot_growth >= 0.3:
                    reasons.append(f"差评环比+{ot_growth * 100:.0f}%")
                if keyword_outbreak:
                    reasons.append("物流/假货关键词爆发")
                risk_reason = " | ".join(reasons)
            elif 3 <= ot_7d <= 5:
                risk_level = "medium"
                risk_reason = "近7天差评3-5条"
            else:
                if 3 <= total_28d_ot <= 5 and not two_period_rise:
                    risk_level = "safe"
                    risk_reason = "28天差评3-5条且无上涨"
                else:
                    risk_level = "medium"
                    risk_reason = "未达安全标准"
            action = {"high": "立即核查物流/发货/退款,暂停放量", "medium": "加强监控,每日观察", "safe": "正常维护,每周巡检"}[risk_level]
            avg_28d = round(df_store_28d['star_rating'].mean(), 2) if reviews_28d else 0
            one_star_total = len(df_store_28d[df_store_28d['star_rating'] == 1])
            one_star_ratio_28d = round(one_star_total / reviews_28d * 100, 2) if reviews_28d else 0
            store_metrics.append({"store_name": store, "total_reviews": total_reviews, "reviews_28d": reviews_28d, "avg_rating_28d": avg_28d, "one_star_ratio_28d": one_star_ratio_28d, "ot_28d": total_28d_ot, "ot_7d": ot_7d, "ot_growth": ot_growth, "risk_level": risk_level, "risk_reason": risk_reason, "suggest_action": action, "period_data": period_data})
        df_stores = pd.DataFrame(store_metrics)
        df_periods = pd.DataFrame(period_details)
        return df_stores, df_periods, df
    except Exception as e:
        st.error(f"数据加载失败：{str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def filter_internal_feedback(df, product=None, year=None, month=None, opera_team=None):
    filtered_df = df.copy()
    if product and product != "全部":
        filtered_df = filtered_df[filtered_df['商品'].astype(str).str.contains(product, na=False)]
    if year and year != "全部":
        filtered_df = filtered_df[filtered_df['年份'] == int(year)]
    if month and month != "全部":
        filtered_df = filtered_df[filtered_df['月份'] == int(month)]
    if opera_team and opera_team != "全部":
        filtered_df = filtered_df[filtered_df['opera_team'].astype(str).str.contains(opera_team, na=False)]
    return filtered_df

def export_external_dashboard(df_stores, df_periods, selected_store=None):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_display = df_stores.copy()
        df_display['风险等级'] = df_display['risk_level'].map({'high': '高危', 'medium': '关注', 'safe': '安全'})
        df_display['差评环比'] = df_display['ot_growth'].apply(lambda x: f"{x * 100:.1f}%")
        df_display = df_display[['store_name', '风险等级', 'total_reviews', 'reviews_28d', 'avg_rating_28d', 'one_star_ratio_28d', 'ot_7d', '差评环比', 'risk_reason', 'suggest_action']]
        df_display.columns = ['店铺域名', '风险等级', '总评论数', '近28天评论', '平均星级', '1星占比(%)', '近7天差评', '差评环比', '风险原因', '建议动作']
        df_display.to_excel(writer, sheet_name='店铺风险总览', index=False)
        df_periods_export = df_periods.copy()
        df_periods_export.columns = ['店铺域名', '周期', '时间范围', '总评论数', '平均星级', '1+2星差评数', '1星占比(%)']
        df_periods_export.to_excel(writer, sheet_name='28天周期趋势', index=False)
        if selected_store:
            _, _, df_reviews = get_store_data()
            df_store_reviews = df_reviews[df_reviews['store_name'] == selected_store].copy()
            cutoff_date = datetime.now() - timedelta(days=28)
            df_bad_reviews = df_store_reviews[(df_store_reviews['star_rating'] <= 2) & (df_store_reviews['published_datetime'] >= cutoff_date)].copy()
            if not df_bad_reviews.empty:
                reviews_list = df_bad_reviews['review_content'].tolist()
                label_results = batch_label_reviews(reviews_list)
                df_bad_reviews['发布时间'] = df_bad_reviews['published_datetime'].dt.strftime('%Y-%m-%d')
                df_bad_reviews['AI风险类型'] = [r['risk_type'] for r in label_results]
                df_bad_reviews['AI原因分析'] = [r['reason_analysis'] for r in label_results]
                df_bad_reviews['AI严重程度'] = [r['severity'] for r in label_results]
                df_bad_reviews_export = df_bad_reviews[['发布时间', 'star_rating', 'review_title', 'review_content', 'AI风险类型', 'AI原因分析', 'AI严重程度']]
                df_bad_reviews_export.columns = ['发布时间', '星级', '标题', '评论内容', 'AI风险类型', 'AI原因分析', 'AI严重程度']
                df_bad_reviews_export.to_excel(writer, sheet_name=f'{selected_store}_差评分析', index=False)
        output.seek(0)
        return output.getvalue()

def show_store_30d_performance(selected_store: str):
    st.subheader("📈 店铺近30天表现")
    df_paypal_30d = load_paypal_30d_data()
    if df_paypal_30d.empty:
        st.info("暂无店铺近30天表现数据")
        return
    df_store_30d = df_paypal_30d[df_paypal_30d['url_prefix'] == selected_store].copy()
    if df_store_30d.empty:
        st.info(f"店铺 {selected_store} 暂无近30天表现数据")
        return
    cols = st.columns(4)
    total_orders = df_store_30d['近30天出单数'].sum()
    total_complaints = df_store_30d['总投诉数'].sum()
    desc_complaints = df_store_30d['与描述不符投诉数'].sum()
    delivery_complaints = df_store_30d['未收到货投诉数'].sum()
    with cols[0]:
        st.metric("近30天出单数", total_orders)
    with cols[1]:
        st.metric("总投诉数", total_complaints)
    with cols[2]:
        st.metric("与描述不符投诉数", desc_complaints)
    with cols[3]:
        st.metric("未收到货投诉数", delivery_complaints)
    display_cols = ['店铺', '落地页', '产品SKU', '运营团队', '运营人员', '近30天出单数', '与描述不符投诉数', '未收到货投诉数', '总投诉数', '与描述不符投诉率', '未收到货投诉率', '总投诉率']
    display_cols = [col for col in display_cols if col in df_store_30d.columns]
    st.dataframe(df_store_30d[display_cols], width='stretch', hide_index=True)

def show_shipping_performance_module(selected_store: str):
    st.subheader("📦 发货表现分析")
    df_shipping = load_shipping_data()
    if df_shipping.empty:
        st.info("暂无发货表现数据")
        return
    df_store_shipping = df_shipping[df_shipping['url_prefix'] == selected_store].copy()
    if df_store_shipping.empty:
        st.info(f"店铺 {selected_store} 暂无发货表现数据")
        return
    rate_columns = []
    for col in df_store_shipping.columns:
        if any(keyword in col.lower() for keyword in ['率', 'rate', 'ratio', 'percent', '%']):
            rate_columns.append(col)
    if not rate_columns:
        st.warning("发货数据中未识别到比率指标列")
        return
    cols = st.columns(min(5, len(rate_columns) + 1))
    with cols[0]:
        st.metric("数据记录数", len(df_store_shipping))
    for idx, col in enumerate(rate_columns[:4]):
        if col in df_store_shipping.columns:
            avg_val = df_store_shipping[col].mean()
            if pd.notna(avg_val):
                with cols[idx + 1]:
                    st.metric(col, f"{avg_val:.2f}%")
    st.subheader("📊 发货表现详细数据")
    display_cols = []
    for col in df_store_shipping.columns:
        if col != 'url_prefix':
            display_cols.append(col)
    def highlight_shipping_rate(val, col_name):
        if pd.isna(val):
            return None
        if '7天' in col_name or '7' in col_name and '天' in col_name:
            if val < 0.7:
                return 'background-color: #FFCDD2; color: #D32F2F; font-weight: bold;'
        if '14天' in col_name:
            if val < 0.8:
                return 'background-color: #FFCDD2; color: #D32F2F; font-weight: bold;'
        if '28天' in col_name:
            if val < 0.9:
                return 'background-color: #FFCDD2; color: #D32F2F; font-weight: bold;'
        return None
    styled_df = df_store_shipping[display_cols].style
    for col in rate_columns:
        if col in display_cols:
            styled_df = styled_df.format({col: lambda x: f"{x * 100:.2f}%" if pd.notna(x) else ''})
    for col in display_cols:
        if col in rate_columns:
            styled_df = styled_df.applymap(lambda x: highlight_shipping_rate(x, col), subset=[col])
    st.dataframe(styled_df, width='stretch', hide_index=True)
    col_export, _ = st.columns([1, 9])
    with col_export:
        excel_data = convert_df_to_excel(df_store_shipping)
        st.download_button(label="📥 导出发货数据", data=excel_data, file_name=f"{selected_store}_发货表现数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

def show_paypal_complaint_module(selected_store: str):
    st.subheader("💳 PayPal投诉情况分析")
    df_paypal = load_paypal_risk_data()
    if df_paypal.empty:
        st.info("暂无PayPal投诉数据")
        return
    df_store_paypal = df_paypal[df_paypal['url_prefix'] == selected_store].copy()
    if df_store_paypal.empty:
        st.info(f"店铺 {selected_store} 暂无PayPal投诉数据")
        return
    col_filter, col_download = st.columns([2, 1])
    with col_filter:
        year_month_list = sorted(df_store_paypal['年月'].unique())
        selected_year_month = st.selectbox("选择统计年月", year_month_list, index=0, key=f"paypal_year_month_{selected_store}")
        df_filtered = df_store_paypal[df_store_paypal['年月'] == selected_year_month].copy()
    with col_download:
        excel_data = convert_df_to_excel(df_filtered)
        st.download_button(label="📥 导出该年月数据", data=excel_data, file_name=f"{selected_store}_PayPal投诉数据_{selected_year_month}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    total_complaints = df_filtered['总投诉数'].sum()
    total_orders = df_filtered['该月出单数'].sum()
    total_desc_rate = (df_filtered['与描述不符投诉数'].sum() / total_orders * 100) if total_orders > 0 else 0
    total_delivery_rate = (df_filtered['未收到货投诉数'].sum() / total_orders * 100) if total_orders > 0 else 0
    overall_complaint_rate = (total_complaints / total_orders * 100) if total_orders > 0 else 0
    with col1:
        st.metric(f"{selected_year_month} 总投诉数", total_complaints)
    with col2:
        st.metric(f"{selected_year_month} 总订单数", total_orders)
    with col3:
        st.metric("与描述不符投诉率", f"{total_desc_rate:.2f}%")
    with col4:
        st.metric("未收到货投诉率", f"{total_delivery_rate:.2f}%")
    with col5:
        st.metric("整体投诉率", f"{overall_complaint_rate:.2f}%")
    st.subheader(f"{selected_year_month} - 详细投诉数据")
    keep_cols = ['年月', '产品SKU', '运营团队', 'prod_url', '该月出单数', '与描述不符投诉数', '与描述不符投诉率', '与描述不符投诉贡献', '未收到货投诉数', '未收到货投诉率', '未收到货投诉贡献', '总投诉数', '总投诉率', '总投诉贡献', '总订单贡献']
    keep_cols = [col for col in keep_cols if col in df_filtered.columns]
    df_display_paypal = df_filtered[keep_cols].copy()
    st.dataframe(df_display_paypal, width='stretch', hide_index=True, on_select="rerun", selection_mode="multi-row", key=f"paypal_complaint_table_{selected_store}")
    selected_rows = st.session_state.get(f"paypal_complaint_table_{selected_store}", {}).get('selection', {}).get('rows', [])
    if selected_rows:
        selected_row = df_display_paypal.iloc[selected_rows[0]]
        selected_sku = selected_row.get('产品SKU', '')
        selected_team = selected_row.get('运营团队', '')
        st.subheader(f"📄 {selected_sku} - 每日舆情")
        df_internal = load_internal_feedback_data()
        if not df_internal.empty:
            mask = (df_internal['商品_清理'] == str(selected_sku).strip()) & (df_internal['opera_team_清理'] == str(selected_team).strip())
            df_filtered_internal = df_internal[mask].copy()
            if not df_filtered_internal.empty:
                display_cols = ['年份', '月份', '商品', 'opera_team', '投诉原因总结']
                st.dataframe(df_filtered_internal[display_cols], width='stretch', hide_index=True)
                excel_data = convert_df_to_excel(df_filtered_internal)
                st.download_button(label="📥 导出该商品内部舆情数据", data=excel_data, file_name=f"{selected_sku}_内部舆情数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info(f"未找到SKU:{selected_sku} 运营团队:{selected_team} 的内部舆情数据")

def show_complaint_query_module():
    st.title("📋 内部舆情看板 - 客诉查询", anchor=False)
    st.divider()
    df_feedback = load_internal_feedback_data()
    if df_feedback.empty:
        st.warning("暂无内部舆情数据")
        return
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        product_list = ["全部"] + sorted(df_feedback['商品'].dropna().unique().tolist())
        selected_product = st.selectbox("商品", product_list, index=0)
    with col2:
        year_list = ["全部"] + sorted(df_feedback['年份'].dropna().unique().astype(int).tolist())
        selected_year = st.selectbox("年份", year_list, index=0)
    with col3:
        month_list = ["全部"] + sorted(df_feedback['月份'].dropna().unique().astype(int).tolist())
        selected_month = st.selectbox("月份", month_list, index=0)
    with col4:
        team_list = ["全部"] + sorted(df_feedback['opera_team'].dropna().unique().tolist())
        selected_team = st.selectbox("运营团队", team_list, index=0)
    df_filtered = filter_internal_feedback(df_feedback, product=selected_product, year=selected_year, month=selected_month, opera_team=selected_team)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("总记录数", len(df_filtered))
    with col2:
        st.metric("涉及商品数", df_filtered['商品'].nunique())
    with col3:
        st.metric("涉及年份数", df_filtered['年份'].nunique())
    with col4:
        st.metric("涉及运营团队数", df_filtered['opera_team'].nunique())
    st.divider()
    st.markdown("⚠️ 内部风险看板 - 运营团队风险分布")
    if not df_filtered.empty:
        team_stats = df_filtered.groupby('opera_team').size().reset_index(name='风险数量')
        team_stats = team_stats.sort_values('风险数量', ascending=False).head(10)
        fig = go.Figure(data=[go.Bar(x=team_stats['opera_team'], y=team_stats['风险数量'], marker=dict(color=team_stats['风险数量'], colorscale='Reds', showscale=True, colorbar=dict(title="风险数量")), text=team_stats['风险数量'], textposition='outside')])
        fig.update_layout(xaxis_title="运营团队", yaxis_title="风险记录数", height=450, xaxis_tickangle=-45, margin=dict(l=20, r=20, t=60, b=100))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("当前筛选条件下无数据，无法生成风险分布图表")
    st.divider()
    st.markdown("### 📄 风险分析报告")
    col1, col2 = st.columns([3, 1])
    with col1:
        report_format = st.selectbox("选择报告格式", ["HTML"], key="complaint_report_format")
    with col2:
        generate_report = st.button("🚨 生成风险分析报告", use_container_width=True, key="generate_complaint_report")
    with st.expander("📋 查看风险报告预览", expanded=True):
        if not generate_report:
            st.info("请点击上方【生成风险分析报告】按钮后查看预览")
        else:
            with st.spinner("正在生成报告..."):
                report_title = f"客诉查询风险分析报告_{selected_product}_{selected_team}"
                data_stats = [{"label": "总记录数", "value": len(df_filtered), "risk": "high"}, {"label": "涉及商品数", "value": df_filtered['商品'].nunique()}, {"label": "涉及年份数", "value": df_filtered['年份'].nunique()}, {"label": "涉及运营团队数", "value": df_filtered['opera_team'].nunique()}]
                add_report_export_ui("internal", df_filtered, report_title, data_stats)
    st.divider()
    st.markdown("### 📊 数据详情")
    display_cols = ['年份', '月份', '商品', 'opera_team', '投诉原因总结']
    display_cols = [col for col in display_cols if col in df_filtered.columns]
    col_display, col_export = st.columns([9, 1])
    with col_display:
        st.dataframe(df_filtered[display_cols], width='stretch', hide_index=True, use_container_width=True)
    with col_export:
        st.subheader("")
        excel_data = convert_df_to_excel(df_filtered)
        st.download_button(label="📥 导出Excel", data=excel_data, file_name=f"客诉查询数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

def show_internal_feedback_subpage(subpage):
    if subpage == "数据总览":
        st.title("📈 内部舆情看板 - 数据总览", anchor=False)
        st.divider()
        show_data_overview_module()
    elif subpage == "每日舆情":
        st.title("📈 客户声音监听", anchor=False)
        st.divider()
        show_daily_sentiment_module()
    elif subpage == "客诉查询":
        show_complaint_query_module()

def show_risk_rules():
    st.markdown("""
### 📋 Trustpilot店铺风险判定规则
#### 一、核心判定维度
| 维度 | 判定标准 |
|------|----------|
| 近7天差评数 | 统计最近7天内1-2星评论数量 |
| 28天差评总数 | 统计近28天内1-2星评论总数 |
| 差评环比增长 | 最新一周 vs 上一周 差评数增长率 |
| 连续增长趋势 | 是否连续2周差评数上升 |
| 风险关键词爆发 | 物流/假货相关关键词命中数 ≥8 |
#### 二、风险等级判定逻辑
**🔴 高危店铺（满足任一条件）：**
- 近7天差评 ≥5条
- 28天差评 >11条
- 连续2周差评上升 且 近2周差评总数 >5条
- 差评环比增长 ≥30%
- 物流/假货关键词爆发（≥8个）
**🟡 关注店铺（满足任一条件）：**
- 近7天差评 3-5条
- 28天差评 3-5条 但有上涨趋势
- 未达安全标准的其他情况
**🟢 安全店铺（满足所有条件）：**
- 近7天差评 <3条
- 28天差评 ≤5条 且 无连续上涨趋势
- 无关键词爆发情况
#### 三、风险应对建议
| 风险等级 | 建议动作 |
|----------|----------|
| 高危 | 立即核查物流/发货/退款,暂停放量 |
| 关注 | 加强监控,每日观察 |
| 安全 | 正常维护,每周巡检 |
""")
    keywords_df = pd.DataFrame([{"风险类型": k, "关键词": ", ".join(v)} for k, v in RISK_KEYWORDS.items()])
    st.dataframe(keywords_df, width='stretch', hide_index=True)
    st.markdown(f"""
#### 五、阈值配置
- 关键词爆发阈值：{RISK_THRESHOLDS['关键词爆发阈值']}个
- 差评统计范围：1-2星评论
- 时间周期：近7天 / 近28天（拆分为4个7天周期）
""")

def show_external_feedback_dashboard():
    st.subheader("🌐 外部舆情风险监控（Trustpilot）", divider='blue')
    col_title, col_button_rule = st.columns([10, 2])
    with col_button_rule:
        if st.button("📋 查看风险判定规则", use_container_width=True):
            st.session_state.show_rules = not st.session_state.get('show_rules', False)
    if st.session_state.get('show_rules', False):
        with st.expander("风险判定规则详情", expanded=True):
            show_risk_rules()
    st.divider()
    df_stores, df_periods, df_reviews = get_store_data()
    if df_stores.empty:
        st.warning("暂无外部舆情数据,请确认数据文件是否存在")
        return
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric(label="监控店铺总数", value=len(df_stores))
    with col2:
        high_risk = len(df_stores[df_stores['risk_level'] == 'high'])
        st.metric(label="高危店铺数", value=high_risk, delta=f"{high_risk / len(df_stores) * 100:.0f}%", delta_color="inverse")
    with col3:
        medium_risk = len(df_stores[df_stores['risk_level'] == 'medium'])
        st.metric(label="关注店铺数", value=medium_risk, delta=f"{medium_risk / len(df_stores) * 100:.0f}%", delta_color="off")
    with col4:
        safe_risk = len(df_stores[df_stores['risk_level'] == 'safe'])
        st.metric(label="安全店铺数", value=safe_risk, delta=f"{safe_risk / len(df_stores) * 100:.0f}%", delta_color="normal")
    with col5:
        st.metric(label="近28天总评论", value=df_stores['reviews_28d'].sum())
    with col6:
        st.metric(label="近7天差评总数", value=df_stores['ot_7d'].sum())
    st.divider()
    col1, col2 = st.columns([1, 2])
    with col1:
        fig_pie = create_risk_overview_chart(df_stores)
        st.plotly_chart(fig_pie, use_container_width=True)
    with col2:
        high_stores = df_stores[df_stores['risk_level'] == 'high']['store_name'].head(5).tolist()
        trend_data = []
        for store in high_stores:
            store_periods = df_periods[df_periods['store'] == store]
            for _, row in store_periods.iterrows():
                trend_data.append({'店铺': store, '周期': row['period'], '差评数': row['ot_count']})
        if trend_data:
            df_trend = pd.DataFrame(trend_data)
            fig_trend = px.line(df_trend, x='周期', y='差评数', color='店铺', title='高危店铺TOP5 - 差评趋势', markers=True)
            fig_trend.update_layout(height=400, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("暂无高危店铺趋势数据")
    st.divider()
    st.subheader("店铺风险总览列表")
    df_display = df_stores.copy()
    df_display['风险等级'] = df_display['risk_level'].map({'high': '🔴 高危', 'medium': '🟡 关注', 'safe': '🟢 安全'})
    df_display['差评环比'] = df_display['ot_growth'].apply(lambda x: f"{x * 100:.1f}%")
    df_display = df_display[['store_name', '风险等级', 'total_reviews', 'reviews_28d', 'avg_rating_28d', 'one_star_ratio_28d', 'ot_7d', '差评环比', 'risk_reason', 'suggest_action']]
    df_display.columns = ['店铺域名', '风险等级', '总评论数', '近28天评论', '平均星级', '1星占比(%)', '近7天差评', '差评环比', '风险原因', '建议动作']
    st.dataframe(df_display, width='stretch', hide_index=True, use_container_width=True)
    col_export, _ = st.columns([1, 9])
    with col_export:
        excel_data = export_external_dashboard(df_stores, df_periods)
        st.download_button(label="📥 导出完整看板数据", data=excel_data, file_name=f"外部舆情看板_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    st.divider()
    st.subheader("🔍 单店铺详情分析")
    selected_store = st.selectbox("选择店铺查看详情", df_stores['store_name'].tolist(), index=0)
    if selected_store:
        store_data = df_stores[df_stores['store_name'] == selected_store].iloc[0]
        df_store_reviews = df_reviews[df_reviews['store_name'] == selected_store].copy()
        risk_icon = {'high': '🔴', 'medium': '🟡', 'safe': '🟢'}[store_data['risk_level']]
        risk_text = {'high': '高危', 'medium': '关注', 'safe': '安全'}[store_data['risk_level']]
        st.markdown(f"### {selected_store} | {risk_icon} {risk_text}")
        st.markdown(f"""
<div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; margin: 10px 0;">
<h4 style="margin-top: 0;">风险判定结果 {risk_icon} {risk_text}</h4>
<p><strong>判定依据：</strong>{store_data['risk_reason']}</p>
<p><strong>建议动作：</strong>{store_data['suggest_action']}</p>
</div>
""", unsafe_allow_html=True)
        cutoff_date = datetime.now() - timedelta(days=28)
        df_bad_reviews = df_store_reviews[(df_store_reviews['star_rating'] <= 2) & (df_store_reviews['published_datetime'] >= cutoff_date)].copy()
        bad_reviews_list = df_bad_reviews['review_content'].tolist()
        paypal_data = load_paypal_risk_data()
        if not paypal_data.empty:
            paypal_store_data = paypal_data[paypal_data['url_prefix'] == selected_store].copy()
            product_skus = paypal_store_data['产品SKU'].dropna().unique().tolist() if '产品SKU' in paypal_store_data.columns else []
        else:
            paypal_store_data = None
            product_skus = []
        internal_feedback_data = get_store_internal_feedback(selected_store, product_skus)
        shipping_data = load_shipping_data()
        if not shipping_data.empty:
            shipping_store_data = shipping_data[shipping_data['url_prefix'] == selected_store].copy()
        else:
            shipping_store_data = None
        charts = {}
        fig_risk = create_risk_overview_chart(df_stores)
        charts['全部店铺风险等级分布'] = fig_risk
        fig_trend = create_risk_trend_chart(store_data)
        charts['店铺风险趋势分析'] = fig_trend
        if paypal_store_data is not None and not paypal_store_data.empty:
            selected_year_month = paypal_store_data['年月'].max()
            fig_paypal = create_paypal_complaint_chart(paypal_store_data, selected_year_month)
            if fig_paypal:
                charts['PayPal投诉分析'] = fig_paypal
            fig_rate = create_complaint_rate_chart(paypal_store_data)
            if fig_rate:
                charts['PayPal投诉率趋势'] = fig_rate
        if internal_feedback_data is not None and not internal_feedback_data.empty:
            fig_internal = create_internal_risk_distribution_chart(internal_feedback_data)
            if fig_internal:
                charts['内部风险分布'] = fig_internal
        report_title = f"外部舆情风险分析报告_{selected_store}_{risk_text}"
        data_stats = [{"label": "风险等级", "value": risk_text, "risk": store_data['risk_level']}, {"label": "近7天差评数", "value": store_data['ot_7d'], "risk": "high" if store_data['ot_7d'] >= 5 else "medium" if store_data['ot_7d'] >= 3 else "safe"}, {"label": "近28天差评数", "value": store_data['ot_28d'], "risk": "high" if store_data['ot_28d'] > 11 else "medium"}, {"label": "平均星级", "value": f"{store_data['avg_rating_28d']}/5.0"}]
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"""
<div style="padding: 15px; border-radius: 8px; background: #f8f9fa; text-align: center;">
<h4>店铺总评分</h4>
<h2>{store_data['avg_rating_28d']}/5.0</h2>
<p>1星占比: {store_data['one_star_ratio_28d']}%</p>
</div>
""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
<div style="padding: 15px; border-radius: 8px; background: #f8f9fa; text-align: center;">
<h4>近28天概览</h4>
<h2>{store_data['reviews_28d']}</h2>
<p>1+2星差评: {store_data['ot_28d']}</p>
<p>日均差评: {store_data['ot_28d'] / 28:.1f}</p>
</div>
""", unsafe_allow_html=True)
        st.divider()
        show_store_30d_performance(selected_store)
        st.divider()
        col1, col2 = st.columns([1, 1])
        with col1:
            st.subheader("28天周期趋势")
            period_items = sorted(store_data['period_data'].items(), key=lambda x: x[0])
            period_list = []
            for idx, (p_num, p_data) in enumerate(period_items):
                period_list.append({'周期': f"{p_data['period']} ({p_data['time_range']})", '评论数': p_data['total'], '平均星级': p_data['avg_rating'], '1+2星差评': p_data['ot']})
            st.dataframe(pd.DataFrame(period_list), width='stretch', hide_index=True, use_container_width=True)
        with col2:
            fig = create_risk_trend_chart(store_data)
            st.plotly_chart(fig, use_container_width=True)
        st.divider()
        st.subheader("最新差评原文（近28天）- AI智能分析")
        if not df_bad_reviews.empty:
            df_bad_reviews = df_bad_reviews.sort_values('published_datetime', ascending=False).head(20)
            reviews_list = df_bad_reviews['review_content'].tolist()
            label_results = batch_label_reviews(reviews_list)
            df_bad_reviews['发布时间'] = df_bad_reviews['published_datetime'].dt.strftime('%Y-%m-%d')
            df_bad_reviews['完整内容'] = df_bad_reviews['review_content']
            df_bad_reviews['AI风险类型'] = [r['risk_type'] for r in label_results]
            df_bad_reviews['AI原因分析'] = [r['reason_analysis'] for r in label_results]
            df_bad_reviews['AI严重程度'] = [r['severity'] for r in label_results]
            st.dataframe(df_bad_reviews[['发布时间', 'star_rating', 'review_title', '完整内容', 'AI风险类型', 'AI原因分析', 'AI严重程度']], width='stretch', hide_index=True, use_container_width=True)
        else:
            st.info("近28天暂无1-2星差评")
        st.divider()
        show_paypal_complaint_module(selected_store)
        st.divider()
        show_shipping_performance_module(selected_store)

# =========================== 首页模块 ===========================
def show_homepage():
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("# 舆情风险监控系统")
    with col2:
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.markdown(f"<p style='text-align:right; margin-top:20px;'>更新时间：{update_time}</p>", unsafe_allow_html=True)
    st.markdown("---")
    with st.expander("⚡ 快速开始", expanded=True):
        st.markdown("""
- **舆情预警**：实时查看最新风险事件
- **报告生成**：一键导出周期舆情分析报告
- **智能分析**：基于AI的情感倾向与趋势预测
""")
        st.info("👈 左侧导航树可查看完整菜单，下方选择器可快速切换演示内容")
    st.markdown("## 监听功能核心模块")
    col_left, col_right = st.columns(2)
    with col_left:
        with st.container(border=True):
            st.subheader("📞 客户声音监听")
            img_path = "customer_voice.png"
            online_fallback = "https://picsum.photos/id/20/400/200"
            img = load_image(img_path, online_fallback)
            if isinstance(img, Image.Image):
                st.image(img, use_container_width=True)
            else:
                st.image(online_fallback, caption="示例图片", use_container_width=True)
            st.markdown("**功能描述**")
            st.markdown("""
- 数据总览：实时展示周期内总文本数、总商品数；自动统计问题分类 TOP10，定位高风险 SKU；支持按季度、月度等自定义周期筛选数据
- 客户声音监听（每日舆情）：按二级标签呈现投诉量趋势；支持二三级标签下钻分析占比；自动生成高投诉商品黑榜 TOP10；支持一键导出每日舆情报表
- 客诉查询：支持多维度精准筛选客诉数据；以热力图展示各运营团队客诉风险分布；自动生成 HTML 风险分析报告；支持客诉明细溯源及 Excel 导出
""")
            st.write("")
    with col_right:
        with st.container(border=True):
            st.subheader("🌐 网络舆情监听")
            img_path2 = "online_opinion.png"
            online_fallback2 = "https://picsum.photos/id/26/400/200"
            img2 = load_image(img_path2, online_fallback2)
            if isinstance(img2, Image.Image):
                st.image(img2, use_container_width=True)
            else:
                st.image(online_fallback2, caption="示例图片", use_container_width=True)
            st.markdown("**功能描述**")
            st.markdown("""
- 全局舆情总览：实时展示总舆情数、差评数、安全店铺数等核心指标，通过风险分布饼图、TOP5 店铺差评趋势图呈现健康度并预警风险。
- 店铺风险总览：展示全店铺风险等级、评论数、星级、差评率等信息，支持分级筛选，实现重点店铺专项监控。
- 单店铺深度分析：生成店铺全景画像与 28 天舆情趋势，AI 解析差评原文并完成风险标签分级，快速溯源问题。
- 全链路风险闭环：整合 PayPal 投诉、物流发货数据，打通舆情、支付、物流全链路，定位风险根源，支撑运营与供应链优化
""")
    st.markdown("---")
    st.caption("舆情风险监控系统 © 实时守护品牌声誉 | 数据动态更新")

# =========================== 主函数 ===========================
def main():
    with st.sidebar:
        st.title("系统导航")
        st.markdown("---")
        st.markdown("**首页**")
        main_page = st.radio("主菜单", ["首页", "内部舆情风险监控", "外部舆情风险监控"], label_visibility="collapsed")
        st.markdown("---")
        with st.expander("**内部舆情风险监控**", expanded=False):
            internal_subpage = st.radio("子菜单", ["数据总览", "每日舆情", "客诉查询"], label_visibility="collapsed")
        st.markdown("---")
        st.caption(f"更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if main_page == "首页":
        show_homepage()
    elif main_page == "内部舆情风险监控":
        show_internal_feedback_subpage(internal_subpage)
    elif main_page == "外部舆情风险监控":
        st.title("外部舆情风险监控", anchor=False)
        st.divider()
        show_external_feedback_dashboard()

if __name__ == "__main__":
    main()
