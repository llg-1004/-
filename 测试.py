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
import datetime
today_date = datetime.datetime.now().strftime("%Y%m%d")

# 1. 每日打标文件夹（GitHub 上的文件夹）
DAILY_DATA_PATH = f"./每日打标/{today_date}_文本处理.xlsx"

# 2. 店铺爬取文件夹（GitHub 上的文件夹）
SAVE_ROOT_PATH = "./店铺爬取/"

# 3. 店铺爬取里的所有文件（固定名字，不要改）
INPUT_FILE_PATH = SAVE_ROOT_PATH + "all_trustpilot_reviews.xlsx"
INTERNAL_FEEDBACK_PATH = SAVE_ROOT_PATH + "llg_zk_prod_feedback.xlsx"
PAYPAL_30D_PATH = SAVE_ROOT_PATH + "paypal_complaint_analysis_2025.xlsx"
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

def generate_data_table_html(df, title, columns=None):
    if df.empty:
        return f"<p>暂无 {title} 数据</p>"
    cols = columns if columns else df.columns.tolist()
    df = df[cols].copy()
    html = f"""
    <div class="section">
        <h3>📋 {title}</h3>
        <table class="data-table">
            <thead><tr>{"".join([f"<th>{c}</th>" for c in cols])}</tr></thead>
            <tbody>
    """
    for _, row in df.iterrows():
        html += "<tr>"
        for col in cols:
            val = row[col]
            if pd.isna(val):
                val = ""
            html += f"<td>{val}</td>"
        html += "</tr>"
    html += """
            </tbody>
        </table>
    </div>
    """
    return html

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
    folder_path = r"./每日打标"
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
        st.warning("未读取到任何每日舆情数据，请检查路径：./每日打标")
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
{charts_html if charts_base64 else ''}
{paypal_analysis_html if paypal_data is not None else ''}
{shipping_analysis_html if shipping_data is not None else ''}
{internal_feedback_html if internal_feedback_data is not None else ''}
"""
    }
    return html_content

# ====================== 主页面入口 ======================
def main():
    st.title("📊 电商舆情与风险监控系统")
    st.sidebar.title("导航菜单")
    menu = ["首页", "数据总览", "趋势统计", "详细投诉数据", "发货表现分析"]
    choice = st.sidebar.radio("选择功能模块", menu, index=0)
    
    if choice == "首页":
        st.success("👋 欢迎使用舆情风险监控系统")
        st.markdown("### 系统功能说明")
        st.info("• 数据总览：查看质量、物流、售后等问题TOP10商品")
        st.info("• 趋势统计：按时间/标签统计投诉趋势、导出黑榜")
        st.info("• 详细投诉数据：单条投诉详情 + 自动匹配发货数据")
        st.info("• 发货表现分析：投诉与物流、时效、异常联动分析")
        
    elif choice == "数据总览":
        show_data_overview_module()
        
    elif choice == "趋势统计":
        show_daily_sentiment_module()
        
    elif choice == "详细投诉数据":
        show_complaint_detail_module()
        
    elif choice == "发货表现分析":
        show_shipping_analysis_module()

if __name__ == "__main__":
    main()
