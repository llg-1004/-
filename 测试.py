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

# -------------------------- 全局配置 --------------------------
st.set_page_config(
    page_title="Trustpilot店铺风险看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 配色方案
COLORS = {
    "high": "#D82C20",  # 高危-红色
    "medium": "#F5A623",  # 关注-橙色
    "safe": "#28A745",  # 安全-绿色
    "text": "#333333",  # 文字-深灰
    "bg": "#F8F9FA",  # 背景-浅灰
    "border": "#E5E7EB"  # 边框-淡灰
}

# 路径配置（按你的实际路径修改）
SAVE_ROOT_PATH = "E:\\Desktop\\店铺爬取\\"
INPUT_FILE_PATH = f"{SAVE_ROOT_PATH}all_trustpilot_reviews.xlsx"

# 风险配置
RISK_KEYWORDS = {
    "物流/发货风险": ["not received", "no delivery", "late", "shipping", "tracking", "never arrived", "delayed",
                      "missing", "lost"],
    "质量/假货风险": ["fake", "scam", "poor quality", "broken", "not as described", "defective", "garbage",
                      "counterfeit", "bad quality"],
    "售后/退款风险": ["no refund", "ignore", "no reply", "worst service", "cheat", "dishonest", "refund denied",
                      "customer service bad"]
}

RISK_THRESHOLDS = {
    "关键词爆发阈值": 8,
}

# DeepSeek API配置（已修正）
DEEPSEEK_CONFIG = {
    "api_key": "sk-c1e135697db64a23830116cba1831272",  # 请替换为有效的API Key
    "api_url": "https://api.deepseek.com/v1/chat/completions",  # 修正完整的API路径
    "model": "deepseek-chat",
    "temperature": 0.2,
    "max_tokens": 1000,  # 降低max_tokens，避免超限
    "timeout": 30,
    "retry_times": 3,  # 重试次数
    "retry_delay": 1  # 重试延迟（秒）
}


# -------------------------- 风险判定规则展示函数 --------------------------
def show_risk_rules():
    """展示风险判定规则的弹窗/板块"""
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
    | 高危 | 立即核查物流/发货/退款，暂停放量 |
    | 关注 | 加强监控，每日观察 |
    | 安全 | 正常维护，每周巡检 |

    #### 四、关键词定义
    """)

    # 展示风险关键词表格
    keywords_df = pd.DataFrame([
        {"风险类型": k, "关键词": ", ".join(v)} for k, v in RISK_KEYWORDS.items()
    ])
    st.dataframe(keywords_df, width='stretch', hide_index=True)

    # 阈值配置
    st.markdown(f"""
    #### 五、阈值配置
    - 关键词爆发阈值：{RISK_THRESHOLDS['关键词爆发阈值']}个
    - 差评统计范围：1-2星评论
    - 时间周期：近7天 / 近28天（拆分为4个7天周期）
    """)


# -------------------------- DeepSeek API 调用函数（已修复） --------------------------
def get_deepseek_label(review_text: str) -> Dict[str, Any]:
    """
    调用DeepSeek API分析评论风险
    增加重试机制、完整URL、参数校验和更健壮的异常处理
    """
    # 空值处理
    if pd.isna(review_text) or review_text.strip() == "":
        return {"risk_type": "其他", "reason_analysis": "无评论内容", "severity": "低"}

    # 限制评论长度，避免API请求过大
    review_text = str(review_text)[:2000]

    prompt = f"""
    你是电商评论分析专家，请分析以下Trustpilot差评：
    1. 风险类型（物流/发货风险、质量/假货风险、售后/退款风险、其他）
    2. 核心原因（≤50字）
    3. 严重程度（高、中、低）
    差评内容：{review_text}
    严格按JSON格式返回，不要添加任何额外内容：
    {{"risk_type":"","reason_analysis":"","severity":""}}
    """

    # 构建请求参数
    payload = {
        "model": DEEPSEEK_CONFIG["model"],
        "messages": [{"role": "user", "content": prompt}],
        "temperature": DEEPSEEK_CONFIG["temperature"],
        "max_tokens": DEEPSEEK_CONFIG["max_tokens"],
        "stream": False
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_CONFIG['api_key']}"
    }

    # 带重试机制的API调用
    for retry in range(DEEPSEEK_CONFIG["retry_times"]):
        try:
            response = requests.post(
                DEEPSEEK_CONFIG["api_url"],
                headers=headers,
                json=payload,
                timeout=DEEPSEEK_CONFIG["timeout"]
            )

            # 状态码检查
            response.raise_for_status()

            # 解析响应
            result = response.json()

            # 提取并验证返回结果
            if "choices" in result and len(result["choices"]) > 0:
                content = result["choices"][0]["message"]["content"].strip()
                # 清理可能的多余字符（如markdown格式）
                content = re.sub(r'^```json|```$', '', content).strip()
                label_result = json.loads(content)

                # 验证返回字段
                risk_type = label_result.get("risk_type", "其他")
                reason_analysis = label_result.get("reason_analysis", "无法分析")[:50]  # 限制长度
                severity = label_result.get("severity", "低")

                # 验证字段值的合法性
                valid_risk_types = ["物流/发货风险", "质量/假货风险", "售后/退款风险", "其他"]
                valid_severities = ["高", "中", "低"]

                if risk_type not in valid_risk_types:
                    risk_type = "其他"
                if severity not in valid_severities:
                    severity = "低"

                return {
                    "risk_type": risk_type,
                    "reason_analysis": reason_analysis,
                    "severity": severity
                }

            else:
                raise ValueError("API返回格式异常，无choices字段")

        except Exception as e:
            error_msg = str(e)
            # 最后一次重试失败才使用关键词匹配
            if retry == DEEPSEEK_CONFIG["retry_times"] - 1:
                st.warning(f"DeepSeek API调用失败（重试{DEEPSEEK_CONFIG['retry_times']}次后）：{error_msg}，使用关键词匹配")
                # 关键词匹配降级方案
                text = review_text.lower()
                risk_type = "其他"

                for rt, keywords in RISK_KEYWORDS.items():
                    if any(kw in text for kw in keywords):
                        risk_type = rt
                        break

                # 根据匹配到的关键词数量判断严重程度
                hit_count = sum(1 for kw_list in RISK_KEYWORDS.values()
                                for kw in kw_list if kw in text)
                severity = "高" if hit_count >= 3 else "中" if hit_count >= 1 else "低"

                return {
                    "risk_type": risk_type,
                    "reason_analysis": f"API调用失败，关键词匹配到{hit_count}个风险词",
                    "severity": severity
                }
            else:
                # 重试前等待
                time.sleep(DEEPSEEK_CONFIG["retry_delay"] * (retry + 1))
                continue

    # 理论上不会执行到这里，作为最后的兜底
    return {"risk_type": "其他", "reason_analysis": "API调用失败", "severity": "中"}


# -------------------------- 缓存打标结果 --------------------------
@st.cache_data(ttl=3600)
def batch_label_reviews(reviews: List[str]) -> List[Dict[str, Any]]:
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    # 去重处理，避免重复调用API
    unique_reviews = list({review: idx for idx, review in enumerate(reviews)}.keys())
    unique_results = {}

    for i, review in enumerate(unique_reviews):
        progress = (i + 1) / len(reviews)
        progress_bar.progress(progress)
        status_text.text(f"正在分析第 {i + 1}/{len(reviews)} 条差评...")

        result = get_deepseek_label(review)
        unique_results[review] = result

        # 增加API调用间隔，避免频率限制
        time.sleep(0.2)

    # 还原原始顺序
    results = [unique_results[review] for review in reviews]

    progress_bar.empty()
    status_text.empty()
    return results


# -------------------------- 数据处理函数（已修复近7天差评计算） --------------------------
def get_store_data():
    try:
        df = pd.read_excel(INPUT_FILE_PATH)

        # 日期解析
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
        cutoff_28d = now - timedelta(days=28)  # 近28天起点
        cutoff_7d = now - timedelta(days=7)  # 近7天起点
        df_28d = df[df['published_datetime'] >= cutoff_28d].copy()

        # 生成28天内的4个周期
        def get_periods():
            periods = []
            for i in range(4):
                end = now - timedelta(days=i * 7)
                start = end - timedelta(days=6)
                if start < cutoff_28d:
                    start = cutoff_28d
                periods.append({
                    'name': f"第{i + 1}周",
                    'start': start,
                    'end': end,
                    'num': i + 1,
                    'time_range': f"{start.strftime('%m-%d')} ~ {end.strftime('%m-%d')}"
                })
            return periods[::-1]  # 第1周最早，第4周最新

        periods = get_periods()
        store_metrics = []
        period_details = []

        for store in df['store_name'].unique():
            df_store = df[df['store_name'] == store].copy()
            df_store_28d = df_28d[df_28d['store_name'] == store].copy()

            # 基础统计（全量+28天）
            total_reviews = len(df_store)
            reviews_28d = len(df_store_28d)

            # 独立计算【近7天差评数】（严格按最近7天）
            df_store_7d = df_store_28d[df_store_28d['published_datetime'] >= cutoff_7d].copy()
            ot_7d = len(df_store_7d[df_store_7d['star_rating'] <= 2])

            period_data = {}
            total_28d_ot = 0

            # 周期统计（只在28天内筛选）
            for p in periods:
                mask = (df_store_28d['published_datetime'] >= p['start']) & (
                        df_store_28d['published_datetime'] <= p['end'])
                df_p = df_store_28d[mask].copy()
                total = len(df_p)
                avg_rating = round(df_p['star_rating'].mean(), 2) if total else 0
                ot = len(df_p[df_p['star_rating'] <= 2])
                one_star = len(df_p[df_p['star_rating'] == 1])
                one_star_ratio = round(one_star / total * 100, 2) if total else 0

                # 关键词命中（累加）
                risk_hits = {}
                for risk_type, keywords in RISK_KEYWORDS.items():
                    if total > 0:
                        txt_series = df_p[df_p['star_rating'] <= 2]['review_content'].fillna('').str.lower()
                        hits = sum(1 for txt in txt_series if any(kw in txt for kw in keywords))
                        risk_hits[risk_type] = hits
                    else:
                        risk_hits[risk_type] = 0

                period_data[p['num']] = {
                    'period': p['name'], 'time_range': p['time_range'],
                    'total': total, 'avg_rating': avg_rating, 'ot': ot,
                    'one_star': one_star, 'one_star_ratio': one_star_ratio,
                    'risk_hits': risk_hits, 'start': p['start'], 'end': p['end']
                }
                total_28d_ot += ot
                period_details.append({
                    'store': store, 'period': p['name'], 'time_range': p['time_range'],
                    'total_reviews': total, 'avg_rating': avg_rating, 'ot_count': ot,
                    'one_star_ratio': one_star_ratio
                })

            # 环比（第4周 vs 第3周）
            ot_growth = 0
            if 3 in period_data and period_data[3]['ot'] > 0:
                ot_growth = (period_data[4]['ot'] - period_data[3]['ot']) / period_data[3]['ot']

            # 风险判定（基于准确的ot_7d和total_28d_ot）
            total_ot = total_28d_ot
            four_period_over_3 = total_ot > 11
            two_period_rise = False
            if 2 in period_data and 3 in period_data and 4 in period_data:
                two_period_rise = (period_data[3]['ot'] > period_data[2]['ot']) and (
                        period_data[4]['ot'] > period_data[3]['ot'])

            logi_fake = sum(
                [period_data[p]['risk_hits']['物流/发货风险'] + period_data[p]['risk_hits']['质量/假货风险'] for p in
                 period_data])
            keyword_outbreak = logi_fake >= RISK_THRESHOLDS["关键词爆发阈值"]

            # 风险等级
            risk_level = "safe"
            risk_reason = ""
            if ot_7d >= 5 or four_period_over_3 or (
                    two_period_rise and (period_data[3]['ot'] + period_data[4]['ot'] > 5)):
                risk_level = "high"
                reasons = []
                if ot_7d >= 5: reasons.append("近7天差评≥5条")
                if four_period_over_3: reasons.append("28天差评>11条")
                if two_period_rise: reasons.append("连续2周差评上升")
                if ot_growth >= 0.3: reasons.append(f"差评环比+{ot_growth * 100:.0f}%")
                if keyword_outbreak: reasons.append("物流/假货关键词爆发")
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

            action = \
                {"high": "立即核查物流/发货/退款，暂停放量", "medium": "加强监控，每日观察", "safe": "正常维护，每周巡检"}[
                    risk_level]

            # 28天平均星级和1星占比
            avg_28d = round(df_store_28d['star_rating'].mean(), 2) if reviews_28d else 0
            one_star_total = len(df_store_28d[df_store_28d['star_rating'] == 1])
            one_star_ratio_28d = round(one_star_total / reviews_28d * 100, 2) if reviews_28d else 0

            store_metrics.append({
                "store_name": store,
                "total_reviews": total_reviews,
                "reviews_28d": reviews_28d,
                "avg_rating_28d": avg_28d,
                "one_star_ratio_28d": one_star_ratio_28d,
                "ot_28d": total_28d_ot,
                "ot_7d": ot_7d,  # 准确的近7天差评数
                "ot_growth": ot_growth,
                "risk_level": risk_level,
                "risk_reason": risk_reason,
                "suggest_action": action,
                "period_data": period_data
            })

        df_stores = pd.DataFrame(store_metrics)
        df_periods = pd.DataFrame(period_details)
        return df_stores, df_periods, df

    except Exception as e:
        st.error(f"数据加载失败：{str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# -------------------------- 看板UI设计 --------------------------
def main():
    st.title("📊 Trustpilot 店铺客诉风险监控看板")
    st.caption(f"更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 添加风险规则按钮和展示区域
    col_title, col_button = st.columns([8, 2])
    with col_button:
        # 添加风险规则查看按钮
        if st.button("📋 查看风险判定规则", use_container_width=True):
            st.session_state.show_rules = not st.session_state.get('show_rules', False)

    # 显示/隐藏风险规则
    if st.session_state.get('show_rules', False):
        with st.expander("风险判定规则详情", expanded=True):
            show_risk_rules()
        st.divider()

    df_stores, df_periods, df_reviews = get_store_data()
    if df_stores.empty:
        st.warning("暂无数据，请先运行爬取脚本生成评论数据")
        return

    # 顶部指标卡片
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric(label="监控店铺总数", value=len(df_stores))
    with col2:
        high_risk = len(df_stores[df_stores['risk_level'] == 'high'])
        st.metric(label="高危店铺数", value=high_risk, delta=f"{high_risk / len(df_stores) * 100:.0f}%",
                  delta_color="inverse")
    with col3:
        medium_risk = len(df_stores[df_stores['risk_level'] == 'medium'])
        st.metric(label="关注店铺数", value=medium_risk, delta=f"{medium_risk / len(df_stores) * 100:.0f}%",
                  delta_color="off")
    with col4:
        safe_risk = len(df_stores[df_stores['risk_level'] == 'safe'])
        st.metric(label="安全店铺数", value=safe_risk, delta=f"{safe_risk / len(df_stores) * 100:.0f}%",
                  delta_color="normal")
    with col5:
        st.metric(label="近28天总评论", value=df_stores['reviews_28d'].sum())
    with col6:
        st.metric(label="近7天差评总数", value=df_stores['ot_7d'].sum())  # 准确汇总

    st.divider()

    # 风险分布饼图 + 趋势图
    col1, col2 = st.columns([1, 2])
    with col1:
        risk_counts = df_stores['risk_level'].value_counts()
        risk_labels = {'high': '高危', 'medium': '关注', 'safe': '安全'}
        fig_pie = px.pie(values=risk_counts.values, names=[risk_labels.get(x, x) for x in risk_counts.index],
                         title="店铺风险分布",
                         color_discrete_map={'高危': COLORS['high'], '关注': COLORS['medium'], '安全': COLORS['safe']})
        fig_pie.update_layout(height=300)
        st.plotly_chart(fig_pie, width='stretch')
    with col2:
        high_stores = df_stores[df_stores['risk_level'] == 'high']['store_name'].head(5).tolist()
        trend_data = []
        for store in high_stores:
            store_periods = df_periods[df_periods['store'] == store]
            for _, row in store_periods.iterrows():
                trend_data.append({'店铺': store, '周期': row['period'], '差评数': row['ot_count']})
        if trend_data:
            df_trend = pd.DataFrame(trend_data)
            fig_trend = px.line(df_trend, x='周期', y='差评数', color='店铺', title='高危店铺TOP5 - 差评趋势',
                                markers=True)
            fig_trend.update_layout(height=300)
            st.plotly_chart(fig_trend, width='stretch')
        else:
            st.info("暂无高危店铺趋势数据")

    st.divider()

    # 店铺风险总览列表（ot_7d准确）
    st.subheader("店铺风险总览列表")
    df_display = df_stores.copy()
    df_display['风险等级'] = df_display['risk_level'].map({'high': '🔴 高危', 'medium': '🟡 关注', 'safe': '🟢 安全'})
    df_display['差评环比'] = df_display['ot_growth'].apply(lambda x: f"{x * 100:.1f}%")
    df_display = df_display[[
        'store_name', '风险等级', 'total_reviews', 'reviews_28d', 'avg_rating_28d', 'one_star_ratio_28d',
        'ot_7d', '差评环比', 'risk_reason', 'suggest_action'
    ]]
    df_display.columns = ['店铺域名', '风险等级', '总评论数', '近28天评论', '平均星级', '1星占比(%)',
                          '近7天差评', '差评环比', '风险原因', '建议动作']
    st.dataframe(df_display, width='stretch', hide_index=True)

    st.divider()

    # 单店铺详情分析
    st.subheader("🔍 单店铺详情分析")
    selected_store = st.selectbox("选择店铺查看详情", df_stores['store_name'].tolist(), index=0)
    if selected_store:
        store_data = df_stores[df_stores['store_name'] == selected_store].iloc[0]
        df_store_reviews = df_reviews[df_reviews['store_name'] == selected_store].copy()

        risk_icon = {'high': '🔴', 'medium': '🟡', 'safe': '🟢'}[store_data['risk_level']]
        risk_text = {'high': '高危', 'medium': '关注', 'safe': '安全'}[store_data['risk_level']]
        st.markdown(f"### {selected_store} | {risk_icon} {risk_text}")

        # 核心指标卡片
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"""
            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; text-align: center;">
                <h4 style="margin: 0; color: #333;">店铺总评分</h4>
                <p style="font-size: 24px; font-weight: bold; color: #D82C20; margin: 10px 0 0 0;">{store_data['avg_rating_28d']}/5.0</p>
                <p style="color: #666; margin: 5px 0 0 0;">1星占比: {store_data['one_star_ratio_28d']}%</p>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; text-align: center;">
                <h4 style="margin: 0; color: #333;">近28天概览</h4>
                <p style="font-size: 18px; color: #333; margin: 10px 0 0 0;">
                    总评论: {store_data['reviews_28d']}<br>
                    1+2星差评: {store_data['ot_28d']}<br>
                    日均差评: {store_data['ot_28d'] / 28:.1f}
                </p>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            growth_text = "上升" if store_data['ot_growth'] > 0 else "下降" if store_data['ot_growth'] < 0 else "持平"
            st.markdown(f"""
            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; text-align: center;">
                <h4 style="margin: 0; color: #333;">近7天爆发</h4>
                <p style="font-size: 24px; font-weight: bold; color: #D82C20; margin: 10px 0 0 0;">{store_data['ot_7d']}</p>
                <p style="color: #666; margin: 5px 0 0 0;">环比: {store_data['ot_growth'] * 100:.1f}% ({growth_text})</p>
            </div>
            """, unsafe_allow_html=True)
        with col4:
            risk_hits = {}
            for risk_type in RISK_KEYWORDS.keys():
                risk_hits[risk_type] = sum([p['risk_hits'][risk_type] for p in store_data['period_data'].values()])
            risk_tags = []
            for risk_type, hits in risk_hits.items():
                if hits > 0:
                    risk_tags.append(
                        f"<span style='background-color: #ffecec; color: #D82C20; padding: 4px 8px; border-radius: 4px; font-size: 12px;'>{risk_type}</span>")
            tags_html = "".join(risk_tags) if risk_tags else "<span style='color: #666; font-size: 12px;'>无明显风险</span>"
            st.markdown(f"""
            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; text-align: center;">
                <h4 style="margin: 0; color: #333;">风险标签</h4>
                <div style="margin: 10px 0 0 0;">{tags_html}</div>
            </div>
            """, unsafe_allow_html=True)

        st.divider()

        # 周期趋势
        col1, col2 = st.columns([1, 1])
        with col1:
            st.subheader("28天周期趋势")
            period_items = sorted(store_data['period_data'].items(), key=lambda x: x[0])
            period_list = []
            for idx, (p_num, p_data) in enumerate(period_items):
                period_list.append({
                    '周期': f"{p_data['period']} ({p_data['time_range']})",
                    '评论数': p_data['total'],
                    '平均星级': p_data['avg_rating'],
                    '1+2星差评': p_data['ot'],
                })
            st.dataframe(pd.DataFrame(period_list), width='stretch', hide_index=True)
        with col2:
            periods = [f"{store_data['period_data'][p]['period']} ({store_data['period_data'][p]['time_range']})" for p
                       in store_data['period_data'].keys()]
            ratings = [store_data['period_data'][p]['avg_rating'] for p in store_data['period_data'].keys()]
            ot_counts = [store_data['period_data'][p]['ot'] for p in store_data['period_data'].keys()]
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=periods, y=ratings, name='平均星级', mode='lines+markers',
                                     line=dict(color=COLORS['safe'], width=2), yaxis='y1'))
            fig.add_trace(go.Scatter(x=periods, y=ot_counts, name='1+2星差评', mode='lines+markers',
                                     line=dict(color=COLORS['high'], width=2), yaxis='y2'))
            fig.update_layout(title='星级 & 差评趋势', yaxis=dict(title='平均星级', side='left', range=[0, 5]),
                              yaxis2=dict(title='差评数', side='right', overlaying='y'), height=300)
            st.plotly_chart(fig, width='stretch')

        st.divider()

        # 风险判定与建议
        col1, col2 = st.columns([1, 2])
        with col1:
            st.markdown(f"""
            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid {COLORS[store_data['risk_level']]};">
                <h4 style="margin: 0 0 10px 0; color: {COLORS[store_data['risk_level']]};">风险判定结果</h4>
                <p style="font-size: 16px; font-weight: bold; margin: 0 0 10px 0;">{risk_icon} {risk_text}</p>
                <p style="color: #666; margin: 0 0 10px 0;"><strong>判定依据：</strong>{store_data['risk_reason']}</p>
                <p style="color: #333; margin: 0;"><strong>建议动作：</strong>{store_data['suggest_action']}</p>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.subheader("最新差评原文（近28天）- AI智能分析")
            cutoff_date = datetime.now() - timedelta(days=28)
            df_bad_reviews = df_store_reviews[
                (df_store_reviews['star_rating'] <= 2) & (df_store_reviews['published_datetime'] >= cutoff_date)].copy()
            df_bad_reviews = df_bad_reviews.sort_values('published_datetime', ascending=False).head(20)
            if not df_bad_reviews.empty:
                reviews_list = df_bad_reviews['review_content'].tolist()
                label_results = batch_label_reviews(reviews_list)
                df_bad_reviews['发布时间'] = df_bad_reviews['published_datetime'].dt.strftime('%Y-%m-%d')
                df_bad_reviews['完整内容'] = df_bad_reviews['review_content']
                df_bad_reviews['AI风险类型'] = [r['risk_type'] for r in label_results]
                df_bad_reviews['AI原因分析'] = [r['reason_analysis'] for r in label_results]
                df_bad_reviews['AI严重程度'] = [r['severity'] for r in label_results]
                st.dataframe(
                    df_bad_reviews[['发布时间', 'star_rating', 'review_title', '完整内容', 'AI风险类型', 'AI原因分析',
                                    'AI严重程度']],
                    width='stretch', hide_index=True,
                    column_config={
                        'star_rating': st.column_config.NumberColumn('星级', format='⭐ %d'),
                        'review_title': st.column_config.TextColumn('标题'),
                        '完整内容': st.column_config.TextColumn('完整内容', width='large'),
                        'AI风险类型': st.column_config.TextColumn('AI风险类型'),
                        'AI原因分析': st.column_config.TextColumn('AI原因分析', width='medium'),
                        'AI严重程度': st.column_config.TextColumn('AI严重程度')
                    }
                )
                severity_counts = df_bad_reviews['AI严重程度'].value_counts()
                if not severity_counts.empty:
                    st.subheader("风险严重程度分布")
                    fig_severity = px.bar(x=severity_counts.index, y=severity_counts.values,
                                          title="AI判定的风险严重程度分布",
                                          color=severity_counts.index,
                                          color_discrete_map={'高': '#D82C20', '中': '#F5A623', '低': '#28A745'})
                    fig_severity.update_layout(height=200)
                    st.plotly_chart(fig_severity, width='stretch')
            else:
                st.info("近28天暂无1-2星差评")


if __name__ == "__main__":
    main()