import streamlit as st
import pandas as pd
import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import plotly.express as px

st.set_page_config(page_title="Dream Boys Analytics", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 0rem; }
    h1 { font-size: 28px; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

st.title("Онлайн-мониторинг системы умных скидок магазина мужской косметики <<Dream Boys>>")

if 'metrics' not in st.session_state:
    st.session_state.metrics = {
        'total_sessions': set(),
        'organic_revenue': 0.0,
        'promo_revenue': 0.0,
        'promos_issued': 0,
        'promo_purchases': 0,
        'history_probs': [],
        'product_revenue': {},
        'brand_revenue': {}
    }

m = st.session_state.metrics

def process_items(items):
    for item in items:
        pid = str(item.get('product_id', 'unknown'))
        brand = item.get('brand', 'unknown')
        price = item.get('price', 0.0)

        m['product_revenue'][pid] = m['product_revenue'].get(pid, 0.0) + price
        if brand != 'unknown':
            m['brand_revenue'][brand] = m['brand_revenue'].get(brand, 0.0) + price

@st.cache_resource
def get_kafka_consumer():
    retries = 5
    while retries > 0:
        try:
            return KafkaConsumer(
                'ml_predictions', 'promo_actions',
                bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092', 'kafka-3:9092'],
                group_id='streamlit_dashboard',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
        except NoBrokersAvailable:
            time.sleep(2)
            retries -= 1
    st.error("Ошибка подключения к брокерам Kafka.")
    st.stop()

consumer = get_kafka_consumer()
try:
    raw_messages = consumer.poll(timeout_ms=400)
except Exception as e:
    st.warning("Kafka is not available. Reconnecting...")
    st.cache_resource.clear()
    time.sleep(3)
    st.rerun()

for tp, messages in raw_messages.items():
    for msg in messages:
        val = msg.value
        session_id = val.get('user_session', 'unknown')

        if msg.topic == 'ml_predictions':
            m['total_sessions'].add(session_id)
            if val.get('status') == 'organic_purchase':
                m['organic_revenue'] += val.get('revenue', 0.0)
                process_items(val.get('cart_items', []))
            elif val.get('status') == 'prediction':
                m['history_probs'].append(val.get('probability', 0.0))

        elif msg.topic == 'promo_actions':
            if val.get('action') == 'discount_issued':
                m['promos_issued'] += 1
            elif val.get('action') == 'simulated_purchase':
                m['promo_purchases'] += 1
                m['promo_revenue'] += val.get('revenue', 0.0)
                process_items(val.get('cart_items', []))

m['history_probs'] = m['history_probs'][-1000:]

total_sessions_count = len(m['total_sessions']) if len(m['total_sessions']) > 0 else 1
total_revenue = m['organic_revenue'] + m['promo_revenue']
promo_conversion = (m['promo_purchases'] / m['promos_issued'] * 100) if m['promos_issued'] > 0 else 0.0
promo_coverage = (m['promos_issued'] / total_sessions_count) * 100

cols = st.columns(6)
cols[0].metric("Всего сессий", f"{total_sessions_count}")
cols[1].metric("Общая выручка", f"${total_revenue:,.0f}")
cols[2].metric("Органическая выручка", f"${m['organic_revenue']:,.0f}")
cols[3].metric("Uplift (Промо)", f"${m['promo_revenue']:,.0f}")
cols[4].metric("Выдано скидок", f"{m['promos_issued']}")
cols[5].metric("Конверсия промо", f"{promo_conversion:.1f}%")

st.markdown("---")

col_chart, col_tables = st.columns([3, 2])

with col_chart:
    st.markdown("##### Распределение вероятностей покупки")
    if m['history_probs']:
        fig = px.histogram(
            x=m['history_probs'],
            nbins=25, range_x=[0, 1],
            color_discrete_sequence=['#3b5b92']
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=20, b=0),
            height=300,
            xaxis_title="Вероятность покупки",
            yaxis_title="Количество сессий"
        )
        fig.add_vrect(x0=0.4, x1=0.7, fillcolor="rgba(255, 165, 0, 0.2)", line_width=0, annotation_text="Зона скидок")
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Ожидание данных от ML-модели...")

with col_tables:
    tab_prod, tab_brand = st.columns(2)

    df_prod = pd.DataFrame(list(m['product_revenue'].items()), columns=['ID Товара', 'Выручка ($)'])
    df_prod = df_prod.sort_values(by='Выручка ($)', ascending=False).head(10)

    df_brand = pd.DataFrame(list(m['brand_revenue'].items()), columns=['Бренд', 'Выручка ($)'])
    df_brand = df_brand.sort_values(by='Выручка ($)', ascending=False).head(10)

    with tab_prod:
        st.markdown("##### Топ-10 товаров")
        st.dataframe(df_prod, hide_index=True, width='stretch', height=400)

    with tab_brand:
        st.markdown("##### Топ-10 брендов")
        st.dataframe(df_brand, hide_index=True, width='stretch', height=400)

time.sleep(1)
st.rerun()
