import pandas as pd
import streamlit as st
import home
import data_analysis
import WeatherMap
import analytics

with st.sidebar:
    st.title("Введите необходимые данные")

    uploaded_file = st.file_uploader("Загрузите файл с историческими данными (CSV)", type="csv")

    api_key = st.text_input("Введите API-ключ OpenWeatherMap:", type="password")

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        cities = df["city"].unique()
        selected_city = st.selectbox("Выберите город", cities)
    else:
        selected_city = None


if uploaded_file is not None:
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    st.session_state.df = df

if api_key:
    st.session_state.api_key = api_key

if selected_city:
    st.session_state.selected_city = selected_city


tab1, tab2, tab3, tab4= st.tabs(["Главная", "Анализ данных", "Текущая погода", "Аналитика"])

with tab1:
    home.app()
with tab2:
    data_analysis.app()
with tab3:
    WeatherMap.app()
with tab4:
    analytics.app()



