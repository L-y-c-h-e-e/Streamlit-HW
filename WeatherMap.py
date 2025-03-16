import streamlit as st

from utils import get_season, anomaly_time, anomalies_time, load_data, check_api_key


def app():
    st.title("Текущая погода")
    st.info("На этой странице мы получим текущую температуру в разных городах с помощью API-ключа и проведем с ней некоторые вычисления.\
            \nДля каждого города с помощью исторических данных определим, является ли текущая температура нормальной для сезона.")
    st.subheader("Загрузите необходимые данные:")
    df = load_data()
    if "api_key" in st.session_state:
        api_key = st.session_state.api_key
        is_valid, error_message = check_api_key(api_key)
        if is_valid:
            st.success("API-ключ успешно загружен!")
            season = get_season()
            cities = df["city"].unique()
            city_name = st.text_input("Введите город:", value="Moscow")

            st.subheader("Сравним синхронный и асинхронный метод для одного города:")
            anomaly_time(city_name, api_key)
            st.info("Как мы видим разницы для одного запроса практически нет.")

            st.subheader("Сравним синхронный и асинхронный метод для 14 городов сразу:")
            anomalies_time(df, api_key, season)
            st.info("В таком случае, даже для 14 городов асинхронные методы работают быстрее в 5-7 раз,\
                    а чем больше запросов, тем больше будет выгода по времени.\
                    Так что уверенно можно сказать, что стоит использовать асинхронные методы.")
        else:
            st.warning(f"Ошибка: {error_message}")
    else:
        st.error("Введите, пожалуйста, API-ключ OpenWeatherMap")