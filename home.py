import streamlit as st

from utils import check_api_key


def app():
    st.title("Главная")
    if "df" in st.session_state:
        st.success("Файл успешно загружен!")
    else:
        st.error("Загрузите, пожалуйста, файл в боковой панели.")
    if "api_key" in st.session_state:
        api_key = st.session_state.api_key
        is_valid, error_message = check_api_key(api_key)
        if is_valid:
            st.success("API-ключ успешно загружен!")
        else:
            st.warning(f"Ошибка: {error_message}")
    else:
        st.error("Введите, пожалуйста, API-ключ OpenWeatherMap")

