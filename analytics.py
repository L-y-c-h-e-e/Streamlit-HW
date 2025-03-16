import dask.dataframe as dd
import streamlit as st
import plotly.express as px

from utils import get_season, data_analysis, get_temperature, is_normal, check_api_key


def app():
    st.title("Аналитика данных")
    st.info("На этой странице вы можете провести анализ своих данных и посмотреть на их визуализацию.")
    st.subheader("Загрузите необходимые данные:")
    is_valid = False
    if "df" not in st.session_state:
        st.error("Загрузите, пожалуйста, файл в боковой панели.")
    else:
        st.success("Файл успешно загружен!")
    if "api_key" not in st.session_state:
        st.error("Введите, пожалуйста, API-ключ OpenWeatherMap")
    else:
        api_key = st.session_state.api_key
        is_valid, error_message = check_api_key(api_key)
        if is_valid:
            st.success("API-ключ успешно загружен!")
        else:
            st.warning(f"Ошибка: {error_message}")


    if "df" in st.session_state and is_valid:
        df1 = st.session_state.df
        selected_city = st.session_state.selected_city
        ddf = dd.from_pandas(df1, npartitions=10)
        df = ddf.map_partitions(data_analysis).compute()
        city_data = df[df["city"] == selected_city]

        st.write("### Выберите данные, которые необходимо вывести:")
        selected_data = st.multiselect("Данные:", [
            "Вывести первые строки из DataFrame",
            "Вывести случайные строки из DataFrame",
            "Вычислить скользящее среднее, std/mean для сезонов и выявить аномалии",
            "Вывести описательную статистику для выбранного города",
        ])
        if "Вывести первые строки из DataFrame" in selected_data:
            st.write(f"### Первые строки загруженных данных:")
            num_fst = st.number_input("Сколько первых строк необходимо вывести?", value=3, step=1)
            st.write(df1.head(num_fst))

        if "Вывести случайные строки из DataFrame" in selected_data:
            st.write(f"### Случайные строки загруженных данных:")
            num_smp = st.number_input("Сколько случайных строк необходимо вывести?", value=3, step=1)
            st.write(df1.sample(num_smp))

        if "Вычислить скользящее среднее, std/mean для сезонов и выявить аномалии" in selected_data:
            st.write(f"### Измененные данные после проведения вычислений:")
            num_anl = st.number_input("Сколько строк необходимо вывести?", value=3, step=1)
            st.write(df.head(num_anl))

        if "Вывести описательную статистику для выбранного города" in selected_data:
            st.write(f"### Описательная статистика для {selected_city}:")
            st.write(city_data["temperature"].describe())




        st.write(f"### Выберите значения, которые необходимо вывести для {selected_city}:")
        selected_value = st.multiselect("Значения:", [
            "Количество аномалий",
            "Среднегодовая температура",
            "Максимальная температура",
            "Является ли текущая температура нормальной для сезона"
        ])
        if f"Количество аномалий" in selected_value:
            num_anomalies = city_data["is_anomaly"].sum()
            st.write(f"Количество аномалий в {selected_city}: {num_anomalies}")

        if f"Среднегодовая температура" in selected_value:
            avg_temp = city_data["temperature"].mean()
            st.write(f"Среднегодовая температура в {selected_city}: *{avg_temp:.2f}°C*")

        if f"Является ли текущая температура нормальной для сезона" in selected_value:
            current_temp = get_temperature(selected_city, st.session_state.api_key)
            if current_temp is not None:
                normal, avg_temp, std_temp = is_normal(selected_city, current_temp, df)
                st.write(f"Текущая температура в {selected_city}: *{current_temp}°C*")
                if normal == 1:
                    st.write(f"Текущая температура нормальна для {selected_city} в {get_season()}")
                else:
                    st.write(f"Текущая температура не нормальна для {selected_city} в {get_season()}")

        if f"Максимальная температура" in selected_value:
            max_temp = city_data["temperature"].max()
            st.write(f"Максимальная температура в {selected_city}: *{round(max_temp,2)}°C*")



        st.write("### Выберите графики, которые необходимо вывести:")
        selected_plots = st.multiselect("Графики:", [
            "Временной ряд температур",
            "Сезонные профили",
        ])
        if f"Временной ряд температур" in selected_plots:
            st.write(f"### Временной ряд температур для {selected_city}:")
            fig = px.line(city_data, x="timestamp", y="temperature",
                          title=f"Температура в городе {selected_city}")

            anomalies = city_data[city_data["is_anomaly"] == 1]
            fig.add_scatter(x=anomalies["timestamp"], y=anomalies["temperature"],
                            mode="markers", name="Аномалия", marker=dict(color="red"))

            fig.update_traces(name="Температура", selector=dict(type="scatter", mode="lines"))

            st.plotly_chart(fig)

        if f"Сезонные профили" in selected_plots:
            st.write(f"### Сезонные профили с указанием std для {selected_city}:")
            seasonal_data = city_data.groupby("season")["temperature"].agg(["mean", "std"]).reset_index()

            fig2 = px.bar(seasonal_data, x="season", y="mean", error_y="std",
                          title=f"Сезонные профили температуры в городе {selected_city}",
                          labels={"mean": "Средняя температура (°C)", "season": "Сезон"})
            st.plotly_chart(fig2)
