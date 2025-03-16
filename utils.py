import dask.dataframe as dd
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime
import aiohttp
import asyncio
import streamlit as st



#Функция для анализа данных и добавления новых данных в DataFrame
@st.cache_data
def data_analysis(df):

    #Вычисляемм скользящее среднее и std(для дальнейших вычислений)
    df["moving_avg"] = (df.groupby("city")["temperature"]
        .rolling(window=30, min_periods=1, center=True)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["std"] = df.groupby("city")["temperature"].transform("std")

    #Вычисляем среднее и стандартное отклонение по сезонам
    df["avg_by_season"] = df.groupby(["city","season"])["temperature"].transform("mean")
    df["std_by_season"] = df.groupby(["city","season"])["temperature"].transform("std")

    #Выявляем аномалии, где температура выходит за пределы среднего на ±2𝜎
    df["lower_bound"] = df["moving_avg"] - 2 * df["std"]
    df["upper_bound"] = df["moving_avg"] + 2 * df["std"]
    df["is_anomaly"] = (df["temperature"] < df["lower_bound"]) | (df["temperature"] > df["upper_bound"])
    df["is_anomaly"] = df["is_anomaly"].astype(int)

    return df


#Функция для замера времени без/c распараллеливанием и сравнения их
def data_analysis_time(df):
    # Замер времени выполнения без распараллеливания
    start_time = time.time()
    df_result = data_analysis(df)
    end_time = time.time()
    single_time = end_time - start_time
    st.write(f"Время выполнения без распараллеливания: {single_time:.2f} секунд")

    ddf = dd.from_pandas(df, npartitions=10)

    # Замер времени выполнения с распараллеливанием
    start_time = time.time()
    df_result_dask = ddf.map_partitions(data_analysis).compute()
    end_time = time.time()
    parallel_time = end_time - start_time
    st.write(f"Время выполнения с распараллеливанием: {parallel_time:.2f} секунд")
    st.write(f"Разница во времени: {single_time - parallel_time:.2f} секунд\n")



#Функция для создания больших данных
@st.cache_data
def create_test_df(size):
    cities = [
        "New York", "London", "Paris", "Tokyo", "Moscow", "Sydney", "Berlin", "Beijing", "Dubai", "Rio de Janeiro",
        "Los Angeles", "Chicago", "Toronto", "Singapore", "Hong Kong", "Bangkok", "Istanbul", "Rome", "Madrid", "Amsterdam",
        "Seoul", "Shanghai", "Mumbai", "Cairo", "Mexico City", "São Paulo", "Buenos Aires", "Cape Town", "Stockholm", "Vienna"
    ]
    data = {
        "city": np.random.choice(cities, size=size),
        "temperature": np.random.normal(loc=10, scale=5, size=size),
        "season": np.random.choice(["winter", "spring", "summer", "autumn"], size=size)
    }
    return pd.DataFrame(data)


# Функция для проверки API-ключа
@st.cache_data
def check_api_key(api_key):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": "London",
        "appid": api_key,
        "units": "metric"
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return True, None
    else:
        error_data = response.json()
        return False, error_data.get("message", "Неизвестная ошибка")


#Функция для определения нормальности температуры в городе
@st.cache_data
def is_normal(city_name, temp, df):
    season = get_season()

    historical_df = df[(df["city"] == city_name) & (df["season"] == season)]

    avg_temp = historical_df["temperature"].mean()
    std_temp = historical_df["temperature"].std()

    min_temp = avg_temp - 2 * std_temp
    max_temp = avg_temp + 2 * std_temp

    if min_temp <= temp <= max_temp:
        return True, round(avg_temp,2), round(std_temp,2)
    else:
        return False, round(avg_temp,2), round(std_temp,2)


#Функция для получения текущего сезона
@st.cache_data(ttl=86400)
def get_season():
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "autumn"


#Синхронная функция получения текущей температуры в городе
def get_temperature(city_name, api_key):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city_name,
        "appid": api_key,
        "units": "metric"
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data["main"]["temp"]
    else:
        st.write(f"Ошибка {response.status_code} для города {city_name}")
        return None


#Асинхронная функция получения текущей температуры в городе
async def async_get_temperature(city_name, api_key):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city_name,
        "appid": api_key,
        "units": "metric"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data["main"]["temp"]
            else:
                st.write(f"Ошибка {response.status} для города {city_name}")
                return None


#Асинхронная функция получения текущей температуры в списке городов одновременно
async def async_get_temperatures(city_names, api_key):
    tasks = [async_get_temperature(city, api_key) for city in city_names]
    temperatures = await asyncio.gather(*tasks)
    return temperatures


#Синхронная функция поиска аномалий в списке городов
def sync_anomalies(df, api_key, season):
    results = []
    for city in df["city"].unique():
        temp = get_temperature(city, api_key)
        normal, avg_temp, std_temp = is_normal(city, temp, df)

        results.append({
            "city": city,
            "is_normal": int(normal),
            "current_temp": temp,
            "avg_temp": avg_temp,
            "std_temp": std_temp,
            "season": season
        })

    return pd.DataFrame(results)


#Асинхронная функция поиска аномалий в списке городов
async def async_anomalies(df, api_key, season):
    cities = df["city"].unique()
    temperatures = await async_get_temperatures(cities, api_key)
    results = []
    for city, temp in zip(cities, temperatures):
        normal, avg_temp, std_temp = is_normal(city, temp, df)

        results.append({
            "city": city,
            "is_normal": int(normal),
            "current_temp": temp,
            "avg_temp": avg_temp,
            "std_temp": std_temp,
            "season": season
        })

    return pd.DataFrame(results)


def anomaly_time(city_name, api_key):
    #Синхронный метод для одного города
    start_time = time.time()
    sync_temp = get_temperature(city_name, api_key)
    sync_time = time.time() - start_time
    st.subheader(f"Текущая температура в {city_name}: {sync_temp}°C")
    st.write(f"Синхронный метод для одного города: {sync_time:.2f} секунд")

    #Асинхронный метод для одного города
    start_time = time.time()
    async_temp = asyncio.run(async_get_temperature(city_name, api_key))
    async_time = time.time() - start_time
    st.write(f"Асинхронный метод для одного города: {async_time:.2f} секунд\n")


def anomalies_time(df, api_key, season):
    #Синхронный метод для всех городов
    start_time = time.time()
    sync_results = sync_anomalies(df, api_key, season)
    sync_time = time.time() - start_time
    st.write(f"Синхронный метод для всех городов: {sync_time:.2f} секунд")

    #Асинхронный метод для всех городов
    start_time = time.time()
    async_results = asyncio.run(async_anomalies(df, api_key, season))
    async_time = time.time() - start_time
    st.write(f"Асинхронный метод для всех городов: {async_time:.2f} секунд")
    st.write(f"*Асинхронный метод быстрее в {sync_time / async_time:.2f} раз.*")

    #Вывод таблиц
    st.subheader("Синхронные результаты:")
    st.write(sync_results.head(5))

    st.subheader("Асинхронные результаты:")
    st.write(async_results.head(5))


#Кэширование данных
@st.cache_data
def load_data():
    data = pd.read_csv("temperature_data.csv")
    return data