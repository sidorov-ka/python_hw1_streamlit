import aiohttp
import asyncio
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import pydeck as pdk
from datetime import datetime

# Функция для получения текущего сезона
def get_current_season():
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "autumn"

# Асинхронная функция для получения температуры
async def fetch_temperature(session, city, owm_api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&APPID={owm_api_key}"
    async with session.get(url) as response:
        data = await response.json()
        if 'main' in data and 'temp' in data['main']:
            return city, data['main']['temp'], data['coord']['lat'], data['coord']['lon']
        else:
            return city, None, None, None

async def main_async(cities, owm_api_key):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_temperature(session, city, owm_api_key) for city in cities]
        results = await asyncio.gather(*tasks)
    return {result[0]: (result[1], result[2], result[3]) for result in results}

def analyze_city(city_data):
    city_data['rolling_mean'] = city_data['temperature'].rolling(window=30, min_periods=1).mean()
    city_data['rolling_std'] = city_data['temperature'].rolling(window=30, min_periods=2).std().bfill()
    seasonal_stats = city_data.groupby('season').agg({'temperature': ['mean', 'std']}).reset_index()
    seasonal_stats.columns = ['season', 'seasonal_mean', 'seasonal_std']
    city_data = city_data.merge(seasonal_stats, on='season')
    city_data['rolling_anomaly'] = np.abs(city_data['temperature'] - city_data['rolling_mean']) > 2 * city_data['rolling_std']
    city_data['seasonal_anomaly'] = np.abs(city_data['temperature'] - city_data['seasonal_mean']) > 2 * city_data['seasonal_std']
    return city_data

def plot_city_analysis(city_data, city):
    rolling_fig = go.Figure()
    rolling_fig.add_trace(go.Scatter(x=city_data['timestamp'], y=city_data['temperature'], mode='lines', name='Температура'))
    rolling_fig.add_trace(go.Scatter(x=city_data['timestamp'], y=city_data['rolling_mean'], mode='lines', name='Скользящее среднее', line=dict(color='orange')))
    rolling_fig.add_trace(go.Scatter(x=city_data[city_data['rolling_anomaly']]['timestamp'], y=city_data[city_data['rolling_anomaly']]['temperature'], mode='markers', name='Аномалии (скользящие)', marker=dict(color='red')))
    rolling_fig.update_layout(title=f'Анализ температуры для {city} (скользящее среднее)', xaxis_title='Дата', yaxis_title='Температура (°C)', legend=dict(yanchor="bottom", y=-0.5, xanchor="center", x=0.5))

    seasonal_fig = go.Figure()
    seasonal_fig.add_trace(go.Scatter(x=city_data['timestamp'], y=city_data['temperature'], mode='lines', name='Температура'))
    seasonal_fig.add_trace(go.Scatter(x=city_data['timestamp'], y=city_data['seasonal_mean'], mode='lines', name='Сезонное среднее', line=dict(color='green')))
    seasonal_fig.add_trace(go.Scatter(x=city_data[city_data['seasonal_anomaly']]['timestamp'], y=city_data[city_data['seasonal_anomaly']]['temperature'], mode='markers', name='Аномалии (сезонные)', marker=dict(color='purple')))
    seasonal_fig.update_layout(title=f'Анализ температуры для {city} (сезонное среднее)', xaxis_title='Дата', yaxis_title='Температура (°C)', legend=dict(yanchor="bottom", y=-0.5, xanchor="center", x=0.5))

    return rolling_fig, seasonal_fig

# Интерфейс Streamlit
st.title('Анализ температуры города')

# Ввод API ключа
owm_api_key = st.text_input("Введите API ключ OpenWeatherMap", type="password")

# Загрузка файла с историческими данными
uploaded_file = st.file_uploader("Загрузите файл с историческими данными", type=["csv"])
if uploaded_file:
    historical_data = pd.read_csv(uploaded_file, parse_dates=['timestamp'])

    # Выбор города
    cities = historical_data['city'].unique()
    selected_city = st.selectbox("Выберите город", cities)

    # Кнопка для отображения визуализации
    if st.button("Показать визуализацию"):
        if owm_api_key and selected_city:
            historical_city_data = historical_data[historical_data['city'] == selected_city].copy()
            analyzed_data = analyze_city(historical_city_data)

            # Отображение визуализаций
            rolling_fig, seasonal_fig = plot_city_analysis(analyzed_data, selected_city)
            st.plotly_chart(rolling_fig)
            st.plotly_chart(seasonal_fig)

            # Получение текущей температуры
            results = asyncio.run(main_async([selected_city], owm_api_key))
            current_temperature, lat, lon = results[selected_city]

            if current_temperature is not None:
                st.write(f"Текущая температура в {selected_city}: {current_temperature}°C")

                # Определение, аномальна ли температура
                current_season = get_current_season()
                city_data = historical_data[historical_data['city'] == selected_city]
                season_data = city_data[city_data['season'] == current_season]

                if not season_data.empty:
                    mean_temp = season_data['temperature'].mean()
                    std_temp = season_data['temperature'].std()

                    if mean_temp - 2 * std_temp <= current_temperature <= mean_temp + 2 * std_temp:
                        st.write(f"Температура в {selected_city} является нормальной для сезона.")
                    else:
                        st.write(f"Температура в {selected_city} является аномальной для сезона.")
                else:
                    st.write(f"Нет данных для сезона {current_season} в {selected_city}.")

                # Отображение 3D карты города
                st.pydeck_chart(pdk.Deck(
                    map_style='mapbox://styles/mapbox/light-v9',
                    initial_view_state=pdk.ViewState(
                        latitude=lat,
                        longitude=lon,
                        zoom=12,
                        pitch=50,
                    ),
                ))
            else:
                st.warning(f"Не удалось получить данные о температуре для {selected_city}.")
        else:
            st.warning("Пожалуйста, введите корректный API ключ и выберите город.")