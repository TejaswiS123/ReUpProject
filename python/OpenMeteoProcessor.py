import requests
import sqlite3
import pandas as pd
import urllib3
import json



def create_weather_table(db_name):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_weather (
                forecast_date DATE,
                temperature_2m_max TEXT,
                precipitation_sum TEXT
            )
        ''')
    connection.commit()
    return connection

def fetch_daily_weather_info(url):
    response = requests.get(url)
    if response.status_code == 200:
        daily_weather = pd.DataFrame(response.json()['daily'])
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")

    return daily_weather

def insert_into_daily_weather(df, connection):
    df.to_sql("daily_weather", connection, if_exists="replace", index=False)

def analyze_daily_weather(daily_weather_df):
    #Get daily max temperature and precipiation sum
    max_daily_temperature_in_forecast = daily_weather_df['temperature_2m_max'].max()
    total_precipitation_sum_in_forecast = daily_weather_df['precipitation_sum'].sum()

    max_precipitation = daily_weather_df['precipitation_sum'].max()
    precip_dates = daily_weather_df[daily_weather_df['precipitation_sum'] == max_precipitation]['time'].values

    print(f"Maximum daily temperature in forecast: {max_daily_temperature_in_forecast}°C ")
    print(f"Total daily precipiation in forecast: {total_precipitation_sum_in_forecast} mm")
    print(f"Highest Precipitation: {max_precipitation} mm")
    print(f"Dates with the Highest Precipitation: {', '.join(map(str, precip_dates))}")

def main():
    print("Setting up the database...")
    print("Creating universities table...")
    conn = create_weather_table("reup.db")

    latitude = 52.52
    longitude = 13.4050
    daily_parameters = "temperature_2m_max,precipitation_sum"
    forecast_days = 16

    #Using the daily open meteo api with parameters temperature_2m_max,precipitation_sum
    data_frame = fetch_daily_weather_info(f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily={daily_parameters}&forecast_days={forecast_days}")
    #inserting into sqlite3 database
    insert_into_daily_weather(data_frame, conn)
    # Analyzing and printing the report
    analyze_daily_weather(data_frame)

    conn.close()


if __name__ == "__main__":
    main()