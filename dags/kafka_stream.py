from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'airflow',
    'start_date': datetime(2024, 1, 1, 10, 30)
}

def get_data(apikey, q, days):
    import requests

    url = 'http://api.weatherapi.com/v1/forecast.json'
    payload = {'key': apikey, 'q': q, 'days': days}
    req = requests.get(url, params=payload)

    return req.json()

def format_data(res):
    data = {}

    location = res['location']
    current = res['current']
    forecast = res['forecast']['forecastday']

    data["name"] = location["name"]
    data["region"] = location["region"]
    data["country"] = location["country"]
    data["lat"] = location["lat"]
    data["lon"] = location["lon"]
    data["time"] = current["last_updated"]
    data["temp_degree_celcius"] = current["temp_c"]
    data["weather"] = current["condition"]["text"]
    data["weather_icon"] = current["condition"]["icon"]
    data["wind_kph"] = current["wind_kph"]
    data["precip_mm"] = current["precip_mm"]

    for day in forecast:
        date = day['date']
        current_date = datetime.today().strftime('%Y-%m-%d')
        if current_date!=date:
            data["forecast_date"] = date
            tomorrow = day["day"]
            data["forecast_max_temp"] = tomorrow["maxtemp_c"]
            data["forecast_min_temp"] = tomorrow["mintemp_c"]
            data["forecast_avg_temp"] = tomorrow["avgtemp_c"]
            data["forecast_max_wind"] = tomorrow["maxwind_kph"]
            data["forecast_rain"] = tomorrow["daily_chance_of_rain"]
            data["forecast_weather"] = tomorrow["condition"]["text"]
            data["forecast_weather_icon"] = tomorrow["condition"]["icon"]
            break

    return data

def stream_data(*op_args, **op_kwargs):
    import json
    from kafka import KafkaProducer
    import logging

    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

    logging.info(op_kwargs)

    apikey = op_kwargs['apikey']
    locations = op_kwargs['locations']
    days = op_kwargs['days']

    for location in locations:
        res = get_data(apikey, location, days)
        res = format_data(res)

        producer.send('weather_data', json.dumps(res).encode('utf-8'))

with DAG(
    'weather',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False
) as dag:

    get_weather_data = PythonOperator(
        task_id='weather_data',
        python_callable=stream_data,
        op_kwargs={'apikey': 'c04cb134e91f45e29c880259241406',
                   'locations': ['Mumbai', 'Pune', 'Surat', 'Kolkata', 'Jammu', 'Bikaner', 'Indore', 'Chennai', 'Bengaluru', 'Ludhiana'],
                   'days': 2}
    )




