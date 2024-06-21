from cassandra.cluster import Cluster
import logging

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS weather.daily_weather (
            name TEXT,
            region TEXT,
            country TEXT,
            lat TEXT,
            lon TEXT,
            datetime TEXT,
            time TEXT,
            temp_degree_celcius TEXT,
            weather TEXT,
            weather_icon TEXT,
            wind_kph TEXT,
            precip_mm TEXT,
            forecast_date TEXT,
            forecast_max_temp TEXT,
            forecast_min_temp TEXT,
            forecast_avg_temp TEXT,
            forecast_max_wind TEXT,
            forecast_rain TEXT,
            forecast_weather TEXT,
            forecast_weather_icon TEXT,
            PRIMARY KEY (name, datetime)
        );
    """)

    print("Table created successfully")

def cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()

        create_keyspace(session)
        create_table(session)

        return session
    except Exception as e:
        logging.error(f"Couldn't create the cassandra session due to exception {e}")
