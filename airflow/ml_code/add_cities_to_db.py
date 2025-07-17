import psycopg2
import pandas as pd


def fill_db():
    conn=psycopg2.connect(
        host='postgres-cities',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='cities'
    )
    cur=conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS
    cities (
            city VARCHAR(100)
    PRIMARY KEY
    );""")

    city_df = pd.read_csv('/opt/airflow/resources/cities_list.csv')
    unique_cities = city_df["asciiname"].dropna().drop_duplicates()
    selected_cities = unique_cities
    cities = selected_cities.tolist()

    for city in cities:
        cur.execute('INSERT INTO cities (city) VALUES (%s) ON CONFLICT DO NOTHING;',(city,))

    conn.commit()
    cur.close()
    conn.close()