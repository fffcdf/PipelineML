import pandas as pd
import psycopg2


def get_from_db():
    conn = psycopg2.connect(
        host='postgres-raw-data',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='raw'
    )

    quary="SELECT * from raw;"
    df=pd.read_sql(quary,conn)

    return df


def change_target(df):
    codes_df = pd.read_csv('/opt/airflow/resources/weatherapi_condition_codes.csv')
    code_to_category = dict(zip(codes_df['code'], codes_df['label']))
    df['target'] = df['condition_code'].map(code_to_category)
    df = df.drop('condition_code', axis=1)
    df=df.dropna()

    return df


def save_to_db(df):
    conn = psycopg2.connect(
        host='postgres-clean-data',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='clean'
    )

    cur = conn.cursor()

    insert_query = """
            INSERT INTO clean (
                temp_c, feelslike_c, humidity, pressure_mb, wind_kph, gust_kph,
                cloud, precip_mm, uv, is_day, target,
                hour, month, weekday, lat, lon
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """
    records = [
        (
            d["temp_c"], d["feelslike_c"], d["humidity"], d["pressure_mb"],
            d["wind_kph"], d["gust_kph"], d["cloud"], d["precip_mm"],
            d["uv"], d["is_day"], d["target"],
            d["hour"], d["month"], d["weekday"], d["lat"], d["lon"]
        )
        for d in df.to_dict(orient='records')
    ]

    cur.executemany(insert_query, records)
    conn.commit()
    cur.close()
    conn.close()


def preprocess():
    df=get_from_db()
    df = change_target(df)
    save_to_db(df)