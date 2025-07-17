import psycopg2
import aiohttp
from datetime import datetime
import asyncio
from dotenv import load_dotenv
import logging


logger = logging.getLogger(__name__)
load_dotenv()
API_KEY = ''
BASE_URL = "http://api.weatherapi.com/v1/current.json"
MAX_CONCURRENT_REQUESTS = 1000

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


def get_citites():
    conn = psycopg2.connect(
        host='postgres-cities',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='cities'
    )

    cur = conn.cursor()
    cur.execute("SELECT city FROM cities LIMIT 5000;")
    rows = cur.fetchall()
    cities = [row[0] for row in rows]
    cur.close()
    conn.close()

    return cities

async def fetch_city_weather(session, city):
    params = {"key": API_KEY, "q": city, "aqi": "no"}

    async with semaphore:
        try:
            async with session.get(BASE_URL, params=params) as response:
                if response.status == 429:
                    logger.info(f"Достигнуто ограничение по тарифу. Пропуск города: {city}")

                    return None

                response.raise_for_status()
                data = await response.json()

                current = data["current"]
                location = data["location"]
                dt = datetime.strptime(location["localtime"], "%Y-%m-%d %H:%M")

                return {
                    "temp_c": current["temp_c"],
                    "feelslike_c": current["feelslike_c"],
                    "humidity": current["humidity"],
                    "pressure_mb": current["pressure_mb"],
                    "wind_kph": current["wind_kph"],
                    "gust_kph": current.get("gust_kph", 0.0),
                    "cloud": current.get("cloud", 0),
                    "precip_mm": current.get("precip_mm", 0.0),
                    "uv": current.get("uv", 0.0),
                    "is_day": current.get("is_day", 1),
                    "condition_code": current["condition"]["code"],
                    "hour": dt.hour,
                    "month": dt.month,
                    "weekday": dt.weekday(),
                    "lat": location["lat"],
                    "lon": location["lon"]
                }
        except Exception as e:
            logger.info(f"[ERROR] {city}: {e}")
            return None

async def fetch_all_weather_data(cities):
    weather_data = []
    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_city_weather(session, city) for city in cities]
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                if result:
                    weather_data.append(result)
            except Exception as e:
                logger.info(f"[ERROR]: {e}")

    return weather_data


def get_data_and_load_data():
    import asyncio

    logger.info("Получаем список городов...")
    CITIES = get_citites()
    logger.info(f"Городов получено: {len(CITIES)}")

    logger.info("Запрос к API...")
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        weather_data = loop.run_until_complete(fetch_all_weather_data(CITIES))

    except Exception as e:
        logger.info(f"Ошибка при получении погодных данных: {e}")
        return

    logger.info(f"Получено {len(weather_data)} погодных записей")

    if not weather_data:
        logger.info("Нет данных для вставки.")

        return

    conn = psycopg2.connect(
        host='postgres-raw-data',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='raw'
    )

    cur = conn.cursor()

    insert_query = """
        INSERT INTO raw (
            temp_c, feelslike_c, humidity, pressure_mb, wind_kph, gust_kph,
            cloud, precip_mm, uv, is_day, condition_code,
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
            d["uv"], d["is_day"], d["condition_code"],
            d["hour"], d["month"], d["weekday"], d["lat"], d["lon"]
        )
        for d in weather_data
    ]

    cur.executemany(insert_query, records)
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Загружено {len(records)} строк в таблицу raw.")

