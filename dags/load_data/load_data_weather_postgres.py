import os
import urllib.request
import json
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

# ----- ENV -----
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PWD = os.getenv("PG_PASSWORD", "postgres")
PG_SCHEMA = "stg"  # ví dụ, có thể đổi

RUN_ID = os.getenv("RUN_ID", "manual")
TASK_ID = os.getenv("TASK_ID", "manual_task")  # Lấy từ Airflow context nếu có

spark = (
    SparkSession.builder.appName("EnrichWeatherOptimized")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# ----- 1. Đọc transactions & stores -----
transactions_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
    .option("dbtable", f"{PG_SCHEMA}.transactions") \
    .option("user", PG_USER).option("password", PG_PWD) \
    .option("driver", "org.postgresql.Driver").load()

stores_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
    .option("dbtable", f"{PG_SCHEMA}.stores") \
    .option("user", PG_USER).option("password", PG_PWD) \
    .option("driver", "org.postgresql.Driver").load()

# ----- 2. Join, lấy unique key -----
trans_stores = transactions_df.join(
    stores_df, transactions_df.store_id == stores_df.store_id, "left"
).withColumn(
    "weather_date", F.to_date("trans_date")
).select(
    F.col("weather_date"),
    F.col("store_city").alias("city"),
    F.col("store_country").alias("country"),
    F.col("store_lat").cast("float").alias("latitude"),
    F.col("store_long").cast("float").alias("longitude")
).distinct().dropna(subset=["latitude", "longitude", "weather_date"])

# Đưa về driver xử lý vì unique key nhỏ
unique_weather_keys = [(
    row['weather_date'].strftime('%Y-%m-%d'),
    row['latitude'],
    row['longitude'],
    row['city'],
    row['country']
) for row in trans_stores.collect()]

# ----- 3. Đọc bảng weather hiện có -----
try:
    weather_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
        .option("dbtable", f"{PG_SCHEMA}.weather") \
        .option("user", PG_USER).option("password", PG_PWD) \
        .option("driver", "org.postgresql.Driver").load()
    existed_keys = set([
        (row['weather_date'].strftime('%Y-%m-%d'), row['latitude'], row['longitude'])
        for row in weather_df.select(
            F.col("weather_date"), F.col("latitude"), F.col("longitude")
        ).distinct().collect()
    ])
except Exception:
    existed_keys = set()  # bảng chưa có dữ liệu

# ----- 4. Lọc những key chưa có -----
to_call_keys = []
for (date, lat, lon, city, country) in unique_weather_keys:
    if (date, lat, lon) not in existed_keys:
        to_call_keys.append((date, lat, lon, city, country))

# ----- 5. Gọi API và lưu mới -----
def fetch_weather(lat, lon, date, city, country):
    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={date}&end_date={date}"
        "&hourly=temperature_2m,rain,showers,snowfall&timezone=Asia/Bangkok"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read())
            temps = data['hourly'].get('temperature_2m', [])
            rain = data['hourly'].get('rain', [])
            showers = data['hourly'].get('showers', [])
            snowfall = data['hourly'].get('snowfall', [])
            temp_avg = sum(temps)/len(temps) if temps else None
            rain_sum = sum(rain) if rain else None
            showers_sum = sum(showers) if showers else None
            snowfall_sum = sum(snowfall) if snowfall else None
            return {
                "weather_date": date,
                "city": city,
                "country": country,
                "latitude": lat,
                "longitude": lon,
                "temperature_avg": temp_avg,
                "precipitation": rain_sum,
                "rain": rain_sum,
                "showers": showers_sum,
                "snowfall": snowfall_sum,
                "created_at": datetime.now(),
                "batch_id": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "run_id": RUN_ID,
                "task_id": TASK_ID
            }
    except Exception as e:
        print(f"❌ Weather fetch error for {lat},{lon},{date}: {e}")
        return None

weather_records = []
for (date, lat, lon, city, country) in to_call_keys:
    rec = fetch_weather(lat, lon, date, city, country)
    if rec:
        weather_records.append(rec)

if weather_records:
    columns = [
        "weather_date", "city", "country", "latitude", "longitude",
        "temperature_avg", "precipitation", "rain", "showers", "snowfall",
        "created_at", "batch_id", "run_id", "task_id"
    ]

    # Sau khi có weather_records:
    weather_new_df = spark.createDataFrame(weather_records)
    weather_new_df = weather_new_df.select(columns)  # reorder columns

    weather_new_df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
        .option("dbtable", f"{PG_SCHEMA}.weather") \
        .option("user", PG_USER) \
        .option("password", PG_PWD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"✅ Loaded {len(weather_records)} new weather records.")
else:
    print("✅ No new weather records to fetch.")

spark.stop()
