import os
import json
import traceback
import sys
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

# ----- 1. ENVIRONMENT SETUP -----
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')
MINIO_CLEAN_BUCKET = os.getenv('MINIO_BUCKET_CLEANED', 'cleaned')

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PWD = os.getenv("PG_PASSWORD", "postgres")

RUN_ID = os.getenv("RUN_ID", "manual")

HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
metadata_path = os.path.join(HOME, "config", "metadata.json")
with open(metadata_path) as f:
    metadata = json.load(f)

spark = (
    SparkSession.builder.appName("LoadCleanedToPostgres")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

def cast_all_columns_to_string(df):
    return df.select([F.col(c).cast(StringType()).alias(c) for c in df.columns])

def extract_source_created_at(dirname, file_prefix):
    import re
    m = re.match(rf"{file_prefix}_(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}})", dirname)
    if m:
        return datetime.strptime(m.group(1), "%Y-%m-%d_%H-%M-%S").strftime("%Y-%m-%d %H:%M:%S")
    return None

def audit_log_to_postgres(log_dict):
    for col in ["dest_created_at", "source_created_at"]:
        if log_dict.get(col) and isinstance(log_dict[col], str):
            try:
                log_dict[col] = datetime.strptime(log_dict[col], "%Y-%m-%d %H:%M:%S")
            except Exception:
                log_dict[col] = None

    log_df = spark.createDataFrame([log_dict])
    log_df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
        .option("dbtable", "audit_load_history") \
        .option("user", PG_USER) \
        .option("password", PG_PWD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# ----- 2. LOAD DATA FROM MINIO TO POSTGRES -----
pg_schema = metadata['layers'][0]['short_name']

for tbl in metadata["tables"]:
    file_prefix = tbl['file_name']
    table_name = tbl['table_name']

    # Read ALL part files inside ALL batch folders for this table
    pattern = f"s3a://{MINIO_CLEAN_BUCKET}/{file_prefix}_*/"
    print(f"🔎 Loading pattern: {pattern}")
    try:
        df = spark.read.option("header", "true").csv(pattern)
        if df.rdd.isEmpty():
            print(f"No data found for {file_prefix}, skipping...")
            continue

        # Read audit table: get list of already loaded batches (by batch folder name)
        try:
            audit_df = spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
                .option("dbtable", "etl_load_history") \
                .option("user", PG_USER) \
                .option("password", PG_PWD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            loaded_batches = set(audit_df.filter(F.col("dest_name") == table_name)
                .select("source_name").rdd.flatMap(lambda x: x).collect())
        except Exception:
            loaded_batches = set()  # Audit table might not exist yet!

        # Tag rows with input file path, extract batch_dir from that path
        df_with_file = df.withColumn("_input_file_name", F.input_file_name())
        df_with_file = df_with_file.withColumn(
            "batch_dir",
            F.regexp_extract(
                F.col("_input_file_name"),
                rf"{file_prefix}_(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}})", 0
            )
        )
        batch_dirs = df_with_file.select("batch_dir").distinct().rdd.flatMap(lambda x: x).collect()

        for batch in batch_dirs:
            if not batch or batch in loaded_batches:
                continue
            batch_df = df_with_file.filter(F.col("batch_dir") == batch).drop("_input_file_name", "batch_dir")
            start_time = datetime.now()
            log = {
                "run_id": RUN_ID,
                "source_system": "minio",
                "dest_system": "postgres",
                "source_name": batch,
                "dest_name": table_name,
                "source_created_at": extract_source_created_at(batch, file_prefix),
                "dest_created_at": None,
                "duration": None,
                "record_count": None,
                "status": "running",
                "message": "",
                "load_type": "append"
            }
            try:
                batch_df_str = cast_all_columns_to_string(batch_df)
                record_count = batch_df_str.count()
                log["record_count"] = record_count
                batch_df_str.write.format("jdbc") \
                    .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
                    .option("dbtable", f'{pg_schema}."{table_name}"') \
                    .option("user", PG_USER) \
                    .option("password", PG_PWD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                log["status"] = "success"
                log["message"] = f"Loaded {batch} to {table_name} successfully."
            except Exception as e:
                log["status"] = "fail"
                log["message"] = f"{e}\n{traceback.format_exc()}"
                print(f"❌ Error loading {batch}:\n", traceback.format_exc())
            finally:
                end_time = datetime.now()
                log["duration"] = (end_time - start_time).total_seconds()
                log["dest_created_at"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
                audit_log_to_postgres(log)
                if log["status"] == "fail":
                    print(f"❌ Failed to load {batch} to {table_name}.")
                    sys.exit(1)
                else:
                    print(f"✅ Successfully loaded {batch} to {table_name}.")
    except Exception as e:
        print(f"❌ Error loading pattern {pattern}:\n", traceback.format_exc())
        sys.exit(1)

spark.stop()
