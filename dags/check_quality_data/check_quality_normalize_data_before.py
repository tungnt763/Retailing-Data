import json
import os
import sys
from pyspark.errors import AnalysisException  # Dành cho Spark >= 3.3
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T

# 1. Spark Session Setup
spark = (
    SparkSession.builder.appName("DQ_Normalize_Data")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'minio:9000'))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minio'))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minio123'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # BẮT BUỘC nếu MinIO không dùng HTTPS!
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', "minio")
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', "minio123")

metadata_path = os.path.join(HOME, "config", "metadata.json")

with open(metadata_path) as f:
    metadata = json.load(f)

global_formats = metadata["global_formats"]
date_formats = metadata.get("date_formats", ["yyyy-mm-dd"])

MINIO_BUCKET = os.getenv('MINIO_BUCKET_RAW', 'raw')
MINIO_CLEAN_BUCKET = os.getenv('MINIO_BUCKET_CLEANED', 'cleaned')

def rename_columns_by_metadata(df, fields):
    """
    Rename columns in df using the column_name mapping in metadata['fields'].
    """
    # Map: new_name (internal name) -> old_name (CSV column)
    mapping = {meta_key: rule['column_name'] for meta_key, rule in fields.items()}
    reverse_mapping = {v: k for k, v in mapping.items()}

    # Only select columns present in the DataFrame (CSV may have extra columns)
    cols = []
    for meta_key, csv_col in mapping.items():
        if csv_col in df.columns:
            cols.append(F.col(csv_col).alias(meta_key))
        else:
            # If a required column is missing, create a column with nulls (to catch not_null errors)
            cols.append(F.lit(None).alias(meta_key))
    return df.select(cols)


def minio_csv_pattern(bucket, file_pattern):
    return f"s3a://{bucket}/{file_pattern}"

def dq_agg_exprs(field_rules):
    exprs = []
    for field, rule in field_rules.items():
        if rule.get("not_null", False):
            exprs.append(F.sum(F.when(F.col(field).isNull() | (F.col(field) == ""), 1).otherwise(0)).alias(f"{field}_null_count"))
        if rule.get("unique", False):
            exprs.append((F.countDistinct(field) - F.count(F.col(field))).alias(f"{field}_dup_count"))
        if "allowed_values" in rule:
            allowed = rule["allowed_values"]
            exprs.append(F.sum(F.when(~F.col(field).isin(allowed), 1).otherwise(0)).alias(f"{field}_invalid_value_count"))
        if "format" in rule:
            fmt = rule["format"]
            regex = global_formats.get(fmt, fmt)
            exprs.append(F.sum(F.when(~F.col(field).rlike(regex) & F.col(field).isNotNull(), 1).otherwise(0)).alias(f"{field}_invalid_format_count"))
        for op in ["min", "max"]:
            if op in rule:
                value = rule[op]
                cond = F.col(field) >= value if op == "min" else F.col(field) <= value
                exprs.append(F.sum(F.when(~cond & F.col(field).isNotNull(), 1).otherwise(0)).alias(f"{field}_{op}_viol_count"))
    return exprs

def dq_and_normalize(df, table_meta):
    field_rules = table_meta["fields"]
    # DQ checks: gom thành 1 agg duy nhất
    exprs = dq_agg_exprs(field_rules)
    dq_result = df.agg(*exprs).collect()[0].asDict()
    errors = []
    for k, v in dq_result.items():
        if v > 0:
            errors.append(f"{k}: {v} error(s)")
    if errors:
        print(f"\n--- Data Quality Issues for {table_meta['table_name']} ---")
        for err in errors:
            print(err)
    else:
        print(f"\n{table_meta['table_name']}: All data quality checks PASSED.")

    # Normalization (lazy): chỉ tạo transform, chưa action
    norm_df = df
    for field, rule in field_rules.items():
        if rule.get("format") == "email":
            norm_df = norm_df.withColumn(field, F.lower(F.trim(F.col(field))))
        if rule.get("type") == "string":
            norm_df = norm_df.withColumn(field, F.trim(F.col(field)))
        if rule.get("type") == "date":
            for fmt in date_formats:
                norm_df = norm_df.withColumn(field, F.to_date(F.col(field), fmt))
        if rule.get("format") == "phone_global":
            norm_df = norm_df.withColumn(field, F.regexp_replace(F.col(field), "[^\\d\\+]", ""))
    return norm_df

if __name__ == "__main__":
    for tbl in metadata["tables"]:
        file_pattern = f"{tbl['file_name']}_*.csv"
        try:
            try:
                df = spark.read.option("header", "true").csv(minio_csv_pattern(MINIO_BUCKET, file_pattern))
            except AnalysisException as ae:
                print(f"⚠️ No files found for pattern {file_pattern}. Skipping table '{tbl['file_name']}'.")
                continue
            except Exception as e:
                print(f"❌ Unexpected error reading files for pattern {file_pattern}:\n{traceback.format_exc()}")
                sys.exit(1)

            if df.rdd.isEmpty():
                print(f"No files matching {file_pattern} found.")
                continue

            df = rename_columns_by_metadata(df, tbl['fields'])
            norm_df = dq_and_normalize(df, tbl)

            # Ghi ra đúng định dạng thư mục theo ngày
            current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            output_dir = f"s3a://{MINIO_CLEAN_BUCKET}/{tbl['file_name']}_{current_time}/"
            norm_df.write.mode("overwrite").option("header", "true").csv(output_dir)
            print(f"Written cleaned files for pattern {file_pattern} to {output_dir}")

            # (Tuỳ chọn) Sau này dùng Python nối lại thành 1 file duy nhất nếu thực sự cần!
        except Exception as e:
            print(f"Error for pattern {file_pattern}: {e}")
            sys.exit(1)


print("All tables processed successfully.")