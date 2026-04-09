import os, sys

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
import json

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("FraudDetection_Streaming") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load model
model = PipelineModel.load("models/best_model/tuned_model")
with open("models/best_model/model_info.json") as f:
    model_info = json.load(f)

optimal_threshold = model_info["optimal_threshold"]
print("=" * 60)
print("FRAUD DETECTION — STREAMING")
print("=" * 60)
print(f"Model: {model_info['best_case']}")
print(f"Threshold: {optimal_threshold}")

# Schema JSON tu socket
json_schema = StructType([
    StructField("_c0", StringType()),
    StructField("trans_date_trans_time", StringType()),
    StructField("cc_num", StringType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("amt", StringType()),
    StructField("first", StringType()),
    StructField("last", StringType()),
    StructField("gender", StringType()),
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", StringType()),
    StructField("lat", StringType()),
    StructField("long", StringType()),
    StructField("city_pop", StringType()),
    StructField("job", StringType()),
    StructField("unix_time", StringType()),
    StructField("merch_lat", StringType()),
    StructField("merch_long", StringType()),
    StructField("is_fraud", StringType()),
    StructField("trans_num", StringType()),
    StructField("dob", StringType()),
])

AMT_UPPER = 192.01
FRAUD_LOG = "data/streaming/fraud_alerts.csv"

os.makedirs("data/streaming", exist_ok=True)
with open(FRAUD_LOG, "w") as f:
    f.write("batch,trans_date_trans_time,cc_num,merchant,category,amt,fraud_score\n")

def preprocess(df):
    df = df \
        .withColumn("trans_ts", F.to_timestamp("trans_date_trans_time", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("dob_date", F.to_date("dob", "yyyy-MM-dd"))
    df = df \
        .withColumn("amt", F.when(F.col("amt") > AMT_UPPER, AMT_UPPER).otherwise(F.col("amt"))) \
        .withColumn("hour", F.hour("trans_ts")) \
        .withColumn("day_of_week", F.dayofweek("trans_ts")) \
        .withColumn("month", F.month("trans_ts")) \
        .withColumn("age", F.floor(F.datediff("trans_ts", "dob_date") / 365.25)) \
        .withColumn("distance_km",
            6371 * 2 * F.asin(F.sqrt(
                F.pow(F.sin(F.radians(F.col("merch_lat") - F.col("lat")) / 2), 2) +
                F.cos(F.radians("lat")) * F.cos(F.radians("merch_lat")) *
                F.pow(F.sin(F.radians(F.col("merch_long") - F.col("long")) / 2), 2)
            ))
        )
    return df.select(
        "merchant", "category", "amt", "gender", "city", "state",
        "city_pop", "job", "hour", "day_of_week", "month", "age",
        "distance_km",
        "trans_date_trans_time", "cc_num", "trans_num"
    )

# Nhan du lieu tu socket
df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON
parsed = df.withColumn("data", F.from_json("value", json_schema)).select("data.*")

# Cast kieu du lieu
parsed = parsed \
    .withColumn("cc_num", F.col("cc_num").cast("long")) \
    .withColumn("amt", F.col("amt").cast("double")) \
    .withColumn("zip", F.col("zip").cast("integer")) \
    .withColumn("lat", F.col("lat").cast("double")) \
    .withColumn("long", F.col("long").cast("double")) \
    .withColumn("city_pop", F.col("city_pop").cast("integer")) \
    .withColumn("unix_time", F.col("unix_time").cast("long")) \
    .withColumn("merch_lat", F.col("merch_lat").cast("double")) \
    .withColumn("merch_long", F.col("merch_long").cast("double")) \
    .withColumn("is_fraud", F.col("is_fraud").cast("integer"))

stats = {"n": 0, "total": 0, "fraud": 0}

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    stats["n"] += 1
    n = stats["n"]

    processed = preprocess(batch_df)
    pred = model.transform(processed)

    if "probability" not in pred.columns:
        result = pred \
            .withColumn("prob_array", vector_to_array("rawPrediction")) \
            .withColumn("fraud_score", 1.0 / (1.0 + F.exp(-F.col("prob_array").getItem(1)))) \
            .withColumn("predicted_fraud", F.when(F.col("fraud_score") >= optimal_threshold, 1).otherwise(0))
    else:
        result = pred \
            .withColumn("fraud_score", vector_to_array("probability").getItem(1)) \
            .withColumn("predicted_fraud", F.when(F.col("fraud_score") >= optimal_threshold, 1).otherwise(0))

    total = result.count()
    fraud_detected = result.filter(F.col("predicted_fraud") == 1).count()

    stats["total"] += total
    stats["fraud"] += fraud_detected

    print(f"\n{'='*60}")
    print(f"BATCH {n} | {total} giao dich | Fraud detected: {fraud_detected}")
    print(f"{'='*60}")

    if fraud_detected > 0:
        print("  !! CANH BAO GIAN LAN !!")
        rows = result.filter(F.col("predicted_fraud") == 1) \
            .select("trans_date_trans_time", "cc_num", "merchant", "category", "amt",
                    F.round("fraud_score", 4).alias("score")) \
            .collect()
        with open(FRAUD_LOG, "a") as f:
            for row in rows:
                print(f"  {row['trans_date_trans_time']} | "
                      f"CC: ...{str(row['cc_num'])[-4:]} | "
                      f"{row['merchant'][:30]} | {row['category']} | "
                      f"${row['amt']:.2f} | score={row['score']}")
                f.write(f"{n},{row['trans_date_trans_time']},{row['cc_num']},"
                        f"{row['merchant']},{row['category']},{row['amt']},{row['score']}\n")
    else:
        print("  Khong phat hien gian lan.")

    print(f"  Tich luy: {stats['total']:,} giao dich | {stats['fraud']} canh bao fraud")

query = parsed.writeStream.foreachBatch(process_batch).start()

print(f"\nDang cho du lieu tu socket localhost:9999...")
print(f"Fraud alerts luu tai: {FRAUD_LOG}")
print(f"Nhan Ctrl+C de dung.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print(f"\n{'='*60}")
    print("TONG KET")
    print(f"{'='*60}")
    print(f"Tong batch: {stats['n']}")
    print(f"Tong giao dich: {stats['total']:,}")
    print(f"Tong fraud alerts: {stats['fraud']}")
    print(f"Fraud alerts da luu tai: {FRAUD_LOG}")
    query.stop()
    spark.stop()
    print("Da dung.")