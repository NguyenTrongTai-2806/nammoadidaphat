from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min
import os

# --- LẤY THÔNG TIN MINIO TỪ BIẾN MÔI TRƯỜNG ---
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "password123")

print("Khởi động Batch Job: Kéo lịch sử từ MinIO...")

# 1. KHỞI TẠO SPARK VÀ CẤU HÌNH S3 
spark = SparkSession.builder \
    .appName("WeatherBatchLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000") # Nếu chạy bằng Airflow Docker thì đổi localhost thành minio
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_USER)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_PASS)
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 2. ĐỌC DỮ LIỆU TỪ MINIO
df_lake = spark.read.parquet("s3a://weather-raw-data/streaming_logs/")

# 3. TÍNH TOÁN 
df_batch_result = df_lake.groupBy("Location").agg(
    avg("Temp_C").alias("Nhiet_Do_Trung_Binh"),
    max("Temp_C").alias("Nhiet_Do_Cao_Nhat"),
    min("Temp_C").alias("Nhiet_Do_Thap_Nhat"),
    avg("Humidity_%").alias("Do_Am_Trung_Binh")
)
print("--- KẾT QUẢ TÍNH TOÁN BATCH TỪ MINIO ---")
df_batch_result.show()

# 4. LƯU KẾT QUẢ VÀO ELASTICSEARCH
df_batch_result.write \
    .format("es") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true") \
    .mode("overwrite") \
    .save("weather_batch_data") 

print("Đã lưu kết quả Batch lên Elasticsearch!")
spark.stop()