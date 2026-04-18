import os
import json
import time 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json , concat, concat_ws , lit , from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType


#KAFKA_BOOTRAP_SERVERS = my-kafka:9092
#KAFKA_TOPIC = raw_weather_data :)

#--- LẤY THÔNG TIN MINIO TỪ BIẾN MÔI TRƯỜNG --- (cấu  hình minio để lưu checkpoint))
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "password123")
#--- KHỞI TẠO SPARK --- (for minio) 
spark = SparkSession.builder \
    .appName("Weather-Speed-Layer") \
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000") 
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_USER)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_PASS)
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# --- SCHEMA ---
current_conditions_schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("datetimeEpoch", LongType(), True),
        StructField("temp", DoubleType(), True),
        StructField("feelslike", DoubleType(), True),   #Cảm giác nhiệt độ thực tế (feels like) có thể khác với nhiệt độ đo được (temp) do độ ẩm, gió, v.v.
        StructField("humidity", DoubleType(), True),
        StructField("dew", DoubleType(), True),        #Điểm sương (dew point) là nhiệt độ mà tại đó không khí bão hòa và hơi nước bắt đầu ngưng tụ thành sương hoặc mưa. Điểm sương càng cao thì không khí càng ẩm.
        StructField("precip", DoubleType(), True),     #Lượng mưa (precipitation) trong khoảng thời gian nhất định, thường tính bằng mm hoặc inch. Giá trị này cho biết lượng nước mưa đã rơi xuống mặt đất.
        StructField("precipprob", DoubleType(), True), #Xác suất có mưa (precipitation probability) được biểu thị dưới dạng phần trăm. Giá trị này cho biết khả năng xảy ra mưa trong khoảng thời gian dự báo.
        StructField("snow", DoubleType(), True),       #Lượng tuyết (snow) trong khoảng thời gian nhất định, thường tính bằng mm hoặc inch. Giá trị này cho biết lượng tuyết đã rơi xuống mặt đất.
        StructField("snowdepth", DoubleType(), True),  #Độ dày tuyết (snow depth) là độ sâu của lớp tuyết trên mặt đất, thường tính bằng cm hoặc inch. Giá trị này cho biết độ dày của lớp tuyết đã tích tụ.
        StructField("preciptype", ArrayType(StringType()), True),  #Loại hình mưa (precipitation type) có thể là mưa, tuyết, mưa đá, v.v. Giá trị này thường được biểu thị dưới dạng một mảng các chuỗi, vì có thể có nhiều loại hình mưa xảy ra cùng lúc.
        StructField("windgust", DoubleType(), True),   #Tốc độ gió giật (wind gust) là tốc độ gió mạnh nhất được ghi nhận trong khoảng thời gian nhất định, thường tính bằng km/h hoặc mph. Giá trị này cho biết mức độ mạnh của gió giật.
        StructField("windspeed", DoubleType(), True),  #Tốc độ gió (wind speed) là tốc độ trung bình của gió trong khoảng thời gian nhất định, thường tính bằng km/h hoặc mph. Giá trị này cho biết mức độ mạnh của gió.
        StructField("winddir", DoubleType(), True),    #Hướng gió (wind direction) được biểu thị bằng góc độ từ 0 đến 360, trong đó 0 hoặc 360 là gió thổi từ phía bắc, 90 là gió thổi từ phía đông, 180 là gió thổi từ phía nam, và 270 là gió thổi từ phía tây. Giá trị này cho biết hướng mà gió đang thổi.
        StructField("pressure", DoubleType(), True),   #Áp suất khí quyển (pressure) thường được tính bằng hPa (hectopascals) hoặc mb (millibars). Giá trị này cho biết mức độ áp suất của không khí tại thời điểm đo.
        StructField("visibility", DoubleType(), True), #Tầm nhìn (visibility) thường được tính bằng km hoặc miles. Giá trị này cho biết khoảng cách mà một người có thể nhìn thấy rõ ràng trong điều kiện thời tiết hiện tại.
        StructField("cloudcover", DoubleType(), True), #Độ che phủ mây (cloud cover) được biểu thị dưới dạng phần trăm. Giá trị này cho biết mức độ che phủ của mây trên bầu trời, với 0% là trời quang đãng và 100% là trời đầy mây.
        StructField("solarradiation", DoubleType(), True), #Bức xạ mặt trời (solar radiation) thường được tính bằng W/m² (watts per square meter). Giá trị này cho biết lượng năng lượng mặt trời chiếu xuống một diện tích nhất định trong khoảng thời gian nhất định.
        StructField("solarenergy", DoubleType(), True),#Năng lượng mặt trời (solar energy) thường được tính bằng kWh/m² (kilowatt-hours per square meter). Giá trị này cho biết lượng năng lượng mặt trời tích tụ trên một diện tích nhất định trong khoảng thời gian nhất định.
        StructField("uvindex", DoubleType(), True),    #Chỉ số UV (UV index) là một thang đo từ 0 trở lên, với các mức độ nguy hiểm tăng dần. Chỉ số này cho biết mức độ nguy hiểm của tia UV từ mặt trời, với 0-2 là thấp, 3-5 là trung bình, 6-7 là cao, 8-10 là rất cao, và 11 trở lên là cực kỳ nguy hiểm.
        StructField("conditions", StringType(), True), #Mô tả điều kiện thời tiết (weather conditions) thường là một chuỗi văn bản ngắn gọn mô tả tình trạng thời tiết hiện tại, như "Clear" (trời quang đãng), "Partly Cloudy" (trời có mây), "Rain" (mưa), "Snow" (tuyết), v.v. Giá trị này cho biết tình trạng thời tiết tổng thể tại thời điểm đo.
        StructField("icon", StringType(), True),       #Biểu tượng thời tiết (weather icon) thường là một chuỗi văn bản hoặc URL dẫn đến một hình ảnh biểu tượng đại diện cho điều kiện thời tiết hiện tại, như "clear-day" (trời quang đãng ban ngày), "rain" (mưa), "snow" (tuyết), v.v. Giá trị này cho biết biểu tượng trực quan để đại diện cho tình trạng thời tiết.
        StructField("stations", ArrayType(StringType()), True),  #Danh sách các trạm thời tiết (weather stations) cung cấp dữ liệu, thường được biểu thị dưới dạng một mảng các chuỗi, trong đó mỗi chuỗi là một mã hoặc tên của trạm thời tiết. Giá trị này cho biết nguồn dữ liệu thời tiết được sử dụng để tạo ra dự báo. 
        StructField("source", StringType(), True),     #Nguồn dữ liệu (source) thường là một chuỗi văn bản ngắn gọn mô tả nguồn gốc của dữ liệu thời tiết, như "Visual Crossing Weather API", "OpenWeatherMap", v.v. Giá trị này cho biết nhà cung cấp hoặc dịch vụ đã cung cấp dữ liệu thời tiết.
        StructField("sunrise", StringType(), True),    #Thời gian mặt trời mọc (sunrise) thường được biểu thị dưới dạng một chuỗi thời gian, như "06:30:00" hoặc "2024-06-01T06:30:00". Giá trị này cho biết thời điểm mặt trời mọc trong ngày.
        StructField("sunriseEpoch", LongType(), True), #Thời gian mặt trời mọc được biểu thị dưới dạng epoch time (số giây kể từ 1/1/1970). Giá trị này cho biết thời điểm mặt trời mọc dưới dạng số nguyên.
        StructField("sunset", StringType(), True),     #Thời gian mặt trời lặn (sunset) thường được biểu thị dưới dạng một chuỗi thời gian, như "18:45:00" hoặc "2024-06-01T18:45:00". Giá trị này cho biết thời điểm mặt trời lặn trong ngày.
        StructField("sunsetEpoch", LongType(), True),  #Thời gian mặt trời lặn được biểu thị dưới dạng epoch time (số giây kể từ 1/1/1970). Giá trị này cho biết thời điểm mặt trời lặn dưới dạng số nguyên.
        StructField("moonphase", DoubleType(), True)   #Pha mặt trăng (moon phase) được biểu thị dưới dạng một số thập phân từ 0 đến 1, trong đó 0 là trăng mới (new moon), 0.25 là trăng khuyết dần (waxing crescent), 0.5 là trăng tròn (full moon), và 0.75 là trăng khuyết lặn (waning crescent). Giá trị này cho biết pha hiện tại của mặt trăng.
    ])
weather_schema = StructType([
    StructField("resolvedAddress", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("currentConditions", current_conditions_schema, True)
])

# --- ĐỌC TỪ KAFKA ---
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-kafka:9092") \
    .option("subscribe", "raw_weather_data") \
    .load()
df_final = df_kafka.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), weather_schema)) \
    .select(
        col("data.resolvedAddress").alias("Location"),
        col("data.timezone").alias("Timezone"),
        from_unixtime(col("data.currentConditions.datetimeEpoch"), "yyyy-MM-dd'T'HH:mm:ss").alias("Local_Time"),
        col("data.currentConditions.temp").alias("Temp_C"),
        col("data.currentConditions.feelslike").alias("Feels_Like"),
        col("data.currentConditions.humidity").alias("Humidity_%"),
        col("data.currentConditions.dew").alias("Dew_Point"),
        col("data.currentConditions.precip").alias("Precip_mm"),
        col("data.currentConditions.precipprob").alias("Precip_Prob_%"),
        col("data.currentConditions.preciptype").alias("Precip_Type"),
        col("data.currentConditions.snow").alias("Snow"),
        col("data.currentConditions.snowdepth").alias("Snow_Depth"),
        col("data.currentConditions.windspeed").alias("Wind_Speed"),
        col("data.currentConditions.windgust").alias("Wind_Gust"),
        col("data.currentConditions.winddir").alias("Wind_Dir"),
        col("data.currentConditions.pressure").alias("Pressure_hPa"),
        col("data.currentConditions.visibility").alias("Visibility_km"),
        col("data.currentConditions.cloudcover").alias("Cloud_Cover_%"),
        col("data.currentConditions.uvindex").alias("UV_Index"),
        col("data.currentConditions.solarradiation").alias("Solar_Rad"),
        col("data.currentConditions.solarenergy").alias("Solar_Energy"),
        col("data.currentConditions.conditions").alias("Conditions"),
        col("data.currentConditions.icon").alias("Icon"),
        col("data.currentConditions.sunrise").alias("Sunrise"),
        col("data.currentConditions.sunset").alias("Sunset"),
        col("data.currentConditions.moonphase").alias("Moon_Phase"),
        col("data.currentConditions.stations").alias("Stations"),
        col("data.currentConditions.source").alias("Source")
    ) \
    .withColumn("es_id", concat_ws("_", col("Location"), col("Local_Time")))  #GHÉP CHUỖI TẠO ID DUY NHẤT

# --- HÀM GHI DỮ LIỆU VÀO ES ---
def write_to_es(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    # 1. Ghi dữ liệu vào Elasticsearch với cấu hình kết nối đến ES trong K8s (qua port-forward)
    batch_df.write \
        .format("es") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.mapping.id", "es_id") \
        .option("es.resource", "weather_realtime")\
        .mode("append") \
        .save()
    
    # 2. Ghi dữ liệu dạng Parquet xuống MinIO 
    # batch_df.write \
    #     .format("parquet") \
    #     .mode("append") \
    #     .save("s3a://weather-raw-data/streaming_logs/")
        
    print(f"--- Đã ghi Micro-Batch {batch_id} vào ES ---")

# --- CHẠY STREAM ---
query = df_final.writeStream \
    .foreachBatch(write_to_es) \
    .option("checkpointLocation", "s3a://raw-weather-data/checkpoints/") \
    .trigger(processingTime="1 minute") \
    .start()

print("Đang chạy ...")
query.awaitTermination()