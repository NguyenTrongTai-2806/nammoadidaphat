import os
import signal
import requests
import json
import time
import sys 
from datetime import datetime 
from kafka import KafkaProducer

print(" Container initialized!")

TOPIC_NAME = 'raw_weather_data'
CITIES = ["Hanoi,VN", "HoChiMinh,VN", "Danang,VN", "Haiphong,VN", "Pleiku,VN", "CanTho,VN"]
API_KEYS_STRING = os.environ.get("WEATHER_API_KEYS")
if not API_KEYS_STRING:
    print("!!! [LỖI]: Không tìm thấy WEATHER_API_KEYS trong biến môi trường.")
    sys.exit(1)
# Cắt chuỗi thành danh sách (list) các keys
API_KEYS = [key.strip() for key in API_KEYS_STRING.split(",")]
current_key_index = 0 # Biến theo dõi xem đang dùng key thứ mấy

# --- CẤU HÌNH KAFKA PRODUCER ---
# Tạo một kafka producer kết nối đến cổng 9092
# Lấy địa chỉ Kafka từ biến môi trường của K8s truyền vào, nếu không có thì mặc định là localhost
KAFKA_BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f" Đang kết nối tới cụm Kafka tại địa chỉ {KAFKA_BROKER}...")

try: 
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8') , 
        api_version_auto_timeout_ms=5000, # Chỉ đợi 5s để dò version
        request_timeout_ms=5000 # Chỉ đợi 5s cho mỗi request
    )
    print("Đã kết nối Kafka thành công! Bắt đầu thu thập dữ liệu...")
except Exception as e:
    print(f"!!!  Không thể kết nối tới Kafka. Chi tiết: {e}")
    sys.exit(1) # Dừng container nếu không kết nối được Kafka

# Graceful Shutdown
def graceful_shutdown(signum, frame):
    print("\n>>> Đang tắt Crawler, đẩy nốt dữ liệu trong buffer lên Kafka...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)



# --- PHẦN 2: HÀM LẤY DỮ LIỆU ---
def get_weather_data(city):
    global current_key_index # Sử dụng biến global để lưu vết index qua các lần gọi hàm
    keys_attempted = 0 # Đếm số key đã thử trong 1 vòng
    
    # Vòng lặp này sẽ thử lần lượt các key nếu gặp lỗi 429
    while keys_attempted < len(API_KEYS):
        current_key = API_KEYS[current_key_index]
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}?unitGroup=metric&contentType=json&key={current_key}"
        
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()           
        elif response.status_code == 429:
            # Nếu bị chặn, tăng biến đếm và đổi sang key tiếp theo
            print(f"[CRITICAL] API Key thứ {current_key_index + 1} reached limit (Rate Limit 429)!")
            current_key_index = (current_key_index + 1) % len(API_KEYS) # Quay vòng lại 0 nếu vượt quá số lượng key
            keys_attempted += 1
            print(f"-> Switching to API Key thứ {current_key_index + 1}...")
            
        else:
            print(f"[LỖI] API trả về mã lỗi: {response.status_code}")
            return None
            
    # Nếu vòng lặp while kết thúc mà vẫn chạy xuống đây, tức là TOÀN BỘ key đều bị 429
    print("[CRITICAL] ALL APIS REACH LIMIT. Sleeping for 10 minutes to avoid IP block...")
    time.sleep(600) # Ép ngủ 10 phút để API Provider không block IP 
    return None

# --- PHẦN 3: Gửi dữ liệu vào kafka ---
print("Bắt đầu thu thập và gửi dữ liệu vào Kafka...")


while True:
    for city in CITIES:
        data = get_weather_data(city)    
        if data:
            # Lấy thời gian hiện tại và format chuẩn ISO 8601 để ES hiểu được
            chuoi_thoi_gian_chuan = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            date_time = data['currentConditions']['datetime'] 
            # if 'currentConditions' in data:
            #     data['currentConditions']['datetime'] = chuoi_thoi_gian_chuan    
            producer.send(TOPIC_NAME, value=data)       
            print(f" query_time:[{chuoi_thoi_gian_chuan}] và local_time:[{date_time}] Đã gửi dữ liệu: {city} ")  #logs 
            # Nghỉ 2 giây 
            time.sleep(2)
    # Nghỉ 20 giây rồi mới lấy tiếp 
    time.sleep(30*60)