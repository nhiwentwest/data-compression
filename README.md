
# Tính năng

- **API Backend (FastAPI)**: Xử lý yêu cầu từ thiết bị IoT và ứng dụng front-end
- **Tích hợp Adafruit IO**: Đồng bộ dữ liệu và lấy dữ liệu về database
- **Nén Dữ liệu IDEALEM**: Giảm dung lượng lưu trữ cần thiết mà vẫn giữ được chất lượng dữ liệu
- **Công cụ giải nén**: Phục hồi dữ liệu gốc từ dữ liệu nén

## Cài đặt và Sử dụng

1. Cài đặt Docker và Docker Compose (Khuyến nghị & tuỳ chọn) 
2. Tạo file `.env` 
``` bash
DATABASE_URL=postgresql://postgres:1234@localhost:5433/iot_db
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# config cho Adafruit IO
ADAFRUIT_IO_USERNAME=
ADAFRUIT_IO_KEY=
MQTT_HOST=io.adafruit.com
MQTT_PORT=8883 
MQTT_USERNAME=${ADAFRUIT_IO_USERNAME}
MQTT_PASSWORD=${ADAFRUIT_IO_KEY}
MQTT_TOPIC=${ADAFRUIT_IO_USERNAME}/feeds/#
MQTT_SSL=true  # Thêm flag để xác định có sử dụng SSL hay không

```
3. Chạy hệ thống:
   
#### Tạo môi trường ảo mới
```
python -m venv docker_env
```

#### Kích hoạt môi trường
```
source docker_env/bin/activate  # Trên macOS/Linux
docker_env\Scripts\activate # Trên Window
```

#### Cài đặt các gói với phiên bản tương thích
```
pip install docker-compose==1.29.2
```

### Thủ công

4. Cài đặt các thư viện

```
pip install -r requirements.txt
```

5. Thiết lặp database PostGre

Mình dùng port 5433 

```
python setup_database.py
```

6. Khởi chạy ứng dụng 
```
uvicorn main:app --reload
```


## Lấy dữ liệu từ Adafruit theo ngày cụ thể

  

Chọn một ngày để lấy dữ liệu từ Adafruit và lưu vào database.

  

### Lệnh cơ bản:

```
python fetch_adafruit_data_manual.py
```

Mặc định sẽ lấy dữ liệu của ngày hiện tại.

  

### Lấy dữ liệu theo ngày cụ thể:

```
python fetch_adafruit_data_manual.py --date 2023-03-30
```


### Giới hạn số lượng bản ghi:

```
python fetch_adafruit_data_manual.py --date 2023-11-20 --limit 100
```

  

### Nếu gặp lỗi, thử ép buộc tải lại dữ liệu:

```
python fetch_adafruit_data_manual.py --date 2025-03-30 --force-reload
```

## Công cụ nén và giải nén dữ liệu (Data Compression) 

Công cụ giải nén dữ liệu dùng để phục hồi dữ liệu gốc từ dữ liệu nén. Xem chi tiết cách sử dụng tại [README_DATA_DECOMPRESSION.md](./README_DATA_DECOMPRESSION.md).

Cấu trúc sử dụng cơ bản:
```
python decompress.py --device-id <name_device>
```

Output sẽ mặc định là <name_device>.json

## Ghi chú 

Mục tiêu tiếp theo:
- Tối ưu frontend.
- Tối ưu lại nội dung file data_compression.
- Hiển thị các test case để đảm bảo tính chính xác.
``` bash
compress.py để sử dụng thuật toán trong file data_compress.py 
visualization_analyzer.py để tạo biểu đồ thông qua compress.py
```

## Tài liệu khác

A data compression algorithm was used, based on the research paper “Dynamic Online Performance Optimization in Streaming Data Compression.”
