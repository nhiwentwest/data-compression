#!/usr/bin/env python3
"""
Script để tạo dữ liệu giả lập có 2 mẫu template khác nhau và lưu vào bảng original_samples.

Mục tiêu là tạo ra dữ liệu trong 1 tuần (2016 điểm, cách 5 phút) với hai mẫu phân phối khác nhau:
1. Mẫu ngày làm việc (thứ 2 - thứ 6): Có đặc trưng riêng của người đi làm
2. Mẫu ngày cuối tuần (thứ 7 - chủ nhật): Có đặc trưng riêng của người nghỉ ngơi

Cách sử dụng:
    python3 gentwo.py [--device-id DEVICE_ID] [--start-date YYYY-MM-DD] 
                      [--num-days NUM_DAYS] [--no-save-db]
"""

import os
import sys
import logging
import argparse
import random
import math
import datetime
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                   handlers=[
                       logging.StreamHandler(),
                       logging.FileHandler("gentwo.log")
                   ])
logger = logging.getLogger(__name__)

# Tải biến môi trường
load_dotenv()

# Kết nối database từ biến môi trường
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:1234@localhost:5433/iot_db")

def setup_database():
    """
    Thiết lập kết nối đến database và đảm bảo bảng original_samples đã được tạo
    
    Returns:
        engine: SQLAlchemy engine, hoặc None nếu không thể kết nối
    """
    try:
        # Tạo engine kết nối đến database
        engine = create_engine(DATABASE_URL)
        
        # Kiểm tra kết nối
        with engine.connect() as conn:
            # Tạo bảng devices nếu chưa tồn tại
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS devices (
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR UNIQUE NOT NULL,
                    name VARCHAR,
                    description VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Tạo index cho bảng devices
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_devices_id ON devices (id);
                CREATE UNIQUE INDEX IF NOT EXISTS ix_devices_device_id ON devices (device_id);
            """))
            
            # Tạo bảng original_samples nếu chưa tồn tại
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS original_samples (
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR NOT NULL,
                    original_data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (device_id) REFERENCES devices(device_id)
                )
            """))
            
            # Tạo index cho bảng original_samples
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_original_samples_device_id ON original_samples (device_id);
                CREATE INDEX IF NOT EXISTS idx_original_samples_timestamp ON original_samples (timestamp);
            """))
            
            conn.commit()
            
        logger.info(f"Đã kết nối thành công đến database: {DATABASE_URL}")
        return engine
    except Exception as e:
        logger.error(f"Lỗi khi kết nối đến database: {str(e)}")
        return None

def generate_workday_pattern(point_time: datetime, base_temp=22.0, base_humidity=65.0) -> Dict[str, float]:
    """
    Tạo mẫu dữ liệu cho ngày làm việc (Thứ 2 - Thứ 6)
    
    Đặc trưng:
    - Sáng sớm (6-8h): Tăng công suất nhanh (chuẩn bị đi làm)
    - Giữa ngày (9-16h): Công suất thấp (đi làm)
    - Chiều tối (17-22h): Công suất cao và ổn định (về nhà)
    - Đêm khuya (23-5h): Công suất rất thấp (ngủ)
    
    Args:
        point_time: Thời điểm dữ liệu
        base_temp, base_humidity: Giá trị cơ bản
        
    Returns:
        Dict chứa các giá trị cảm biến
    """
    hour = point_time.hour
    minute = point_time.minute
    
    # Tính giờ dạng thập phân (ví dụ: 8:30 = 8.5)
    decimal_hour = hour + minute / 60.0
    
    # Thêm nhiễu ngẫu nhiên theo ngày
    day_of_year = point_time.timetuple().tm_yday
    noise_factor = math.sin(day_of_year / 10.0) * 2.0  # Yếu tố nhiễu theo ngày trong năm
    
    # Thêm chu kỳ nhiệt độ dài hạn (theo mùa)
    seasonal_cycle = math.sin(2 * math.pi * day_of_year / 365) * 3.0
    
    # Nhiệt độ có chu kỳ ngày, cao nhất vào buổi trưa
    temp_cycle = math.sin(math.pi * (decimal_hour - 4) / 12) if 4 <= decimal_hour <= 16 else -0.6
    temperature = base_temp + 6.0 * temp_cycle + seasonal_cycle + noise_factor + random.uniform(-0.5, 0.5)
    
    # Chèn các đột biến nhiệt độ ngẫu nhiên (5% xác suất)
    if random.random() < 0.05:
        temperature += random.choice([-2.5, -2.0, -1.5, 1.5, 2.0, 2.5])
    
    # Độ ẩm ngược với nhiệt độ
    humidity = base_humidity - 15.0 * temp_cycle - seasonal_cycle * 0.5 + noise_factor + random.uniform(-1.5, 1.5)
    
    # Thêm mô phỏng thời tiết ngẫu nhiên ảnh hưởng đến độ ẩm
    if random.random() < 0.1:  # 10% xác suất có sự kiện thời tiết đặc biệt
        # Mô phỏng mưa hoặc độ ẩm tăng đột biến
        humidity_spike = random.uniform(5, 15)
        humidity += humidity_spike
        # Khi mưa, nhiệt độ thường giảm
        temperature -= humidity_spike * 0.2
    
    # Áp suất có biến động phức tạp hơn
    pressure_base = 1013.0
    # Chu kỳ ngày
    pressure_daily = 2.0 * math.sin(math.pi * decimal_hour / 12)
    # Chu kỳ nhiều ngày (mô phỏng hệ thống thời tiết)
    pressure_weather = 3.0 * math.sin(2 * math.pi * day_of_year / 30)
    # Nhiễu ngẫu nhiên
    pressure_noise = random.uniform(-0.5, 0.5)
    
    pressure = pressure_base + pressure_daily + pressure_weather + pressure_noise
    
    # Công suất điện theo mẫu ngày làm việc với nhiều đột biến hơn
    if 0 <= decimal_hour < 5:  # Ngủ đêm
        base_power = 50.0
        # Thêm các đột biến ngẫu nhiên (thiết bị tự động hoạt động trong đêm)
        if random.random() < 0.08:  # 8% xác suất
            random_spike = random.uniform(30, 100)
            power = base_power + random_spike + random.uniform(-5, 5)
        else:
            power = base_power + random.uniform(-5, 5)
    elif 5 <= decimal_hour < 8:  # Chuẩn bị đi làm
        # Tăng dần từ 5h-8h với nhiều biến động hơn
        progress = (decimal_hour - 5) / 3
        power = 50.0 + 250.0 * progress + noise_factor * 5 + random.uniform(-15, 25)
        
        # Thêm đỉnh tiêu thụ điện đột ngột khi dùng thiết bị lớn (máy sấy, bình nóng lạnh)
        if random.random() < 0.15:  # 15% xác suất
            power += random.uniform(100, 200)
    elif 8 <= decimal_hour < 17:  # Đi làm/đi học
        base_power = 60.0
        
        # Thêm mô phỏng các thiết bị tự động hoạt động trong ngày
        hour_fraction = decimal_hour - int(decimal_hour)
        # Chu kỳ hoạt động của tủ lạnh (khoảng 15-20 phút mỗi giờ)
        if 0 <= hour_fraction < 0.3:
            fridge_power = 30.0 * math.sin(hour_fraction * 20) if hour_fraction < 0.25 else 0
            power = base_power + fridge_power + random.uniform(-10, 10)
        else:
            power = base_power + random.uniform(-10, 10)
            
        # Thêm các đột biến tiêu thụ điện do thiết bị khác (5% xác suất)
        if random.random() < 0.05:
            power += random.uniform(50, 150)
    elif 17 <= decimal_hour < 22:  # Về nhà buổi tối
        base_power = 280.0
        
        # Mô phỏng nấu cơm tối (19h-20h)
        if 19 <= decimal_hour < 20:
            cooking_factor = 1.0 - abs((decimal_hour - 19.5) * 2)  # Cao nhất lúc 19:30
            power = base_power + cooking_factor * 150 + random.uniform(-20, 20)
        else:
            power = base_power + random.uniform(-30, 30)
            
        # Thêm các đỉnh tiêu thụ ngẫu nhiên (TV, máy giặt, v.v)
        if random.random() < 0.1:
            power += random.uniform(50, 200)
    else:  # Chuẩn bị đi ngủ
        # Giảm dần từ 22h-24h, nhưng có nhiều biến động
        progress = (decimal_hour - 22) / 2
        power = 280.0 - 230.0 * progress + noise_factor * 5 + random.uniform(-20, 20)
        
        # Đỉnh điện cuối ngày khi vệ sinh cá nhân
        if 22 <= decimal_hour < 22.5:
            power += random.uniform(20, 80) * (1 - (decimal_hour - 22) * 2)
    
    # Điều chỉnh theo ngày trong tuần (ví dụ: thứ 6 có xu hướng tiêu thụ điện nhiều hơn)
    weekday = point_time.weekday()
    if weekday == 4:  # Thứ 6
        power *= 1.1  # Tăng 10%
    
    # Ngoài trường power, thêm các thông tin khác để làm phong phú dữ liệu
    return {
        "temperature": round(temperature, 2),
        "humidity": round(max(0, min(100, humidity)), 2),  # Giới hạn 0-100%
        "pressure": round(pressure, 2),
        "power": round(power, 2)
    }

def generate_weekend_pattern(point_time: datetime, base_temp=23.0, base_humidity=62.0) -> Dict[str, float]:
    """
    Tạo mẫu dữ liệu cho ngày cuối tuần (Thứ 7 - Chủ Nhật)
    
    Đặc trưng:
    - Sáng sớm (6-9h): Công suất tăng chậm (ngủ nướng)
    - Cả ngày: Công suất dao động khác nhau (ở nhà)
    - Chiều tối (18-23h): Công suất cao hơn (giải trí)
    
    Args:
        point_time: Thời điểm dữ liệu
        base_temp, base_humidity: Giá trị cơ bản
        
    Returns:
        Dict chứa các giá trị cảm biến
    """
    hour = point_time.hour
    minute = point_time.minute
    
    # Tính giờ dạng thập phân (ví dụ: 8:30 = 8.5)
    decimal_hour = hour + minute / 60.0
    
    # Thêm nhiễu ngẫu nhiên theo ngày
    day_of_year = point_time.timetuple().tm_yday
    noise_factor = math.sin(day_of_year / 12.0) * 2.5  # Yếu tố nhiễu theo ngày trong năm
    
    # Thêm chu kỳ nhiệt độ dài hạn (theo mùa)
    seasonal_cycle = math.sin(2 * math.pi * day_of_year / 365) * 3.5
    
    # Nhiệt độ có chu kỳ ngày, cao nhất vào buổi trưa muộn
    temp_cycle = math.sin(math.pi * (decimal_hour - 5) / 12) if 5 <= decimal_hour <= 17 else -0.6
    temperature = base_temp + 5.5 * temp_cycle + seasonal_cycle + noise_factor + random.uniform(-0.5, 0.5)
    
    # Chèn các đột biến nhiệt độ ngẫu nhiên (cuối tuần nhiều khả năng sử dụng máy lạnh/sưởi)
    if random.random() < 0.08:  # 8% xác suất
        temperature += random.choice([-3.0, -2.5, -2.0, 2.0, 2.5, 3.0])
    
    # Độ ẩm ngược với nhiệt độ nhưng dao động nhiều hơn
    humidity = base_humidity - 12.0 * temp_cycle - seasonal_cycle * 0.6 + noise_factor * 1.2 + random.uniform(-2.0, 2.0)
    
    # Thêm mô phỏng thời tiết ngẫu nhiên ảnh hưởng đến độ ẩm (cuối tuần có xu hướng thay đổi thời tiết)
    if random.random() < 0.12:  # 12% xác suất có sự kiện thời tiết đặc biệt
        # Mô phỏng mưa hoặc độ ẩm tăng đột biến
        humidity_spike = random.uniform(8, 18)
        humidity += humidity_spike
        # Khi mưa, nhiệt độ thường giảm
        temperature -= humidity_spike * 0.3
    
    # Áp suất có biến động phức tạp hơn
    pressure_base = 1014.0
    # Chu kỳ ngày
    pressure_daily = 1.5 * math.sin(math.pi * decimal_hour / 12)
    # Chu kỳ nhiều ngày (mô phỏng hệ thống thời tiết)
    pressure_weather = 3.5 * math.sin(2 * math.pi * day_of_year / 30)
    # Chu kỳ theo giờ (biến động ngắn hạn)
    pressure_hourly = 1.0 * math.sin(2 * math.pi * decimal_hour / 4)
    # Nhiễu ngẫu nhiên
    pressure_noise = random.uniform(-0.6, 0.6)
    
    pressure = pressure_base + pressure_daily + pressure_weather + pressure_hourly + pressure_noise
    
    # Công suất điện theo mẫu cuối tuần với nhiều đột biến hơn
    if 0 <= decimal_hour < 7:  # Ngủ đêm và ngủ nướng
        base_power = 70.0
        
        # Thêm các đột biến ngẫu nhiên (thiết bị tự động hoạt động trong đêm)
        if random.random() < 0.08:  # 8% xác suất
            random_spike = random.uniform(40, 120)
            power = base_power + random_spike + random.uniform(-7, 7)
        else:
            power = base_power + random.uniform(-7, 7)
    elif 7 <= decimal_hour < 10:  # Thức dậy từ từ
        # Tăng dần từ 7h-10h với nhiều biến động
        progress = (decimal_hour - 7) / 3
        power = 70.0 + 180.0 * progress + noise_factor * 8 + random.uniform(-20, 30)
        
        # Mô phỏng nấu ăn sáng cuối tuần
        if 8 <= decimal_hour < 9:
            cooking_factor = 1.0 - abs((decimal_hour - 8.5) * 2)  # Cao nhất lúc 8:30
            power += cooking_factor * 120
    elif 10 <= decimal_hour < 18:  # Ở nhà hoặc đi chơi
        # Dao động không đều trong ngày với nhiều đỉnh tiêu thụ hơn
        hour_sin = math.sin(decimal_hour * 0.8) * 40
        hour_cos = math.cos(decimal_hour * 1.2) * 30
        variation = hour_sin + hour_cos  # Tạo đường cong phức tạp hơn
        
        # Thêm các đột biến tiêu thụ điện (thiết bị điện lớn, nấu ăn, v.v)
        base_power = 200.0
        
        # Hoạt động nấu ăn trưa (11:30-13:30)
        if 11.5 <= decimal_hour < 13.5:
            cooking_intensity = 1.0 - min(1.0, abs(decimal_hour - 12.5))  # Cao nhất lúc 12:30
            power = base_power + variation + cooking_intensity * 150 + random.uniform(-25, 25)
        else:
            power = base_power + variation + random.uniform(-20, 20)
            
        # Thêm các sự kiện tiêu thụ điện đột biến (máy giặt, hút bụi, v.v)
        if random.random() < 0.15:  # 15% xác suất
            power += random.uniform(80, 250)
    elif 18 <= decimal_hour < 23:  # Tối cuối tuần
        base_power = 320.0
        
        # Mô phỏng nấu cơm tối muộn hơn ngày thường (19:30-20:30)
        if 19.5 <= decimal_hour < 20.5:
            cooking_factor = 1.0 - abs((decimal_hour - 20) * 2)  # Cao nhất lúc 20:00
            power = base_power + cooking_factor * 180 + random.uniform(-25, 25)
        else:
            power = base_power + random.uniform(-25, 25)
            
        # Mô phỏng dùng thiết bị giải trí (TV, máy game, máy tính...)
        entertainment_pattern = math.sin((decimal_hour - 18) * 0.8) * 100
        power += entertainment_pattern
        
        # Thêm các đỉnh tiêu thụ ngẫu nhiên (máy lạnh, lò vi sóng, v.v)
        if random.random() < 0.2:  # 20% xác suất cao hơn vào cuối tuần
            power += random.uniform(70, 250)
    else:  # Chuẩn bị đi ngủ
        # Giảm dần từ 23h-24h với dao động phức tạp
        progress = (decimal_hour - 23)
        power = 320.0 - 250.0 * progress + noise_factor * 8 + random.uniform(-20, 20)
        
        # Đỉnh điện cuối ngày khi vệ sinh cá nhân
        if 23 <= decimal_hour < 23.7:
            power += random.uniform(30, 100) * (1 - (decimal_hour - 23) * 1.5)
    
    # Điều chỉnh theo ngày (thứ 7 và chủ nhật có thể khác nhau)
    weekday = point_time.weekday()
    if weekday == 6:  # Chủ nhật
        power *= 0.9  # Giảm 10% (thường ít hoạt động hơn thứ 7)
    
    # Ngoài trường power, thêm các thông tin khác để làm phong phú dữ liệu
    return {
        "temperature": round(temperature, 2),
        "humidity": round(max(0, min(100, humidity)), 2),  # Giới hạn 0-100%
        "pressure": round(pressure, 2),
        "power": round(power, 2)
    }

def generate_template_data(num_days: int = 7, device_id: str = "template_test", start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Tạo dữ liệu giả lập với 2 mẫu template rõ ràng
    
    Args:
        num_days: Số ngày cần tạo dữ liệu
        device_id: ID của thiết bị
        start_date: Thời gian bắt đầu (nếu None sẽ dùng thời gian hiện tại)
        
    Returns:
        Danh sách các điểm dữ liệu
    """
    # Kiểm tra device_id hợp lệ
    if not device_id or device_id == "final":
        logger.warning(f"Device ID '{device_id}' không hợp lệ. Sử dụng 'template_test' thay thế.")
        device_id = "template_test"
    
    data_points = []
    
    # Tính số điểm dữ liệu (5 phút/điểm, 12 điểm/giờ, 288 điểm/ngày)
    points_per_day = 288  # 24 giờ * 12 điểm mỗi giờ
    num_points = num_days * points_per_day
    
    # Đảm bảo số ngày ít nhất là 1
    if num_days < 1:
        num_days = 1
        num_points = points_per_day
    
    # Tạo thời gian bắt đầu
    if start_date is None:
        # Sử dụng thời gian hiện tại (không cần reset về 00:00:00)
        current_time = datetime.now()
        # Làm tròn xuống 5 phút gần nhất
        minute_rounded = current_time.minute - (current_time.minute % 5)
        start_time = current_time.replace(minute=minute_rounded, second=0, microsecond=0)
        logger.info(f"Sử dụng thời gian hiện tại: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Tính thời gian bắt đầu của tuần (thứ 2) nếu dùng thời gian hiện tại
        days_to_monday = start_time.weekday()
        if days_to_monday > 0:
            # Nếu không phải thứ 2, điều chỉnh thời gian bắt đầu
            week_start = start_time - timedelta(days=days_to_monday)
            logger.info(f"Điều chỉnh thời gian bắt đầu về thứ Hai: {week_start.strftime('%Y-%m-%d')}")
        else:
            week_start = start_time
    else:
        # Sử dụng thời gian được chỉ định, giữ nguyên giờ và ngày được chỉ định
        start_time = start_date
        week_start = start_time  # Không điều chỉnh về thứ Hai khi người dùng chỉ định ngày cụ thể
        logger.info(f"Sử dụng thời gian được chỉ định: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    logger.info(f"Bắt đầu tạo dữ liệu từ: {week_start.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Tạo điểm dữ liệu
    for i in range(num_points):
        # Mỗi điểm tăng đúng 5 phút
        point_time = week_start + timedelta(minutes=i * 5)
        
        # Xác định loại ngày
        weekday = point_time.weekday()
        
        # Tạo dữ liệu cảm biến dựa vào loại ngày
        if weekday < 5:  # Thứ 2 - Thứ 6
            sensor_data = generate_workday_pattern(point_time)
        else:  # Thứ 7 - Chủ nhật
            sensor_data = generate_weekend_pattern(point_time)
        
        # Tạo điểm dữ liệu
        data_point = {
            "device_id": device_id,
            "timestamp": point_time,
            "original_data": sensor_data
        }
        
        data_points.append(data_point)
        
        # Hiển thị tiến trình
        if i % points_per_day == 0 or i == 0:
            current_date = point_time.strftime("%Y-%m-%d %H:%M:%S")
            day_name = ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật'][weekday]
            logger.info(f"Đang tạo dữ liệu cho: {current_date} ({day_name})")
    
    logger.info(f"Đã tạo xong {len(data_points)} điểm dữ liệu trong {num_days} ngày")
    return data_points

def save_to_database(data_points: List[Dict[str, Any]], engine) -> int:
    """
    Lưu dữ liệu vào database
    
    Args:
        data_points: Danh sách các điểm dữ liệu
        engine: SQLAlchemy engine
        
    Returns:
        Số lượng bản ghi được lưu thành công
    """
    try:
        # Kết nối đến database
        with engine.connect() as conn:
            # Lấy tất cả các device_id riêng biệt từ dữ liệu
            unique_device_ids = set(point['device_id'] for point in data_points)
            
            # Log thông tin về các device_id được tìm thấy
            logger.info(f"Tìm thấy {len(unique_device_ids)} device_id khác nhau: {', '.join(unique_device_ids)}")
            
            # Kiểm tra và thêm thiết bị vào bảng devices nếu chưa tồn tại
            for device_id in unique_device_ids:
                # Kiểm tra xem device_id đã tồn tại trong bảng devices chưa
                result = conn.execute(
                    text("SELECT device_id FROM devices WHERE device_id = :device_id"),
                    {"device_id": device_id}
                ).fetchone()
                
                # Nếu device_id chưa tồn tại, thêm vào bảng devices
                if not result:
                    device_name = f"Template Test Device {device_id}"
                    device_description = f"Thiết bị giả lập với 2 mẫu template khác nhau"
                    
                    try:
                        conn.execute(text("""
                            INSERT INTO devices (device_id, name, description, created_at)
                            VALUES (:device_id, :name, :description, :created_at)
                        """), {
                            "device_id": device_id,
                            "name": device_name,
                            "description": device_description,
                            "created_at": datetime.now()
                        })
                        conn.commit()
                        logger.info(f"Đã tạo thiết bị mới với device_id: {device_id}")
                    except Exception as e:
                        logger.error(f"Lỗi khi tạo thiết bị {device_id}: {str(e)}")
                        conn.rollback()
                        # Bỏ qua các bản ghi với device_id này
                        data_points = [point for point in data_points if point['device_id'] != device_id]
                        logger.warning(f"Đã loại bỏ {len(data_points)} bản ghi với device_id={device_id}")
            
            # Nếu không còn dữ liệu sau khi lọc, thoát sớm
            if not data_points:
                logger.warning("Không còn điểm dữ liệu nào để lưu sau khi lọc")
                return 0
            
            # Đếm số lượng bản ghi được lưu thành công
            success_count = 0
            
            # Tạo connection trực tiếp để thực hiện câu lệnh SQL thông thường
            import psycopg2
            import psycopg2.extras
            
            # Parse connection string từ engine
            db_url = engine.url
            database_params = {
                'host': db_url.host if db_url.host else 'localhost',
                'port': db_url.port if db_url.port else 5432,
                'database': db_url.database,
                'user': db_url.username,
                'password': db_url.password
            }
            
            # Kết nối trực tiếp qua psycopg2
            pg_conn = psycopg2.connect(**database_params)
            cursor = pg_conn.cursor()
            
            # Kiểm tra lại một lần nữa xem device_id có tồn tại trong bảng devices không
            cursor.execute("SELECT device_id FROM devices")
            existing_devices = {row[0] for row in cursor.fetchall()}
            logger.info(f"Thiết bị hiện có trong database: {', '.join(existing_devices)}")
            
            # Lọc ra các điểm dữ liệu có device_id hợp lệ
            valid_data_points = [point for point in data_points if point['device_id'] in existing_devices]
            
            if len(valid_data_points) < len(data_points):
                logger.warning(f"Loại bỏ {len(data_points) - len(valid_data_points)} điểm dữ liệu có device_id không hợp lệ")
                data_points = valid_data_points
            
            # Sử dụng kích thước lô (batch) để giảm thiểu ảnh hưởng của lỗi
            batch_size = 100
            num_batches = (len(data_points) + batch_size - 1) // batch_size
            
            logger.info(f"Chia dữ liệu thành {num_batches} lô, mỗi lô {batch_size} điểm")
            
            # Xử lý theo lô
            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(data_points))
                batch_points = data_points[start_idx:end_idx]
                
                try:
                    # Bắt đầu giao dịch mới cho mỗi lô
                    batch_success = 0
                    
                    # Chèn dữ liệu vào bảng original_samples
                    for point in batch_points:
                        try:
                            # Chuyển đổi dict sang JSON string
                            original_data_json = json.dumps(point['original_data'])
                            
                            # Thực hiện INSERT trực tiếp bằng psycopg2
                            cursor.execute("""
                                INSERT INTO original_samples (device_id, original_data, timestamp)
                                VALUES (%s, %s::jsonb, %s)
                            """, (
                                point['device_id'],
                                original_data_json,
                                point['timestamp']
                            ))
                            batch_success += 1
                        except Exception as e:
                            # Nếu có lỗi với một điểm dữ liệu cụ thể, ghi nhật ký lỗi nhưng không làm gián đoạn lô
                            logger.error(f"Lỗi khi lưu điểm dữ liệu ở lô {batch_idx+1}/{num_batches}: {str(e)}")
                            # Rollback để hủy bỏ giao dịch đang lỗi và bắt đầu giao dịch mới
                            pg_conn.rollback()
                            # Bắt đầu giao dịch mới ngay lập tức
                            continue
                    
                    # Commit lô này nếu có ít nhất một thao tác thành công
                    if batch_success > 0:
                        pg_conn.commit()
                        success_count += batch_success
                        logger.info(f"Đã lưu thành công {batch_success}/{len(batch_points)} điểm dữ liệu trong lô {batch_idx+1}/{num_batches}")
                except Exception as e:
                    # Xử lý lỗi cho toàn bộ lô
                    logger.error(f"Lỗi xử lý lô {batch_idx+1}/{num_batches}: {str(e)}")
                    pg_conn.rollback()
            
            # Đóng kết nối
            cursor.close()
            pg_conn.close()
            
            logger.info(f"Đã lưu thành công {success_count}/{len(data_points)} điểm dữ liệu vào database")
            return success_count
    except Exception as e:
        logger.error(f"Lỗi khi lưu dữ liệu vào database: {str(e)}")
        return 0

def main():
    """
    Hàm chính chương trình
    """
    parser = argparse.ArgumentParser(description="Tạo dữ liệu giả lập có 2 mẫu template khác nhau và lưu vào bảng original_samples")
    parser.add_argument("--device-id", type=str, default="template_two",
                       help="ID của thiết bị cảm biến (mặc định: 'template_two')")
    parser.add_argument("--start-date", type=str,
                       help="Ngày bắt đầu tạo dữ liệu (định dạng YYYY-MM-DD) - nếu không có sẽ dùng thời gian hiện tại")
    parser.add_argument("--num-days", type=int, default=7,
                       help="Số ngày cần tạo dữ liệu (mặc định: 7)")
    parser.add_argument("--no-save-db", action="store_true",
                       help="Không lưu dữ liệu vào database")
    
    # Parse các đối số
    args = parser.parse_args()
    
    # Kiểm tra và cảnh báo nếu device_id không hợp lệ
    if args.device_id == "final":
        logger.warning("Device ID 'final' không được khuyến nghị và có thể gây lỗi. Đổi thành 'template_two'")
        args.device_id = "template_two"
    
    # Kiểm tra và chuyển đổi ngày bắt đầu nếu được cung cấp
    start_date = None
    if args.start_date:
        try:
            # Khi có tham số --start-date, sử dụng ngày được chỉ định
            raw_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            # Sử dụng giờ hiện tại cho ngày được chỉ định
            current_time = datetime.now()
            start_date = raw_date.replace(
                hour=current_time.hour,
                minute=current_time.minute,
                second=0,
                microsecond=0
            )
            logger.info(f"Sử dụng thời gian bắt đầu được chỉ định: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        except ValueError:
            logger.error(f"Định dạng ngày không hợp lệ: {args.start_date}, cần định dạng YYYY-MM-DD")
            sys.exit(1)
    else:
        # Không có tham số --start-date, sử dụng thời gian hiện tại
        current_time = datetime.now()
        # Làm tròn xuống 5 phút gần nhất
        minute_rounded = current_time.minute - (current_time.minute % 5)
        start_date = current_time.replace(minute=minute_rounded, second=0, microsecond=0)
        logger.info(f"Sử dụng thời gian hiện tại: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Tạo dữ liệu
    data_points = generate_template_data(
        num_days=args.num_days,
        device_id=args.device_id,
        start_date=start_date
    )
    
    # Lưu dữ liệu vào database nếu yêu cầu
    if not args.no_save_db:
        # Thiết lập kết nối database
        engine = setup_database()
        if not engine:
            logger.error("Không thể kết nối đến database! Kết thúc chương trình.")
            sys.exit(1)
            
        # Lưu dữ liệu
        save_to_database(data_points, engine)
    else:
        logger.info("Đã bỏ qua việc lưu dữ liệu vào database theo yêu cầu")
    
    logger.info("Chương trình đã hoàn thành.")

if __name__ == "__main__":
    main()
