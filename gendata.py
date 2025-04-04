#!/usr/bin/env python3
"""
Script để tạo dữ liệu giả lập và lưu vào bảng original_samples trong database.

Cách sử dụng:
    python3 gendata.py [--num-points NUM_POINTS] [--device-id DEVICE_ID] [--start-date YYYY-MM-DD] [--start-hour HOUR] [--start-minute MINUTE]
    
Tham số:
    --num-points: Số lượng điểm dữ liệu cần tạo (mặc định: 100)
    --device-id: ID của thiết bị cảm biến (mặc định: "sensor_sim")
    --start-date: Ngày bắt đầu tạo dữ liệu (định dạng YYYY-MM-DD)
    --start-hour: Giờ bắt đầu trong ngày (0-23, mặc định: 10)
    --start-minute: Phút bắt đầu (0-59, mặc định: 0)
    
Chú ý:
    - Mỗi điểm dữ liệu cách nhau 5 phút
    - Nếu số lượng điểm lớn (>288), dữ liệu sẽ tự động chuyển sang ngày tiếp theo
    - Ví dụ: 288 điểm x 5 phút = 1440 phút = 24 giờ (1 ngày)
"""

import os
import sys
import logging
import json
import argparse
import random
import math
import time
import datetime
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, Float, JSON, MetaData
from dotenv import load_dotenv

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Tải biến môi trường
load_dotenv()

# Cấu hình kết nối database
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

def generate_sensor_data(num_points: int = 100, device_id: str = "sensor_sim", start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Tạo dữ liệu giả lập từ cảm biến với mỗi điểm cách nhau đúng 5 phút
    
    Dữ liệu được mô phỏng dựa trên các yếu tố thực tế như:
    - Thời gian trong ngày (giờ): Nhiệt độ, độ ẩm và tiêu thụ điện thay đổi theo chu kỳ ngày
    - Ngày trong tuần: Các ngày làm việc và cuối tuần có mẫu tiêu thụ khác nhau
    - Tháng và mùa trong năm: Mỗi mùa có đặc điểm nhiệt độ, độ ẩm, và tiêu thụ điện khác nhau
    - Các sự kiện đặc biệt/ngày lễ: Tiêu thụ điện có thể tăng đột biến vào các dịp lễ
    
    Args:
        num_points: Số lượng điểm dữ liệu cần tạo
        device_id: ID của thiết bị
        start_date: Thời gian bắt đầu (nếu None sẽ dùng mặc định là 10:00:00 của ngày hiện tại)
        
    Returns:
        Danh sách các điểm dữ liệu
    """
    data_points = []
    
    # Đảm bảo số điểm ít nhất là 2
    if num_points <= 1:
        num_points = 2
    
    # Tạo thời gian bắt đầu
    if start_date is None:
        # Sử dụng mốc thời gian mặc định là 10:00 sáng ngày hiện tại
        today = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0)
        start_time = today
    else:
        # Sử dụng thời gian được chỉ định
        start_time = start_date.replace(microsecond=0)
    
    # Lấy thông tin ngày/tháng/mùa ban đầu
    initial_month = start_time.month
    initial_weekday = start_time.weekday()  # 0 = Thứ 2, 6 = Chủ nhật
    weekday_name = ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật'][initial_weekday]
    initial_day = start_time.day
    
    # Định nghĩa các mùa theo tháng
    seasons = {
        1: "Đông", 2: "Đông", 3: "Xuân",
        4: "Xuân", 5: "Xuân", 6: "Hè",
        7: "Hè", 8: "Hè", 9: "Thu",
        10: "Thu", 11: "Thu", 12: "Đông"
    }
    
    initial_season = seasons[initial_month]
    
    # Định nghĩa giá trị cơ bản của các đại lượng theo tháng
    # Format: (nhiệt độ, độ ẩm, áp suất, công suất điện)
    monthly_base_values = {
        1: (15.0, 70.0, 1015.0, 200.0),  # Tháng 1 - Đông
        2: (16.0, 68.0, 1016.0, 190.0),  # Tháng 2 - Đông
        3: (18.0, 65.0, 1014.0, 180.0),  # Tháng 3 - Xuân
        4: (22.0, 60.0, 1012.0, 170.0),  # Tháng 4 - Xuân
        5: (26.0, 55.0, 1010.0, 190.0),  # Tháng 5 - Xuân
        6: (30.0, 50.0, 1008.0, 220.0),  # Tháng 6 - Hè
        7: (32.0, 48.0, 1006.0, 250.0),  # Tháng 7 - Hè
        8: (31.0, 52.0, 1007.0, 240.0),  # Tháng 8 - Hè
        9: (27.0, 58.0, 1009.0, 210.0),  # Tháng 9 - Thu
        10: (23.0, 65.0, 1011.0, 190.0), # Tháng 10 - Thu
        11: (19.0, 68.0, 1013.0, 200.0), # Tháng 11 - Thu
        12: (16.0, 72.0, 1015.0, 210.0)  # Tháng 12 - Đông
    }
    
    # Biên độ nhiệt dao động trong ngày theo tháng
    month_temp_amplitude = {
        1: 5.0, 2: 6.0, 3: 7.0, 4: 8.0, 5: 8.5, 6: 9.0,
        7: 9.0, 8: 8.5, 9: 8.0, 10: 7.0, 11: 6.0, 12: 5.0
    }
    
    # Biên độ độ ẩm dao động trong ngày theo tháng
    month_humidity_amplitude = {
        1: 10.0, 2: 12.0, 3: 15.0, 4: 18.0, 5: 20.0, 6: 25.0,
        7: 28.0, 8: 25.0, 9: 20.0, 10: 18.0, 11: 15.0, 12: 10.0
    }
    
    # Định nghĩa các ngày lễ đặc biệt (tháng, ngày)
    holidays = [
        (1, 1),   # Tết Dương lịch
        (4, 30),  # Ngày giải phóng miền Nam
        (5, 1),   # Quốc tế Lao động
        (9, 2),   # Quốc khánh Việt Nam
    ]
    
    # Khởi tạo giá trị ban đầu dựa trên tháng
    base_temp, base_humidity, base_pressure, base_power = monthly_base_values[initial_month]
    
    # Lưu giá trị ngày/tháng/mùa hiện tại
    current_day = initial_day
    current_month = initial_month
    current_season = initial_season
    
    for i in range(num_points):
        # Tạo thời gian cho điểm dữ liệu hiện tại, mỗi điểm tăng đúng 5 phút
        point_time = start_time + timedelta(minutes=i * 5)
        
        # Kiểm tra các thay đổi về ngày/tháng/mùa
        new_day = point_time.day
        new_month = point_time.month
        new_season = seasons[new_month]
        new_weekday = point_time.weekday()
        
        # Kiểm tra nếu đang là ngày lễ
        is_holiday = (new_month, new_day) in holidays
        
        # Kiểm tra nếu đã sang ngày mới
        day_changed = new_day != current_day
        
        # Kiểm tra nếu đã sang tháng mới
        month_changed = new_month != current_month
        
        # Kiểm tra nếu đã sang mùa mới
        season_changed = new_season != current_season
        
        # Cập nhật giá trị cơ bản nếu có thay đổi về tháng
        if month_changed:
            base_temp, base_humidity, base_pressure, base_power = monthly_base_values[new_month]
            logger.info(f"Chuyển sang tháng {new_month}, cập nhật giá trị cơ bản: temp={base_temp}, humidity={base_humidity}, pressure={base_pressure}, power={base_power}")
            
        # Cập nhật các thông số hiện tại
        current_day = new_day
        current_month = new_month
        current_season = new_season
        
        # Format timestamp theo ISO 8601 không có phần microsecond
        timestamp = point_time.strftime("%Y-%m-%dT%H:%M:%S")
        
        # Tính giờ trong ngày (0-23) để tạo xu hướng trong ngày
        hour = point_time.hour
        
        # ==== Tạo dữ liệu nhiệt độ ====
        # Nhiệt độ thấp nhất vào sáng sớm (4-5h), cao nhất vào buổi trưa (13-14h)
        # Điều chỉnh giờ cao điểm nhiệt độ theo mùa
        peak_hour = 14 if current_season in ["Xuân", "Hè"] else 13  # Mùa hè nắng kéo dài hơn
        hour_factor_temp = math.sin(math.pi * (hour - 5) / 12) if 5 <= hour <= (peak_hour + 3) else -0.5
        
        # Biên độ nhiệt trong ngày thay đổi theo tháng
        temp_daily_amplitude = month_temp_amplitude[current_month]
        temp_daily_cycle = temp_daily_amplitude * hour_factor_temp
        
        # Thêm nhiễu ngẫu nhiên cho nhiệt độ, biên độ nhiễu cũng thay đổi theo mùa
        temp_noise_amplitude = 1.2 if current_season in ["Xuân", "Thu"] else 0.8  # Giao mùa nhiễu lớn hơn
        temp_noise = random.uniform(-temp_noise_amplitude, temp_noise_amplitude)
        
        # Giá trị nhiệt độ cuối cùng
        temperature = base_temp + temp_daily_cycle + temp_noise
        
        # ==== Tạo dữ liệu độ ẩm ====
        # Độ ẩm thường ngược với nhiệt độ (cao vào buổi sáng và tối, thấp vào buổi trưa)
        # Biên độ dao động độ ẩm thay đổi theo tháng
        humidity_daily_amplitude = month_humidity_amplitude[current_month]
        humidity_daily_cycle = -humidity_daily_amplitude * hour_factor_temp
        
        # Thêm nhiễu ngẫu nhiên cho độ ẩm
        humid_noise_amplitude = 3.0 if current_season in ["Hè", "Thu"] else 2.0  # Mùa mưa nhiễu lớn hơn
        humid_noise = random.uniform(-humid_noise_amplitude, humid_noise_amplitude)
        
        # Giá trị độ ẩm cuối cùng
        humidity = base_humidity + humidity_daily_cycle + humid_noise
        
        # ==== Tạo dữ liệu áp suất ====
        # Áp suất thay đổi theo mùa và giờ trong ngày
        pressure_cycle_amplitude = 3.0 if current_season in ["Xuân", "Thu"] else 2.0  # Giao mùa biến động lớn hơn
        pressure_cycle = pressure_cycle_amplitude * math.sin(math.pi * hour / 12 + math.pi/6)
        
        # Thêm nhiễu ngẫu nhiên cho áp suất
        pressure_noise_amplitude = 0.5 if current_season in ["Xuân", "Thu"] else 0.3
        pressure_noise = random.uniform(-pressure_noise_amplitude, pressure_noise_amplitude)
        
        # Giá trị áp suất cuối cùng
        pressure = base_pressure + pressure_cycle + pressure_noise
        
        # ==== Tạo dữ liệu tiêu thụ điện ====
        # Tiêu thụ điện thay đổi theo mùa, giờ trong ngày, và ngày trong tuần
        
        # Hệ số mùa cho tiêu thụ điện (cao hơn vào mùa hè và mùa đông)
        season_power_factor = 1.0
        if current_season == "Hè":
            season_power_factor = 1.3  # Sử dụng điều hòa làm mát
        elif current_season == "Đông":
            season_power_factor = 1.2  # Sử dụng thiết bị sưởi
        
        # Hệ số tiêu thụ buổi trưa - cao hơn vào mùa hè do sử dụng điều hòa
        lunch_peak_start = 11
        lunch_peak_end = 14
        power_lunch_factor = 0.0
        
        if lunch_peak_start <= hour <= lunch_peak_end:
            power_lunch_factor = 0.7 + 0.3 * math.sin(math.pi * (hour - lunch_peak_start) / (lunch_peak_end - lunch_peak_start))
            # Mùa hè buổi trưa tiêu thụ cao hơn do điều hòa
            if current_season == "Hè":
                power_lunch_factor *= 1.5
        
        # Hệ số tiêu thụ buổi tối - cao nhất vào tối muộn (18-21h)
        evening_peak_start = 18
        evening_peak_end = 22
        power_evening_factor = 0.0
        
        if evening_peak_start <= hour <= evening_peak_end:
            power_evening_factor = 0.8 + 0.2 * math.sin(math.pi * (hour - evening_peak_start) / (evening_peak_end - evening_peak_start))
            # Mùa đông buổi tối tiêu thụ cao hơn do sưởi ấm
            if current_season == "Đông":
                power_evening_factor *= 1.5
        
        # Hệ số cuối tuần - thấp hơn vào ngày trong tuần, cao hơn vào cuối tuần
        weekend_factor = 1.0
        if new_weekday < 5:  # Ngày trong tuần (thứ 2 đến thứ 6)
            weekend_factor = 0.8
        else:  # Cuối tuần (thứ 7, chủ nhật)
            weekend_factor = 1.2
        
        # Hệ số giờ thấp điểm (ban đêm và sáng sớm)
        power_base_factor = 0.5
        
        if 0 <= hour <= 5:
            power_base_factor = 0.1 + 0.05 * hour  # Tăng dần từ khuya đến sáng
            # Mùa đông tiêu thụ điện cao hơn vào ban đêm do sưởi
            if current_season == "Đông":
                power_base_factor += 0.1
        elif 6 <= hour <= 10:
            # Vào ngày trong tuần, buổi sáng tiêu thụ nhiều hơn (chuẩn bị đi làm)
            if new_weekday < 5:
                power_base_factor = 0.3 + 0.15 * math.sin(math.pi * (hour - 6) / 4)
                if current_season == "Đông":
                    power_base_factor += 0.1  # Mùa đông buổi sáng tiêu thụ cao hơn (sưởi, nước nóng)
            else:
                # Cuối tuần buổi sáng thường tiêu thụ ít hơn (ngủ nướng)
                power_base_factor = 0.2 + 0.05 * math.sin(math.pi * (hour - 6) / 4)
        elif 15 <= hour <= 17:
            # Giờ tan học, tan làm
            power_base_factor = 0.4
            if new_weekday < 5:  # Ngày trong tuần
                power_base_factor += 0.1  # Tăng lên do người về nhà
        elif 23 == hour:
            power_base_factor = 0.2
            if is_holiday:
                power_base_factor += 0.3  # Các ngày lễ, đêm khuya vẫn hoạt động nhiều
        
        # Hiệu ứng ngày lễ đặc biệt
        holiday_factor = 1.5 if is_holiday else 1.0
            
        # Kết hợp các hệ số
        power_factor = max(power_base_factor, power_lunch_factor, power_evening_factor)
        
        # Áp dụng các hệ số nhân
        power_factor *= weekend_factor * season_power_factor * holiday_factor
        
        # Thêm nhiễu ngẫu nhiên (±10% giá trị)
        power_noise = random.uniform(-0.1, 0.1) * base_power * power_factor
        
        # Giá trị công suất điện cuối cùng
        power = base_power * power_factor + power_noise
        
        # Tạo điểm dữ liệu
        data_point = {
            "device_id": device_id,
            "timestamp": timestamp,
            "readings": {
                "temperature": round(temperature, 3),
                "humidity": round(humidity, 3),
                "pressure": round(pressure, 3),
                "power": round(power, 3)
            }
        }
        
        # Thêm cờ đánh dấu ngày lễ
        if is_holiday:
            data_point["readings"]["is_holiday"] = True
        
        data_points.append(data_point)
        
        # Hiển thị thông tin về việc chuyển ngày
        if day_changed and i > 0:
            logger.info(f"Chuyển sang ngày mới: {point_time.strftime('%Y-%m-%d')}")
    
    # Thống kê cuối cùng
    season_count = {}
    month_count = {}
    day_count = {}
    
    for point in data_points:
        ts = datetime.fromisoformat(point["timestamp"].replace("Z", "+00:00"))
        month = ts.month
        season = seasons[month]
        day = ts.strftime("%Y-%m-%d")
        
        if month not in month_count:
            month_count[month] = 0
        month_count[month] += 1
        
        if season not in season_count:
            season_count[season] = 0
        season_count[season] += 1
        
        if day not in day_count:
            day_count[day] = 0
        day_count[day] += 1
    
    # In thống kê phân bố dữ liệu theo ngày, tháng và mùa
    logger.info(f"Phân bố dữ liệu theo mùa: {season_count}")
    logger.info(f"Phân bố dữ liệu theo tháng: {month_count}")
    logger.info(f"Phân bố dữ liệu theo ngày: {day_count}")
    
    logger.info(f"Đã tạo {len(data_points)} điểm dữ liệu giả lập cho thiết bị {device_id}")
    return data_points

def save_original_data(data_points: List[Dict[str, Any]], engine) -> int:
    """
    Lưu dữ liệu gốc vào bảng original_samples
    
    Args:
        data_points: Danh sách các điểm dữ liệu cần lưu
        engine: SQLAlchemy engine kết nối đến database
        
    Returns:
        Số lượng điểm dữ liệu đã lưu thành công
    """
    try:
        # Kết nối đến database
        with engine.connect() as conn:
            # Chuẩn bị metadata cho bảng
            metadata = MetaData()
            metadata.reflect(bind=engine)
            
            # Lấy bảng devices và original_samples
            devices = Table('devices', metadata, autoload_with=engine)
            original_samples = Table('original_samples', metadata, autoload_with=engine)
            
            # Lấy tất cả các device_id riêng biệt từ dữ liệu
            unique_device_ids = set(point['device_id'] for point in data_points)
            
            # Kiểm tra và thêm thiết bị vào bảng devices nếu chưa tồn tại
            for device_id in unique_device_ids:
                # Kiểm tra xem device_id đã tồn tại trong bảng devices chưa
                result = conn.execute(
                    text("SELECT device_id FROM devices WHERE device_id = :device_id"),
                    {"device_id": device_id}
                ).fetchone()
                
                # Nếu device_id chưa tồn tại, thêm vào bảng devices
                if not result:
                    device_name = f"Sensor {device_id}"
                    device_description = f"Auto-generated sensor device {device_id}"
                    
                    conn.execute(
                        devices.insert().values(
                            device_id=device_id,
                            name=device_name,
                            description=device_description,
                            created_at=datetime.now()
                        )
                    )
                    logger.info(f"Đã tạo thiết bị mới với device_id: {device_id}")
            
            # Chuẩn bị dữ liệu để chèn vào bảng
            records = []
            for point in data_points:
                record = {
                    'device_id': point['device_id'],
                    'original_data': point['readings'],  # Lưu trực tiếp đối tượng Python, SQLAlchemy sẽ chuyển đổi thành JSONB
                    'timestamp': datetime.fromisoformat(point['timestamp'])
                }
                records.append(record)
            
            # Chèn dữ liệu vào bảng
            if records:
                result = conn.execute(original_samples.insert(), records)
                conn.commit()
                logger.info(f"Đã lưu {len(records)} điểm dữ liệu vào bảng original_samples")
                return len(records)
            else:
                logger.warning("Không có dữ liệu nào được lưu vào bảng original_samples")
                return 0
                
    except Exception as e:
        logger.error(f"Lỗi khi lưu dữ liệu gốc: {str(e)}")
        return 0

def main():
    """
    Hàm chính để xử lý tham số dòng lệnh và thực thi chương trình
    """
    parser = argparse.ArgumentParser(description="Tạo dữ liệu giả lập và lưu vào bảng original_samples")
    
    # Thêm các tham số dòng lệnh
    parser.add_argument("--num-points", type=int, default=100, 
                        help="Số lượng điểm dữ liệu cần tạo (mặc định: 100)")
    
    parser.add_argument("--device-id", type=str, default="sensor_sim", 
                        help="ID của thiết bị cảm biến (mặc định: 'sensor_sim')")
    
    parser.add_argument("--start-date", type=str, 
                        help="Ngày bắt đầu tạo dữ liệu (định dạng YYYY-MM-DD)")
    
    parser.add_argument("--start-hour", type=int, default=10,
                        help="Giờ bắt đầu trong ngày (0-23, mặc định: 10)")
    
    parser.add_argument("--start-minute", type=int, default=0,
                        help="Phút bắt đầu (0-59, mặc định: 0)")
    
    # Parse các đối số
    args = parser.parse_args()
    
    # Kiểm tra giờ và phút
    if args.start_hour < 0 or args.start_hour > 23:
        logger.error(f"Giờ bắt đầu không hợp lệ: {args.start_hour}, cần từ 0-23")
        sys.exit(1)
        
    if args.start_minute < 0 or args.start_minute > 59:
        logger.error(f"Phút bắt đầu không hợp lệ: {args.start_minute}, cần từ 0-59")
        sys.exit(1)
    
    # Chuyển đổi ngày bắt đầu nếu được cung cấp
    start_date = None
    if args.start_date:
        try:
            # Tạo datetime với ngày được chỉ định và giờ/phút từ tham số
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(
                hour=args.start_hour, minute=args.start_minute, second=0, microsecond=0)
            logger.info(f"Sử dụng thời gian bắt đầu: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        except ValueError:
            logger.error(f"Định dạng ngày không hợp lệ: {args.start_date}, cần định dạng YYYY-MM-DD")
            sys.exit(1)
    else:
        # Nếu không có ngày bắt đầu, sử dụng ngày hiện tại với giờ/phút được chỉ định
        start_date = datetime.now().replace(
            hour=args.start_hour, minute=args.start_minute, second=0, microsecond=0)
        logger.info(f"Không có ngày bắt đầu được chỉ định, sử dụng ngày hiện tại: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Tính thời gian kết thúc dự kiến
    end_time = start_date + timedelta(minutes=(args.num_points - 1) * 5)
    total_days = (end_time.date() - start_date.date()).days + 1
    
    # In thông tin các tham số
    logger.info(f"Số lượng điểm dữ liệu cần tạo: {args.num_points}")
    logger.info(f"ID thiết bị: {args.device_id}")
    logger.info(f"Khoảng thời gian dữ liệu: {start_date.strftime('%Y-%m-%d %H:%M')} đến {end_time.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"Tổng số ngày: {total_days} (mỗi điểm cách nhau 5 phút)")
    
    # Thiết lập kết nối database
    engine = setup_database()
    if not engine:
        logger.error("Không thể kết nối đến database, kết thúc chương trình!")
        sys.exit(1)
    
    # Tạo dữ liệu giả lập
    logger.info("Bắt đầu tạo dữ liệu giả lập...")
    data_points = generate_sensor_data(
        num_points=args.num_points,
        device_id=args.device_id,
        start_date=start_date
    )
    
    # Lưu dữ liệu vào database
    logger.info("Bắt đầu lưu dữ liệu vào database...")
    saved_count = save_original_data(data_points, engine)
    
    # Kết thúc chương trình
    logger.info(f"Đã tạo và lưu {saved_count}/{args.num_points} điểm dữ liệu giả lập vào database")
    logger.info("Chương trình kết thúc.")

if __name__ == "__main__":
    main()
