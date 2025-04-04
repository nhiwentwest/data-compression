from sqlalchemy import create_engine, inspect
import os
import sys
import time

# Đợi database khởi động hoàn tất
time.sleep(5)

# Thêm thư mục gốc vào sys.path để import models
sys.path.append('/app')

# Kết nối đến database
engine = create_engine('postgresql://postgres:1234@db:5432/iot_db')

# Import tất cả các models
from models import Base, User, DeviceConfig, Device, SensorData

# Kiểm tra xem các bảng đã tồn tại chưa
inspector = inspect(engine)
existing_tables = inspector.get_table_names()
print(f"Các bảng hiện có: {existing_tables}")

# Tạo tất cả các bảng nếu chưa tồn tại
Base.metadata.create_all(bind=engine)

# Kiểm tra lại sau khi tạo
inspector = inspect(engine)
tables_after_creation = inspector.get_table_names()
print(f"Các bảng sau khi tạo: {tables_after_creation}")

# Kiểm tra xem tất cả các bảng cần thiết đã được tạo chưa
required_tables = ['users', 'device_configs', 'devices', 'sensor_data']
missing_tables = [table for table in required_tables if table not in tables_after_creation]

if missing_tables:
    print(f"CẢNH BÁO: Các bảng sau chưa được tạo: {missing_tables}")
else:
    print("Tất cả các bảng cần thiết đã được tạo thành công!") 