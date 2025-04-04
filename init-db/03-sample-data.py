from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import datetime
import random

# Thêm thư mục gốc vào sys.path để import models
sys.path.append('/app')

# Kết nối đến database
engine = create_engine('postgresql://postgres:1234@db:5432/iot_db')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()

# Import models
from models import User, DeviceConfig, Device, SensorData

try:
    # Kiểm tra xem đã có dữ liệu chưa
    existing_users = db.query(User).count()
    existing_devices = db.query(Device).count()
    
    if existing_users > 0 and existing_devices > 0:
        print("Dữ liệu mẫu đã tồn tại, bỏ qua bước tạo dữ liệu mẫu.")
        sys.exit(0)
    
    # Tạo user mẫu
    sample_user = User(
        username="sample_user",
        email="sample@example.com",
        hashed_password="$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW"  # "password"
    )
    db.add(sample_user)
    db.commit()
    db.refresh(sample_user)
    
    # Tạo device mẫu
    sample_devices = [
        Device(device_id="device001", name="Temperature Sensor", description="Living room temperature sensor"),
        Device(device_id="device002", name="Humidity Sensor", description="Bathroom humidity sensor"),
        Device(device_id="device003", name="Light Sensor", description="Garden light sensor")
    ]
    
    for device in sample_devices:
        db.add(device)
    
    db.commit()
    
    # Tạo device config mẫu
    sample_config = DeviceConfig(
        user_id=sample_user.id,
        device_name="device001",
        config_data={"threshold": 25, "interval": 60}
    )
    db.add(sample_config)
    db.commit()
    
    # Tạo dữ liệu cảm biến mẫu
    now = datetime.datetime.utcnow()
    
    # Tạo dữ liệu cho 7 ngày gần đây
    for device in sample_devices:
        feed_id = f"{device.name.lower().replace(' ', '_')}"
        
        for i in range(7*24):  # 7 ngày, mỗi giờ 1 bản ghi
            timestamp = now - datetime.timedelta(hours=i)
            
            if "Temperature" in device.name:
                value = round(random.uniform(18, 30), 1)  # Nhiệt độ từ 18-30°C
            elif "Humidity" in device.name:
                value = round(random.uniform(40, 80), 1)  # Độ ẩm từ 40-80%
            else:
                value = round(random.uniform(0, 1000), 1)  # Cường độ ánh sáng
            
            sensor_data = SensorData(
                device_id=device.device_id,
                feed_id=feed_id,
                value=value,
                timestamp=timestamp
            )
            db.add(sensor_data)
    
    db.commit()
    print("Đã tạo dữ liệu mẫu thành công!")

except Exception as e:
    db.rollback()
    print(f"Lỗi khi tạo dữ liệu mẫu: {str(e)}")
    
finally:
    db.close() 