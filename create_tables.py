from sqlalchemy import create_engine
from models import Base

def create_tables():
    try:
        # Thông tin kết nối
        DATABASE_URL = "postgresql://postgres:1234@localhost:5433/iot_db"
        
        # Tạo engine để kết nối
        engine = create_engine(DATABASE_URL)
        
        # Tạo tất cả các bảng được định nghĩa trong models.py
        Base.metadata.create_all(bind=engine)
        print("Đã tạo các bảng thành công!")
            
    except Exception as e:
        print("Lỗi khi tạo bảng:", str(e))

if __name__ == "__main__":
    create_tables() 