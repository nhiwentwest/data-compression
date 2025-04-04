from sqlalchemy import create_engine, text
import models
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def recreate_tables():
    try:
        # Thông tin kết nối
        DATABASE_URL = "postgresql://postgres:1234@localhost:5433/iot_db"
        
        # Tạo engine để kết nối
        engine = create_engine(DATABASE_URL)
        
        # Xóa tất cả các bảng cũ
        logger.info("Dropping all tables...")
        models.Base.metadata.drop_all(bind=engine)
        logger.info("All tables dropped successfully")
        
        # Tạo lại các bảng mới
        logger.info("Creating new tables...")
        models.Base.metadata.create_all(bind=engine)
        logger.info("All tables created successfully")
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == "__main__":
    recreate_tables() 