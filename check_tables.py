from sqlalchemy import create_engine, inspect, text
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_tables():
    try:
        # Thông tin kết nối
        DATABASE_URL = "postgresql://postgres:1234@localhost:5433/iot_db"
        
        # Tạo engine để kết nối
        engine = create_engine(DATABASE_URL)
        
        # Tạo inspector
        inspector = inspect(engine)
        
        # Lấy danh sách các bảng
        tables = inspector.get_table_names()
        logger.info(f"Các bảng trong database: {tables}")
        
        # Kiểm tra chi tiết của từng bảng
        for table_name in tables:
            columns = inspector.get_columns(table_name)
            constraints = inspector.get_foreign_keys(table_name)
            indexes = inspector.get_indexes(table_name)
            
            logger.info(f"\nBảng {table_name}:")
            logger.info("Columns:")
            for column in columns:
                logger.info(f"- {column['name']}: {column['type']}")
            
            logger.info("\nForeign Keys:")
            for fk in constraints:
                logger.info(f"- {fk}")
            
            logger.info("\nIndexes:")
            for idx in indexes:
                logger.info(f"- {idx}")
        
        # Thử truy vấn dữ liệu
        with engine.connect() as connection:
            # Kiểm tra users
            result = connection.execute(text("SELECT * FROM users")).fetchall()
            logger.info(f"\nSố lượng users: {len(result)}")
            
            # Kiểm tra device_configs
            result = connection.execute(text("SELECT * FROM device_configs")).fetchall()
            logger.info(f"Số lượng device_configs: {len(result)}")
            
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra bảng: {str(e)}")

if __name__ == "__main__":
    check_tables() 