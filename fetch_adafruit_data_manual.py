#!/usr/bin/env python3
"""
Script để lấy dữ liệu thủ công từ Adafruit IO cho một ngày cụ thể và lưu vào database PostgreSQL

Cách sử dụng:
    python fetch_adafruit_data_manual.py --date 2023-11-20 --limit 100
    python fetch_adafruit_data_manual.py --date 2023-11-20 --force-reload
    
    Tham số:
    --date: Ngày cần lấy dữ liệu (định dạng YYYY-MM-DD, mặc định: hôm nay)
    --limit: Số lượng bản ghi cần lấy cho mỗi feed (mặc định: 50)
    --force-reload: Bỏ qua kiểm tra trùng lặp, tải lại tất cả dữ liệu
"""

import argparse
import datetime
import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
import requests
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv()

# Cấu hình logging
log_file = 'fetch_adafruit_manual.log'
log_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        log_handler
    ]
)
logger = logging.getLogger(__name__)

# Cấu hình Database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:1234@localhost:5433/iot_db")

# Tạo models
Base = declarative_base()

class SensorData(Base):
    __tablename__ = "sensor_data"
    
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, index=True)
    feed_id = Column(String, index=True)
    value = Column(Float)
    raw_data = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class AdafruitIOManualFetcher:
    def __init__(self, username: str = None, key: str = None, force_reload: bool = False):
        """
        Khởi tạo client để lấy dữ liệu từ Adafruit IO
        
        Args:
            username: Adafruit IO username
            key: Adafruit IO key
            force_reload: Bỏ qua kiểm tra trùng lặp nếu True
        """
        self.username = username or os.getenv("ADAFRUIT_IO_USERNAME")
        self.key = key or os.getenv("ADAFRUIT_IO_KEY")
        self.force_reload = force_reload
        
        if not self.username or not self.key:
            error_msg = "Thiếu thông tin đăng nhập Adafruit IO. Vui lòng cung cấp qua tham số hoặc biến môi trường."
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.base_url = f"https://io.adafruit.com/api/v2/{self.username}"
        self.headers = {
            "X-AIO-Key": self.key,
            "Content-Type": "application/json"
        }
        
        # Kết nối database
        try:
            self.engine = create_engine(DATABASE_URL)
            Base.metadata.create_all(self.engine)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.info(f"Đã kết nối thành công đến database: {DATABASE_URL}")
        except Exception as e:
            logger.error(f"Lỗi kết nối database: {str(e)}")
            raise
    
    def _ensure_device_exists(self, device_id: str = "default") -> bool:
        """
        Đảm bảo thiết bị tồn tại trong database
        """
        try:
            db = self.SessionLocal()
            
            # Kiểm tra xem bảng devices có tồn tại không bằng cách sử dụng inspect
            inspector = inspect(self.engine)
            has_devices_table = inspector.has_table("devices")
            
            if not has_devices_table:
                # Nếu không có bảng devices, tạo bản ghi trực tiếp trong SensorData
                logger.warning("Bảng devices không tồn tại, lưu trực tiếp vào SensorData")
                db.close()
                return True
                
            # Nếu có bảng devices, kiểm tra và tạo thiết bị nếu cần
            from sqlalchemy import text
            result = db.execute(text(f"SELECT id FROM devices WHERE device_id = :device_id"), 
                               {"device_id": device_id}).fetchone()
            
            if not result:
                # Tạo thiết bị mới
                db.execute(text("""
                    INSERT INTO devices (device_id, name, description, created_at) 
                    VALUES (:device_id, :name, :description, NOW())
                """), {
                    "device_id": device_id,
                    "name": f"Feed {device_id}",
                    "description": f"Thiết bị dữ liệu từ Adafruit IO feed: {device_id}"
                })
                db.commit()
                logger.info(f"Đã tạo thiết bị với ID: {device_id}")
            else:
                logger.info(f"Thiết bị {device_id} đã tồn tại trong bảng devices")
            
            db.close()
            return True
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra thiết bị: {str(e)}")
            if 'db' in locals():
                db.close()
            return False
    
    def get_feeds(self) -> List[Dict[str, Any]]:
        """
        Lấy danh sách tất cả feeds từ Adafruit IO
        
        Returns:
            Danh sách các feed
        """
        try:
            url = f"{self.base_url}/feeds"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                feeds = response.json()
                logger.info(f"Đã lấy được {len(feeds)} feeds từ Adafruit IO")
                return feeds
            else:
                logger.error(f"Lỗi khi lấy feeds: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Lỗi khi lấy feeds: {str(e)}")
            return []
    
    def get_feed_data_for_date(self, feed_key: str, date: datetime.date, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu từ một feed cho một ngày cụ thể
        
        Args:
            feed_key: Feed key/ID
            date: Ngày cần lấy dữ liệu
            limit: Số lượng bản ghi tối đa cần lấy
            
        Returns:
            Danh sách dữ liệu từ feed
        """
        try:
            # Tạo thời gian bắt đầu và kết thúc cho ngày đã chọn (UTC)
            start_time = datetime.datetime.combine(date, datetime.time.min).replace(tzinfo=datetime.timezone.utc)
            end_time = datetime.datetime.combine(date, datetime.time.max).replace(tzinfo=datetime.timezone.utc)
            
            start_time_str = start_time.isoformat()
            end_time_str = end_time.isoformat()
            
            url = f"{self.base_url}/feeds/{feed_key}/data"
            params = {
                "limit": limit,
                "start_time": start_time_str,
                "end_time": end_time_str
            }
                
            logger.info(f"Lấy dữ liệu feed {feed_key} từ {start_time_str} đến {end_time_str}")
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Đã lấy được {len(data)} điểm dữ liệu từ feed {feed_key} cho ngày {date}")
                return data
            else:
                logger.error(f"Lỗi khi lấy dữ liệu feed {feed_key}: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu feed: {str(e)}")
            return []
    
    def save_to_database(self, feed_id: str, data_points: List[Dict[str, Any]]) -> int:
        """
        Lưu dữ liệu từ Adafruit IO vào database
        
        Args:
            feed_id: ID của feed
            data_points: Danh sách các điểm dữ liệu
            
        Returns:
            Số lượng bản ghi đã lưu
        """
        try:
            if not data_points:
                logger.info(f"Không có dữ liệu từ feed {feed_id}")
                return 0
                
            self._ensure_device_exists(feed_id)
            db = self.SessionLocal()
            count = 0
            
            for point in data_points:
                try:
                    # Lấy ID để kiểm tra trùng lặp (chỉ khi không có force_reload)
                    if not self.force_reload:
                        point_id = point.get("id")
                        if point_id:
                            # Kiểm tra xem điểm dữ liệu đã tồn tại trong database chưa
                            from sqlalchemy import text
                            result = db.execute(text(f"SELECT id FROM sensor_data WHERE raw_data LIKE '%{point_id}%' LIMIT 1")).fetchone()
                            if result:
                                logger.debug(f"Bỏ qua điểm dữ liệu {point_id} (đã tồn tại trong database)")
                                continue
                
                    # Lấy giá trị và chuyển đổi sang số
                    value_str = point.get("value", "0")
                    try:
                        value = float(value_str)
                    except (ValueError, TypeError):
                        value = 0.0
                    
                    # Xử lý timestamp
                    timestamp_str = point.get("created_at")
                    if timestamp_str:
                        try:
                            timestamp = datetime.datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                        except (ValueError, TypeError):
                            timestamp = datetime.datetime.utcnow()
                    else:
                        timestamp = datetime.datetime.utcnow()
                    
                    # Tạo bản ghi mới
                    new_data = SensorData(
                        device_id=feed_id,
                        feed_id=feed_id,
                        value=value,
                        raw_data=json.dumps(point),
                        timestamp=timestamp
                    )
                    
                    db.add(new_data)
                    count += 1
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý điểm dữ liệu: {str(e)}")
            
            if count > 0:
                db.commit()
                logger.info(f"Đã lưu {count} điểm dữ liệu mới vào database từ feed {feed_id}")
            
            db.close()
            return count
        except Exception as e:
            logger.error(f"Lỗi khi lưu vào database: {str(e)}")
            if 'db' in locals():
                db.rollback()
                db.close()
            return 0
    
    def fetch_and_save_for_date(self, date: datetime.date, limit: int = 50) -> int:
        """
        Lấy dữ liệu từ tất cả feeds cho một ngày cụ thể và lưu vào database
        
        Args:
            date: Ngày cần lấy dữ liệu
            limit: Số lượng bản ghi cần lấy cho mỗi feed
            
        Returns:
            Tổng số bản ghi đã lưu
        """
        total_saved = 0
        
        logger.info(f"Bắt đầu lấy dữ liệu cho ngày {date} với tối đa {limit} bản ghi cho mỗi feed")
        
        # Lấy danh sách tất cả feeds
        feeds = self.get_feeds()
        if not feeds:
            logger.warning("Không thể lấy danh sách feeds. Vui lòng kiểm tra kết nối hoặc thông tin đăng nhập Adafruit IO.")
            return 0
            
        logger.info(f"Tìm thấy {len(feeds)} feeds từ Adafruit IO")
        for feed in feeds:
            feed_key = feed.get("key")
            feed_name = feed.get("name", "Không có tên")
            
            if feed_key:
                logger.info(f"Đang xử lý feed: {feed_name} (key: {feed_key})")
                data = self.get_feed_data_for_date(feed_key, date, limit)
                saved = self.save_to_database(feed_key, data)
                total_saved += saved
                logger.info(f"Đã lưu {saved}/{len(data)} bản ghi từ feed {feed_name}")
                
                # Tạm dừng một chút giữa các request để tránh giới hạn rate
                import time
                time.sleep(0.5)
        
        return total_saved

def main():
    parser = argparse.ArgumentParser(description="Lấy dữ liệu thủ công từ Adafruit IO cho một ngày cụ thể")
    parser.add_argument("--date", type=str, help="Ngày cần lấy dữ liệu (định dạng YYYY-MM-DD, mặc định: hôm nay)")
    parser.add_argument("--limit", type=int, default=50, help="Số lượng bản ghi cần lấy cho mỗi feed (mặc định: 50)")
    parser.add_argument("--force-reload", action="store_true", help="Bỏ qua kiểm tra trùng lặp, tải lại tất cả dữ liệu")
    
    args = parser.parse_args()
    
    # Xử lý tham số ngày
    target_date = None
    if args.date:
        try:
            target_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Định dạng ngày không hợp lệ: {args.date}. Vui lòng sử dụng định dạng YYYY-MM-DD.")
            sys.exit(1)
    else:
        target_date = datetime.datetime.now().date()
    
    logger.info(f"Đang lấy dữ liệu cho ngày: {target_date}")
    if args.force_reload:
        logger.info("Chế độ FORCE RELOAD: Bỏ qua kiểm tra trùng lặp, tải lại tất cả dữ liệu")
    
    try:
        fetcher = AdafruitIOManualFetcher(force_reload=args.force_reload)
        total_saved = fetcher.fetch_and_save_for_date(target_date, args.limit)
        
        logger.info(f"Hoàn thành: Đã lưu tổng cộng {total_saved} bản ghi mới vào database")
        
        # In đánh dấu phân cách để dễ theo dõi trong log
        print("="*80)
        print(f"ĐÃ LƯU {total_saved} BẢN GHI MỚI TỪ ADAFRUIT IO CHO NGÀY {target_date}")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 