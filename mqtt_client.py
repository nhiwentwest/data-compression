#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module cung cấp lớp MQTTClient để kết nối tới Adafruit IO MQTT broker.
"""

import paho.mqtt.client as mqtt
import ssl
import logging
import json
import time
import socket
from database import SessionLocal
from config import settings
import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MQTTClient:
    """
    Lớp MQTTClient cung cấp các phương thức để kết nối, xuất bản và theo dõi các feeds trên Adafruit IO.
    """
    
    def __init__(self):
        """
        Khởi tạo đối tượng MQTT client với cấu hình từ settings.
        """
        self.client = mqtt.Client(client_id=f"fastapi_client_{int(time.time())}")
        self.username = settings.MQTT_USERNAME or settings.ADAFRUIT_IO_USERNAME
        self.password = settings.MQTT_PASSWORD or settings.ADAFRUIT_IO_KEY
        self.host = settings.MQTT_HOST
        self.port = settings.MQTT_PORT
        self.topic = settings.MQTT_TOPIC or f"{self.username}/feeds/#"
        self.use_ssl = settings.MQTT_SSL
        self.is_connected = False
        
        # Thiết lập callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        
        # Thiết lập thông tin xác thực
        self.client.username_pw_set(self.username, self.password)
        
        logger.info(f"MQTT Client initialized with host={self.host}, port={self.port}, username={self.username}")
    
    def connect(self):
        """
        Kết nối tới MQTT broker.
        """
        try:
            # Thử ping host trước để xác nhận kết nối mạng
            try:
                socket.gethostbyname(self.host)
                logger.info(f"Đã phân giải tên miền thành công: {self.host}")
            except socket.gaierror as e:
                logger.error(f"Không thể phân giải tên miền {self.host}: {str(e)}")
                return False
                
            if self.use_ssl:
                self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
                logger.info("SSL/TLS enabled for MQTT connection")
            
            logger.info(f"Connecting to MQTT broker at {self.host}:{self.port}")
            self.client.connect(self.host, self.port, keepalive=60)
            
            # Bắt đầu vòng lặp network trong một thread riêng biệt
            self.client.loop_start()
            logger.info("MQTT client loop started")
            
            # Đợi kết nối được thiết lập
            retries = 0
            while not self.is_connected and retries < 5:
                logger.info("Waiting for connection to be established...")
                time.sleep(1)
                retries += 1
            
            if not self.is_connected:
                logger.error("Failed to establish connection after multiple attempts")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error connecting to MQTT broker: {str(e)}")
            return False
    
    def disconnect(self):
        """
        Ngắt kết nối từ MQTT broker.
        """
        try:
            if self.is_connected:
                logger.info("Disconnecting from MQTT broker")
                self.client.loop_stop()
                self.client.disconnect()
                self.is_connected = False
                return True
            return False
        except Exception as e:
            logger.error(f"Error disconnecting from MQTT broker: {str(e)}")
            return False
    
    def publish(self, feed_id, value):
        """
        Xuất bản một giá trị tới một feed cụ thể.
        
        Args:
            feed_id (str): ID của feed (không bao gồm username)
            value (str/int/float): Giá trị để xuất bản
        
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            if not self.is_connected:
                logger.warning("Not connected to MQTT broker. Attempting to connect...")
                if not self.connect():
                    return False
            
            # Định dạng topic
            topic = f"{self.username}/feeds/{feed_id}"
            
            # Chuyển đổi giá trị thành chuỗi nếu cần
            if not isinstance(value, str):
                if isinstance(value, (dict, list)):
                    payload = json.dumps(value)
                else:
                    payload = str(value)
            else:
                payload = value
            
            logger.info(f"Publishing to {topic}: {payload}")
            result = self.client.publish(topic, payload, qos=1)
            
            # Kiểm tra kết quả
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"Failed to publish: {mqtt.error_string(result.rc)}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error publishing to MQTT: {str(e)}")
            return False
    
    # Thêm alias cho publish để hỗ trợ mã cũ
    publish_message = publish
    
    def subscribe(self, feed_id=None):
        """
        Đăng ký nhận tin nhắn từ một feed cụ thể hoặc tất cả các feeds.
        
        Args:
            feed_id (str, optional): ID của feed cụ thể. Mặc định là None (tất cả feeds).
        
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            if not self.is_connected:
                logger.warning("Not connected to MQTT broker. Attempting to connect...")
                if not self.connect():
                    return False
            
            # Định dạng topic
            if feed_id:
                topic = f"{self.username}/feeds/{feed_id}"
            else:
                topic = self.topic
            
            logger.info(f"Subscribing to {topic}")
            result = self.client.subscribe(topic)
            
            # Kiểm tra kết quả
            if result[0] != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"Failed to subscribe: {mqtt.error_string(result[0])}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error subscribing to MQTT: {str(e)}")
            return False
    
    # Các hàm callback
    def on_connect(self, client, userdata, flags, rc):
        """
        Callback khi kết nối tới broker được thiết lập.
        """
        if rc == 0:
            logger.info("Connected to MQTT broker successfully")
            self.is_connected = True
            
            # Tự động đăng ký nhận tin nhắn từ tất cả feeds
            self.subscribe()
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")
            self.is_connected = False
    
    def on_disconnect(self, client, userdata, rc):
        """
        Callback khi ngắt kết nối từ broker.
        """
        logger.info(f"Disconnected from MQTT broker with code: {rc}")
        self.is_connected = False
        
        # Thử kết nối lại nếu ngắt kết nối không mong muốn
        if rc != 0:
            logger.warning("Unexpected disconnection from MQTT broker. Attempting to reconnect...")
            time.sleep(5)
            self.connect()
    
    def on_message(self, client, userdata, msg):
        """
        Callback khi nhận được tin nhắn từ broker.
        """
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            # Extract feed_id from topic
            parts = topic.split('/')
            if len(parts) >= 3 and parts[1] == "feeds":
                feed_id = parts[2]
                
                # Cố gắng parse JSON nếu payload là JSON
                try:
                    data = json.loads(payload)
                    logger.info(f"Received JSON message from {topic} (feed: {feed_id}): {data}")
                except json.JSONDecodeError:
                    data = payload
                    logger.info(f"Received message from {topic} (feed: {feed_id}): {payload}")
                
                # Xử lý tin nhắn và lưu vào database nếu cần
                self.handle_message(feed_id, data)
            else:
                logger.warning(f"Received message with invalid topic format: {topic}")
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
    
    def on_publish(self, client, userdata, mid):
        """
        Callback khi tin nhắn được xuất bản thành công.
        """
        logger.debug(f"Message {mid} published successfully")
        
    def ensure_default_device(self, db, device_id="default"):
        """
        Đảm bảo thiết bị mặc định tồn tại trong database
        """
        try:
            from models import Device
            
            # Kiểm tra xem thiết bị đã tồn tại chưa
            device = db.query(Device).filter(Device.device_id == device_id).first()
            
            if not device:
                # Tạo thiết bị mới
                new_device = Device(
                    device_id=device_id,
                    name="Default Device",
                    description="Thiết bị mặc định cho dữ liệu Adafruit IO"
                )
                db.add(new_device)
                db.commit()
                logger.info(f"Đã tạo thiết bị mặc định với ID: {device_id}")
            
            return True
        except Exception as e:
            logger.error(f"Lỗi khi đảm bảo thiết bị mặc định: {str(e)}")
            db.rollback()
            return False
            
    def get_data_dimension(self, feed_id):
        """
        Xác định chiều dữ liệu (dimension) dựa vào tên feed.
        Mặc định là power, trừ khi feed_id chứa từ khóa cụ thể.
        
        Args:
            feed_id (str): ID của feed
            
        Returns:
            str: Tên chiều dữ liệu (power, humidity, pressure, temperature)
        """
        feed_id_lower = feed_id.lower()
        
        # Mapping từ từ khóa đến dimension
        dimension_mapping = {
            "power": ["power", "energy", "công-suất", "congsuat"],
            "humidity": ["humidity", "hum", "độ-ẩm", "doam"],
            "pressure": ["pressure", "press", "áp-suất", "apsuat"],
            "temperature": ["temperature", "temp", "nhiệt-độ", "nhietdo"]
        }
        
        # Kiểm tra từng dimension
        for dimension, keywords in dimension_mapping.items():
            if any(keyword in feed_id_lower for keyword in keywords):
                return dimension
                
        # Mặc định là power nếu không có từ khóa nào khớp
        return "power"
            
    def handle_message(self, feed_id, data):
        """
        Xử lý tin nhắn nhận được từ Adafruit IO và lưu vào database
        
        Args:
            feed_id (str): ID của feed
            data: Dữ liệu nhận được (có thể là dict, str, hoặc kiểu dữ liệu khác)
        """
        try:
            # Tạo session mới
            db = SessionLocal()
            
            # Sử dụng feed_id làm device_id
            device_id = feed_id
            
            # Đảm bảo thiết bị tồn tại trong database
            try:
                # Import các model cần thiết
                from models import Device, OriginalSample
                
                # Kiểm tra xem thiết bị đã tồn tại chưa
                device = db.query(Device).filter(Device.device_id == device_id).first()
                
                if not device:
                    # Tạo thiết bị mới với tên là feed_id
                    new_device = Device(
                        device_id=device_id,
                        name=f"Feed {feed_id}",
                        description=f"Thiết bị được tạo tự động từ feed Adafruit IO: {feed_id}"
                    )
                    db.add(new_device)
                    db.commit()
                    logger.info(f"Đã tạo thiết bị mới với ID: {device_id}")
                
                # Xử lý giá trị trước khi chuyển đổi
                float_value = 0.0
                skip_saving = False
                
                # Kiểm tra nếu dữ liệu là URL hoặc dữ liệu không phải số
                if isinstance(data, str):
                    # Kiểm tra nếu là URL
                    if data.startswith(('http://', 'https://', 'www.')):
                        logger.info(f"Phát hiện URL trong dữ liệu: {data[:50]}...")
                        # Có thể bỏ qua hoặc xử lý đặc biệt với URL
                        skip_saving = True  # Bỏ qua lưu vào database
                    elif ',' in data:
                        # Nếu giá trị có dạng CSV, lấy giá trị đầu tiên
                        first_part = data.split(',')[0].strip()
                        try:
                            if first_part:
                                float_value = float(first_part)
                        except (ValueError, TypeError):
                            logger.warning(f"Giá trị CSV không thể chuyển đổi sang số: {first_part}")
                    else:
                        # Kiểm tra nếu chuỗi có thể chuyển đổi thành số
                        try:
                            float_value = float(data)
                        except (ValueError, TypeError):
                            logger.warning(f"Giá trị chuỗi không thể chuyển đổi sang số: {data}")
                            # Không bỏ qua lưu vào database, nhưng giữ giá trị mặc định là 0.0
                elif isinstance(data, dict) and "value" in data:
                    # Nếu là dict và có key "value"
                    try:
                        if isinstance(data["value"], (int, float)):
                            float_value = float(data["value"])
                        elif isinstance(data["value"], str):
                            # Kiểm tra nếu chuỗi giá trị là URL
                            if data["value"].startswith(('http://', 'https://', 'www.')):
                                logger.info(f"Phát hiện URL trong data['value']: {data['value'][:50]}...")
                                # Bỏ qua hoặc xử lý đặc biệt
                            else:
                                float_value = float(data["value"])
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Không thể chuyển đổi giá trị trong dict: {data['value']}, lỗi: {str(e)}")
                elif isinstance(data, (int, float)):
                    float_value = float(data)
                
                # Bỏ qua lưu trữ nếu đã đánh dấu
                if skip_saving:
                    logger.info(f"Bỏ qua lưu dữ liệu không phải số cho feed {feed_id}")
                    return
                
                # Xác định chiều dữ liệu từ feed_id
                dimension = self.get_data_dimension(feed_id)
                
                # Tạo dữ liệu đa chiều với chiều xác định từ feed_id
                original_data = {
                    "power": 0.0,
                    "humidity": 0.0,
                    "pressure": 0.0,
                    "temperature": 0.0
                }
                
                # Cập nhật giá trị cho chiều dữ liệu đúng
                original_data[dimension] = float_value
                
                # Tạo bản ghi mới trong bảng original_samples
                new_sample = OriginalSample(
                    device_id=device_id,
                    original_data=original_data,
                    timestamp=datetime.datetime.utcnow()
                )
                
                # Lưu vào SensorData như trước đây để duy trì tính tương thích ngược
                from models import SensorData
                new_data = SensorData(
                    device_id=device_id,
                    feed_id=feed_id,
                    value=float_value,
                    raw_data=str(data)
                )
                
                # Thêm và commit vào database
                db.add(new_sample)
                db.add(new_data)
                db.commit()
                
                logger.info(f"Đã xử lý dữ liệu từ feed {feed_id}: {float_value}")
                logger.info(f"Đã lưu vào original_samples với device_id={device_id}, {dimension}={float_value}")
                logger.info(f"Đã lưu dữ liệu từ feed {feed_id} vào sensor_data với giá trị {float_value}")
                
            except Exception as e:
                logger.error(f"Lỗi khi xử lý và lưu dữ liệu: {str(e)}")
                db.rollback()
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Lỗi khi xử lý tin nhắn: {str(e)}") 