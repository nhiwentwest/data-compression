from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import timedelta, datetime
from typing import List, Dict, Any
import models, auth
from pydantic import BaseModel, Field
import logging
import json
from fastapi.responses import JSONResponse
from config import settings
from mqtt_client import MQTTClient
from database import engine, get_db, init_db

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try import compression API
try:
    from compression_api import router as compression_router
    has_compression_api = True
    logger.info("Successfully imported compression_api module")
except ImportError:
    has_compression_api = False
    logger.warning("Could not import compression_api module. Compression features will be disabled.")

# Thông tin kết nối database từ config
logger.info(f"Initializing database connection with URL: {settings.DATABASE_URL}")

# Kiểm tra kết nối database
try:
    with engine.connect() as connection:
        logger.info("Successfully connected to database")
except Exception as e:
    logger.error(f"Failed to connect to database: {str(e)}")
    raise

# Tạo các bảng nếu chưa tồn tại
init_db()

app = FastAPI()

# Thêm router compression nếu có thể import
if has_compression_api:
    app.include_router(compression_router)
    logger.info("Included compression_api router in FastAPI application")
else:
    logger.warning("Compression API router was not included")

# Khởi tạo MQTT client
mqtt_client = None

@app.on_event("startup")
async def startup_event():
    global mqtt_client
    try:
        # Kích hoạt kết nối MQTT
        logger.info("Đang kết nối tới MQTT broker (Adafruit IO)...")
        mqtt_client = MQTTClient()
        mqtt_client.connect()
        logger.info("MQTT client connected successfully")
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        # Không raise exception để app vẫn có thể khởi động ngay cả khi MQTT thất bại
        # raise

@app.on_event("shutdown")
async def shutdown_event():
    global mqtt_client
    if mqtt_client:
        try:
            mqtt_client.disconnect()
            logger.info("MQTT client disconnected successfully")
        except Exception as e:
            logger.error(f"Error disconnecting MQTT client: {str(e)}")

class UserCreate(BaseModel):
    username: str
    email: str
    password: str

class DeviceConfigCreate(BaseModel):
    device_id: str = Field(..., min_length=1, max_length=100)
    config_data: Dict[str, float] = Field(..., description="Cấu hình với 4 thông số: power, humidity, pressure, temperature")

    class Config:
        json_schema_extra = {
            "example": {
                "device_id": "device123",
                "config_data": {
                    "power": 100.0,
                    "humidity": 60.0,
                    "pressure": 1013.25,
                    "temperature": 25.5
                }
            }
        }

class Token(BaseModel):
    access_token: str
    token_type: str

@app.post("/register/", response_model=dict)
def register(user: UserCreate, db: Session = Depends(get_db)):
    try:
        db_user = db.query(models.User).filter(models.User.username == user.username).first()
        if db_user:
            raise HTTPException(status_code=400, detail="Username already registered")
        
        hashed_password = auth.get_password_hash(user.password)
        db_user = models.User(
            username=user.username,
            email=user.email,
            hashed_password=hashed_password
        )
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        return {"message": "User created successfully"}
    except Exception as e:
        logger.error(f"Error in register: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/login/", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    try:
        logger.info(f"Login attempt for username: {form_data.username}")
        
        # Tìm user trong database
        user = db.query(models.User).filter(models.User.username == form_data.username).first()
        logger.info(f"User found in database: {user is not None}")
        
        if not user:
            logger.error(f"User not found: {form_data.username}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
            
        # Kiểm tra password
        is_password_correct = auth.verify_password(form_data.password, user.hashed_password)
        logger.info("Password verification result: " + str(is_password_correct))
        
        if not is_password_correct:
            logger.error("Incorrect password")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
            
        # Tạo access token
        logger.info("Creating access token...")
        access_token_expires = timedelta(minutes=30)
        access_token = auth.create_access_token(
            data={"sub": user.username}, expires_delta=access_token_expires
        )
        logger.info("Access token created successfully")
        
        return {"access_token": access_token, "token_type": "bearer"}
        
    except HTTPException as http_ex:
        logger.error(f"HTTP Exception in login: {str(http_ex)}")
        raise http_ex
    except Exception as e:
        logger.error(f"Unexpected error in login: {str(e)}")
        logger.exception("Full error traceback:")
        raise HTTPException(
            status_code=500,
            detail=f"Login error: {str(e)}"
        )

@app.post("/device-config/", response_model=dict)
async def create_device_config(
    config: DeviceConfigCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        logger.info(f"Received request for user: {current_user.username} (ID: {current_user.id})")
        logger.info(f"Device config data: {config.dict()}")

        # Validate config_data
        if not isinstance(config.config_data, dict):
            logger.error("Invalid config_data format")
            raise HTTPException(
                status_code=400,
                detail="config_data must be a valid dictionary"
            )
            
        # Kiểm tra xem tất cả các phần tử trong config_data có phải là số không
        if not all(isinstance(item, (float, int)) for item in config.config_data.values()):
            logger.error("Invalid config_data: contains non-numeric items")
            raise HTTPException(
                status_code=400,
                detail="All items in config_data must be numeric"
            )
            
        # Kiểm tra xem config_data có chứa đủ 4 thông số cần thiết không
        required_dimensions = ["power", "humidity", "pressure", "temperature"]
        missing_dimensions = [dim for dim in required_dimensions if dim not in config.config_data]
        
        if missing_dimensions:
            logger.error(f"Missing required dimensions in config_data: {missing_dimensions}")
            raise HTTPException(
                status_code=400,
                detail=f"Config data must include all of these dimensions: {', '.join(required_dimensions)}"
            )
            
        # Kiểm tra phạm vi giá trị hợp lệ
        valid_ranges = {
            "power": {"min": 0.0, "max": 1000.0},
            "humidity": {"min": 0.0, "max": 100.0},
            "pressure": {"min": 800.0, "max": 1200.0},
            "temperature": {"min": -50.0, "max": 100.0}
        }
        
        invalid_values = []
        for dim, value in config.config_data.items():
            if dim in valid_ranges:
                v_range = valid_ranges[dim]
                if value < v_range["min"] or value > v_range["max"]:
                    invalid_values.append(f"{dim}: {value} (expected range: {v_range['min']} - {v_range['max']})")
                    
        if invalid_values:
            logger.error(f"Invalid values in config_data: {invalid_values}")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid values detected: {', '.join(invalid_values)}"
            )
            
        # Kiểm tra xem device_id có tồn tại trong bảng devices hay không
        device = db.query(models.Device).filter(models.Device.device_id == config.device_id).first()
        if not device:
            logger.error(f"Device with ID {config.device_id} not found")
            raise HTTPException(
                status_code=404,
                detail=f"Device with ID {config.device_id} not found. The device must exist in the devices table."
            )

        logger.info("Creating device config in database...")
        try:
            # Create device config
            db_config = models.DeviceConfig(
                device_id=config.device_id,
                config_data=config.config_data,
                user_id=current_user.id
            )
            logger.info(f"Created device config object: {db_config.__dict__}")

            db.add(db_config)
            logger.info("Added to session")
            
            db.commit()
            logger.info("Committed to database")
            
            db.refresh(db_config)
            logger.info("Refreshed object")
            
            response_data = {
                "message": "Device configuration saved successfully",
                "id": db_config.id,
                "device_id": db_config.device_id,
                "device_name": device.name,
                "user_id": db_config.user_id,
                "config_data": db_config.config_data,
                "dimensions_count": len(db_config.config_data)
            }
            logger.info(f"Success response: {response_data}")
            return JSONResponse(
                status_code=200,
                content=response_data
            )
            
        except Exception as db_error:
            db.rollback()
            logger.error(f"Database error details: {str(db_error)}")
            logger.exception("Full database error traceback:")
            raise HTTPException(
                status_code=500,
                detail=f"Error saving to database: {str(db_error)}"
            )

    except HTTPException as http_ex:
        logger.error(f"HTTP exception: {str(http_ex)}")
        raise http_ex
    except Exception as e:
        logger.error(f"Unexpected error details: {str(e)}")
        logger.exception("Full error traceback:")
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

@app.get("/device-config/{user_id}", response_model=List[Dict])
def get_device_configs(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        if current_user.id != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to access this resource")
        
        # Lấy tất cả cấu hình thiết bị của người dùng, bao gồm thông tin thiết bị
        configs = db.query(models.DeviceConfig).filter(
            models.DeviceConfig.user_id == user_id
        ).all()
        
        # Chuyển đổi dữ liệu thành định dạng response
        result = []
        for config in configs:
            # Đảm bảo config_data là dictionary
            config_data = config.config_data if isinstance(config.config_data, dict) else {}
            
            # Chuẩn bị dữ liệu trả về
            config_info = {
                "id": config.id,
                "device_id": config.device_id,
                "device_name": config.device.name if config.device else "Unknown",
                "user_id": config.user_id,
                "config_data": config_data,
                "dimensions_count": len(config_data)
            }
            
            # Thêm các thông số riêng biệt nếu có
            if "power" in config_data:
                config_info["power"] = config_data["power"]
            if "humidity" in config_data:
                config_info["humidity"] = config_data["humidity"]
            if "pressure" in config_data:
                config_info["pressure"] = config_data["pressure"]
            if "temperature" in config_data:
                config_info["temperature"] = config_data["temperature"]
            
            result.append(config_info)
        
        return result
    except Exception as e:
        logger.error(f"Error in get_device_configs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/device-data/")
async def publish_device_data(
    data: Dict[str, Any],
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        # Thêm thông tin user vào data
        data["user_id"] = current_user.id
        data["timestamp"] = datetime.utcnow().isoformat()
        
        # Kiểm tra MQTT client trước khi publish
        if mqtt_client:
            # Publish message
            topic = f"iot/devices/{current_user.id}"
            mqtt_client.publish_message(topic, data)
            return {"message": "Device data published successfully"}
        else:
            # Trả về thông báo khi MQTT không khả dụng
            return {"message": "MQTT is disabled, data logged but not published"}
    except Exception as e:
        logger.error(f"Error publishing device data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"message": "IoT Backend API"}

@app.get("/sensor-data/")
def get_sensor_data(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    data = db.query(models.SensorData).offset(skip).limit(limit).all()
    return data

@app.get("/publish/{feed_id}/{value}")
def publish_to_feed(feed_id: str, value: str):
    if mqtt_client:
        success = mqtt_client.publish_message(feed_id, value)
        if success:
            return {"status": "success", "message": f"Đã gửi {value} đến feed {feed_id}"}
        else:
            raise HTTPException(status_code=500, detail="Không thể gửi dữ liệu")
    else:
        # Thông báo khi MQTT không khả dụng
        return {"status": "info", "message": "MQTT client đang bị vô hiệu hóa"}

@app.get("/device-config-schema/", response_model=Dict)
def get_device_config_schema():
    """
    Trả về schema và giá trị mặc định cho cấu hình thiết bị.
    """
    return {
        "required_dimensions": ["power", "humidity", "pressure", "temperature"],
        "default_values": {
            "power": 100.0,
            "humidity": 60.0,
            "pressure": 1013.25,
            "temperature": 25.5
        },
        "valid_ranges": {
            "power": {"min": 0.0, "max": 1000.0, "unit": "W"},
            "humidity": {"min": 0.0, "max": 100.0, "unit": "%"},
            "pressure": {"min": 800.0, "max": 1200.0, "unit": "hPa"},
            "temperature": {"min": -50.0, "max": 100.0, "unit": "°C"}
        },
        "description": {
            "power": "Công suất tiêu thụ của thiết bị (W)",
            "humidity": "Độ ẩm môi trường (%)",
            "pressure": "Áp suất không khí (hPa)",
            "temperature": "Nhiệt độ môi trường (°C)"
        }
    }

@app.get("/feed-mapping/", response_model=Dict)
def get_feed_dimension_mapping():
    """
    Trả về thông tin về cách các feed được ánh xạ vào các chiều dữ liệu.
    """
    # Sử dụng danh sách các từ khóa từ MQTTClient.get_data_dimension
    return {
        "dimension_mapping": {
            "power": ["power", "energy", "công-suất", "congsuat"],
            "humidity": ["humidity", "hum", "độ-ẩm", "doam"],
            "pressure": ["pressure", "press", "áp-suất", "apsuat"],
            "temperature": ["temperature", "temp", "nhiệt-độ", "nhietdo"]
        },
        "default_dimension": "power",
        "description": "Feed được phân loại dựa trên các từ khóa có trong tên feed. "
                      "Nếu không khớp với từ khóa nào, mặc định sẽ là 'power'."
    }

@app.get("/device-samples/{device_id}", response_model=List[Dict])
def get_device_samples(
    device_id: str,
    limit: int = 100,
    skip: int = 0,
    db: Session = Depends(get_db)
):
    """
    Lấy dữ liệu mẫu từ thiết bị dựa trên ID.
    
    Args:
        device_id: ID của thiết bị cần lấy dữ liệu
        limit: Số lượng bản ghi tối đa (mặc định: 100)
        skip: Số bản ghi bỏ qua (mặc định: 0)
    """
    try:
        # Kiểm tra thiết bị tồn tại
        device = db.query(models.Device).filter(models.Device.device_id == device_id).first()
        if not device:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy thiết bị với ID: {device_id}")
            
        # Lấy dữ liệu mẫu từ bảng original_samples
        samples = db.query(models.OriginalSample).filter(
            models.OriginalSample.device_id == device_id
        ).order_by(
            models.OriginalSample.timestamp.desc()
        ).offset(skip).limit(limit).all()
        
        # Chuẩn bị kết quả trả về
        result = []
        for sample in samples:
            sample_data = {
                "id": sample.id,
                "device_id": sample.device_id,
                "timestamp": sample.timestamp.isoformat(),
                "data": sample.original_data
            }
            
            # Thêm các chiều dữ liệu riêng lẻ để dễ truy cập
            if isinstance(sample.original_data, dict):
                for dim in ["power", "humidity", "pressure", "temperature"]:
                    if dim in sample.original_data:
                        sample_data[dim] = sample.original_data[dim]
            
            result.append(sample_data)
            
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn dữ liệu thiết bị: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Lỗi server: {str(e)}") 