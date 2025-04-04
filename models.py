#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Models cho các bảng dữ liệu trong hệ thống.
Các lớp này định nghĩa cấu trúc dữ liệu cho SQLAlchemy ORM.
"""

from sqlalchemy import Column, Integer, String, ForeignKey, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB, TSRANGE
import datetime

Base = declarative_base()

class User(Base):
    """
    Bảng chứa thông tin người dùng hệ thống.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    device_configs = relationship("DeviceConfig", back_populates="owner")
    
    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"

class DeviceConfig(Base):
    """
    Bảng chứa cấu hình thiết bị của người dùng.
    """
    __tablename__ = "device_configs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    device_id = Column(String, ForeignKey("devices.device_id"), index=True)
    config_data = Column(JSONB)
    owner = relationship("User", back_populates="device_configs")
    device = relationship("Device", back_populates="device_configs")
    
    def __repr__(self):
        return f"<DeviceConfig(id={self.id}, user_id='{self.user_id}', device_id='{self.device_id}')>"

class Device(Base):
    """
    Bảng chứa thông tin về các thiết bị IoT trong hệ thống.
    """
    __tablename__ = "devices"
    
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, unique=True, index=True)
    name = Column(String)
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Relationship với các bảng khác
    original_samples = relationship("OriginalSample", back_populates="device")
    compressed_data_optimized = relationship("CompressedDataOptimized", back_populates="device")
    device_configs = relationship("DeviceConfig", back_populates="device")
    sensor_data = relationship("SensorData", back_populates="device")
    
    def __repr__(self):
        return f"<Device(id={self.id}, device_id='{self.device_id}', name='{self.name}')>"

class OriginalSample(Base):
    """
    Bảng chứa dữ liệu gốc từ các thiết bị IoT.
    Mỗi bản ghi chứa dữ liệu theo định dạng JSONB có thể chứa nhiều chiều dữ liệu
    như power, humidity, pressure, temperature.
    """
    __tablename__ = "original_samples"
    
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, ForeignKey("devices.device_id"))
    original_data = Column(JSONB, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)
    
    # Relationship với Device
    device = relationship("Device", back_populates="original_samples")
    
    def __repr__(self):
        return f"<OriginalSample(id={self.id}, device_id='{self.device_id}')>"

class SensorData(Base):
    """
    Bảng chứa dữ liệu cảm biến từ các thiết bị.
    Mỗi bản ghi chứa một giá trị cảm biến và thông tin liên quan.
    """
    __tablename__ = "sensor_data"
    
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, ForeignKey("devices.device_id"))
    feed_id = Column(String, index=True)
    value = Column(Float)
    raw_data = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Relationship với Device
    device = relationship("Device", back_populates="sensor_data")
    
    def __repr__(self):
        return f"<SensorData(id={self.id}, device_id='{self.device_id}', feed_id='{self.feed_id}', value={self.value})>"

class CompressedDataOptimized(Base):
    """
    Bảng chứa dữ liệu nén theo cấu trúc tối ưu mới.
    Mỗi bản ghi chứa đầy đủ thông tin về quá trình nén, bao gồm:
    - compression_metadata: Metadata của quá trình nén (tỷ lệ nén, hit ratio, v.v.)
    - templates: Danh sách các template được sử dụng
    - encoded_stream: Chuỗi mã hóa chứa thông tin về cách sử dụng các template
    - time_range: Phạm vi thời gian của dữ liệu được nén
    """
    __tablename__ = "compressed_data_optimized"
    
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, ForeignKey("devices.device_id"))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)
    compression_metadata = Column(JSONB, comment="Lưu thông tin nén (compression_ratio, hit_ratio, etc)")
    templates = Column(JSONB, comment="Lưu templates")
    encoded_stream = Column(JSONB, comment="Lưu chuỗi mã hóa")
    time_range = Column(TSRANGE, comment="Phạm vi thời gian của dữ liệu", index=True)
    
    # Relationship
    device = relationship("Device", back_populates="compressed_data_optimized")
    
    def __repr__(self):
        return f"<CompressedDataOptimized(id={self.id}, device_id='{self.device_id}')>"

    def get_compression_ratio(self):
        """Trả về tỷ lệ nén từ metadata"""
        if self.compression_metadata and 'compression_ratio' in self.compression_metadata:
            return self.compression_metadata['compression_ratio']
        return 0
    
    def get_time_range_display(self):
        """Trả về phạm vi thời gian dưới dạng hiển thị"""
        if self.time_range:
            try:
                if hasattr(self.time_range, 'lower') and hasattr(self.time_range, 'upper'):
                    lower = self.time_range.lower.isoformat() if self.time_range.lower else "N/A"
                    upper = self.time_range.upper.isoformat() if self.time_range.upper else "N/A"
                    return f"{lower} đến {upper}"
            except:
                pass
        return "Không có thông tin thời gian"
