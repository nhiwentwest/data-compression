#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để tạo dữ liệu đa chiều mẫu và thêm vào bảng original_samples.
"""

import json
import psycopg2
from datetime import datetime, timedelta

def main():
    # Kết nối tới database
    conn = psycopg2.connect('postgresql://postgres:1234@localhost:5433/iot_db')
    cursor = conn.cursor()

    # Tạo thiết bị nếu chưa tồn tại
    device_id = 'test_db_multi'
    cursor.execute('SELECT device_id FROM devices WHERE device_id = %s', (device_id,))
    if not cursor.fetchone():
        cursor.execute(
            'INSERT INTO devices (device_id, name, description) VALUES (%s, %s, %s)',
            (device_id, 'Test DB Multi Device', 'Test device for multi-dimensional data')
        )

    # Tạo dữ liệu mẫu
    num_records = 50
    start_time = datetime.now()
    for i in range(num_records):
        timestamp = start_time + timedelta(minutes=i*5)
        
        # Tạo dữ liệu cảm biến
        original_data = {
            'power': 120 + (i % 10),
            'temperature': 25 + (i % 5) / 10,
            'humidity': 60 + (i % 8) / 10,
            'pressure': 1013 + (i % 6) / 10
        }
        
        # Chuyển đổi dữ liệu thành JSON
        data_json = json.dumps(original_data)
        
        # Thêm vào bảng original_samples
        cursor.execute(
            'INSERT INTO original_samples (device_id, original_data, timestamp) VALUES (%s, %s::jsonb, %s)',
            (device_id, data_json, timestamp)
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f'Đã thêm {num_records} bản ghi đa chiều vào bảng original_samples với device_id: {device_id}')

if __name__ == "__main__":
    main() 