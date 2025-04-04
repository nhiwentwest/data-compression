#!/usr/bin/env python3
"""
Module để phân tích và tạo biểu đồ từ dữ liệu nén được lưu trong bảng compressed_data.
Tập trung vào:
1. So sánh dữ liệu trước và sau khi nén (số dòng, kích thước)
2. Phân tích quá trình nén (templates, kích thước khối)
"""

import os
import sys
import json
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Any, Tuple
from sqlalchemy import create_engine, text
import logging
from matplotlib.ticker import FuncFormatter

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình đồ thị
plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12

def get_database_connection():
    """
    Tạo kết nối đến cơ sở dữ liệu
    
    Returns:
        SQLAlchemy engine
    """
    # Sử dụng DATABASE_URL từ biến môi trường nếu có, nếu không thì tạo từ các thành phần
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        # Lấy thông tin kết nối từ biến môi trường
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5433")
        db_name = os.getenv("DB_NAME", "iot_db")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "1234")
        
        # Tạo URL kết nối
        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        # Tạo engine
        engine = create_engine(database_url)
        
        # Kiểm tra kết nối
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            
        logger.info(f"Kết nối đến cơ sở dữ liệu thành công: {database_url}")
        return engine
        
    except Exception as e:
        logger.error(f"Lỗi kết nối đến cơ sở dữ liệu: {str(e)}")
        raise

def get_compression_data(engine, compression_id: int) -> Dict[str, Any]:
    """
    Lấy dữ liệu nén từ bảng compressed_data với ID chỉ định
    
    Args:
        engine: SQLAlchemy engine
        compression_id: ID của bản ghi nén
        
    Returns:
        Dict chứa thông tin về dữ liệu nén
    """
    try:
        # Câu truy vấn SQL
        query = """
        SELECT c.* 
        FROM compressed_data c
        WHERE c.id = :compression_id
        LIMIT 1
        """
        
        # Thực hiện truy vấn
        with engine.connect() as conn:
            result = conn.execute(text(query), {"compression_id": compression_id})
            row = result.fetchone()
            
            if not row:
                raise ValueError(f"Không tìm thấy dữ liệu nén với ID: {compression_id}")
                
            # Chuyển đổi thành dict
            column_names = result.keys()
            record = {col: row[idx] for idx, col in enumerate(column_names)}
            
            # Nếu không có trường compressed_data trong bảng, tạo dict rỗng
            if "compressed_data" not in record or record["compressed_data"] is None:
                compressed_data = {}
                record["compressed_data"] = compressed_data
            else:
                # Parse các trường JSON nếu cần
                if isinstance(record["compressed_data"], str):
                    try:
                        record["compressed_data"] = json.loads(record["compressed_data"])
                    except json.JSONDecodeError:
                        record["compressed_data"] = {}
                compressed_data = record["compressed_data"]
            
            # Parse config nếu có
            if "config" in record and isinstance(record["config"], str):
                try:
                    record["config"] = json.loads(record["config"])
                except json.JSONDecodeError:
                    record["config"] = {}
            
            # Trích xuất thông tin từ compressed_data hoặc gán giá trị mặc định
            record["id"] = compression_id
            record["total_values"] = compressed_data.get("total_values", 0)
            record["templates_count"] = len(compressed_data.get("templates", {}))
            record["blocks_processed"] = compressed_data.get("blocks_processed", 0)
            record["hit_ratio"] = compressed_data.get("hit_ratio", 0)
            
            # Trích xuất thông tin mới từ thuật toán nén đã cải tiến
            record["avg_cer"] = compressed_data.get("avg_cer", 0.0)
            record["avg_similarity"] = compressed_data.get("avg_similarity", 0.0)
            record["cost"] = compressed_data.get("cost", 0.0)
            
            # Lấy thêm thông tin về similarity scores và CER values nếu có
            record["similarity_scores"] = compressed_data.get("similarity_scores", [])
            record["cer_values"] = compressed_data.get("cer_values", [])
            
            # Đảm bảo có compression_ratio từ bảng
            if "compression_ratio" not in record or not record["compression_ratio"]:
                record["compression_ratio"] = compressed_data.get("compression_ratio", 1.0)
            
        logger.info(f"Đã lấy dữ liệu nén với ID: {compression_id}")
        return record
        
    except Exception as e:
        logger.error(f"Lỗi lấy dữ liệu nén: {str(e)}")
        raise

def get_original_data(engine, start_date: str = None, end_date: str = None, limit: int = 100) -> pd.DataFrame:
    """
    Lấy dữ liệu gốc từ bảng original_samples
    
    Args:
        engine: SQLAlchemy engine
        start_date: Ngày bắt đầu (YYYY-MM-DD) - tùy chọn
        end_date: Ngày kết thúc (YYYY-MM-DD) - tùy chọn
        limit: Số lượng bản ghi tối đa khi không chỉ định ngày - mặc định 100
        
    Returns:
        DataFrame chứa dữ liệu gốc
    """
    try:
        # Xây dựng câu truy vấn SQL dựa trên các tham số đầu vào
        if start_date and end_date:
            query = """
            SELECT *
            FROM original_samples
            WHERE timestamp BETWEEN :start_date AND :end_date
            ORDER BY timestamp
            """
            params = {"start_date": start_date, "end_date": end_date}
            
            # Thực hiện truy vấn
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
                
            logger.info(f"Đã lấy {len(df)} bản ghi dữ liệu gốc từ {start_date} đến {end_date}")
        else:
            # Nếu không có ngày, lấy số lượng bản ghi giới hạn theo thứ tự thời gian
            query = """
            SELECT *
            FROM original_samples
            ORDER BY timestamp
            LIMIT :limit
            """
            params = {"limit": limit}
            
            # Thực hiện truy vấn
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
                
            logger.info(f"Đã lấy {len(df)} bản ghi dữ liệu gốc gần nhất")
            
        return df
        
    except Exception as e:
        logger.error(f"Lỗi lấy dữ liệu gốc: {str(e)}")
        raise

def analyze_compression_ratio(compression_data, output_dir):
    """
    Tạo biểu đồ phân tích tỷ lệ nén
    
    Args:
        compression_data: Dữ liệu nén đã được truy xuất
        output_dir: Thư mục đầu ra để lưu biểu đồ
    """
    # Kiểm tra đường dẫn đầu ra
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # Lấy thông tin từ compression_data
    device_id = compression_data.get('device_id')
    compression_id = compression_data.get('id')
    total_values = compression_data.get('metadata', {}).get('total_values', 0)
    templates_count = len(compression_data.get('templates', {}))
    blocks_processed = len(compression_data.get('encoded_stream', []))
    compression_ratio = compression_data.get('metadata', {}).get('compression_ratio', 0)
    
    if not compression_id or not device_id:
        logger.warning("Không có thông tin compression_id hoặc device_id, không thể phân tích tỷ lệ nén từ database")
        return
        
    # Kết nối đến database để lấy kích thước thực tế
    engine = get_database_connection()
    if not engine:
        logger.error("Không thể kết nối đến database")
        return
    
    try:
        # Lấy kích thước dữ liệu nén từ bảng compressed_data_optimized
        query_compressed = """
        SELECT 
            pg_column_size(templates) as templates_size,
            pg_column_size(encoded_stream) as encoded_size,
            pg_column_size(compression_metadata) as metadata_size
        FROM compressed_data_optimized
        WHERE id = :compression_id
        """
        
        # Lấy kích thước dữ liệu gốc từ bảng original_samples
        query_original = """
        SELECT 
            SUM(pg_column_size(original_data)) as original_size
        FROM original_samples
        WHERE device_id = :device_id
        """
        
        with engine.connect() as conn:
            # Lấy kích thước dữ liệu nén
            result_compressed = conn.execute(text(query_compressed), {"compression_id": compression_id})
            row_compressed = result_compressed.fetchone()
            
            # Lấy kích thước dữ liệu gốc
            result_original = conn.execute(text(query_original), {"device_id": device_id})
            row_original = result_original.fetchone()
            
            if not row_compressed or not row_original:
                logger.error("Không thể lấy thông tin kích thước từ database")
                return
                
            templates_size = row_compressed[0] or 0
            encoded_size = row_compressed[1] or 0
            metadata_size = row_compressed[2] or 0
            compressed_size = templates_size + encoded_size + metadata_size
            
            original_size = row_original[0] or 0
            
            # Tính toán tỷ lệ nén thực tế từ database
            real_compression_ratio = original_size / compressed_size if compressed_size > 0 else 1.0
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn kích thước từ database: {str(e)}")
        return
    
    # Tạo biểu đồ
    plt.figure(figsize=(10, 6))
    bars = plt.bar(['Original Data', 'Compressed Data'], [original_size, compressed_size], color=['#3498db', '#2ecc71'])
    
    # Thêm nhãn
    plt.title('Compare the size of data', fontsize=14)
    plt.ylabel('Size (bytes)', fontsize=12)
    
    # Thêm giá trị lên đầu thanh
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'{int(height)}', ha='center', va='bottom', fontsize=11)
    
    # Thêm thông tin tỷ lệ nén
    textstr = f"""
    Tỷ lệ nén thực tế: {real_compression_ratio:.2f}x
    Tỷ lệ nén báo cáo: {compression_ratio:.2f}x
    Tổng số giá trị: {total_values}
    Số templates: {templates_count}
    Số block: {blocks_processed}
    Kích thước dữ liệu gốc: {original_size/1024:.2f} KB
    Kích thước dữ liệu nén: {compressed_size/1024:.2f} KB
    """
    
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    plt.text(0.05, 0.95, textstr, transform=plt.gca().transAxes, fontsize=10,
            verticalalignment='top', bbox=props)
    
    # Lưu biểu đồ
    output_file = os.path.join(output_dir, 'compression_ratio.png')
    plt.savefig(output_file, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Đã tạo biểu đồ phân tích tỷ lệ nén: {output_file}")
    return output_file

def analyze_templates(compression_data: Dict[str, Any], output_prefix: str = None):
    """
    Phân tích và tạo biểu đồ về các template
    
    Args:
        compression_data: Dict chứa dữ liệu nén
        output_prefix: Tiền tố cho tên file biểu đồ
    """
    # Trích xuất thông tin template
    compressed_data_json = compression_data.get("compressed_data", {})
    if isinstance(compressed_data_json, str):
        try:
            compressed_data = json.loads(compressed_data_json)
        except json.JSONDecodeError:
            logger.error("Lỗi giải mã JSON từ trường compressed_data")
            return
    else:
        compressed_data = compressed_data_json
    
    templates = compressed_data.get("templates", {})
    
    # Kiểm tra nếu không có templates
    if not templates:
        logger.warning("Không có templates để phân tích")
        return
    
    # Chuyển đổi templates thành DataFrame để dễ phân tích
    template_info = []
    for tid, template in templates.items():
        template_info.append({
            "id": tid,
            "use_count": template.get("use_count", 0),
            "dimensions": len(template.get("values", [])[0]) if template.get("values") and template.get("values") else 0,
            "values_count": len(template.get("values", []))
        })
    
    template_df = pd.DataFrame(template_info)
    
    # Tạo biểu đồ phân tích template
    fig, axs = plt.subplots(1, 2, figsize=(16, 6))
    
    # 1. Biểu đồ top templates được sử dụng nhiều nhất
    top_n = min(10, len(template_df))
    top_templates = template_df.nlargest(top_n, 'use_count')
    
    bars = axs[0].bar(top_templates['id'], top_templates['use_count'], color='orange')
    axs[0].set_title(f"Top {top_n} templates được sử dụng nhiều nhất")
    axs[0].set_xlabel("Template ID")
    axs[0].set_ylabel("Số lần sử dụng")
    
    # Thêm số liệu lên biểu đồ
    for bar in bars:
        height = bar.get_height()
        axs[0].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                  f'{height:.0f}',
                  ha='center', va='bottom', fontweight='bold')
    
    # 2. Biểu đồ phân bố số lần sử dụng template
    axs[1].hist(template_df['use_count'], bins=10, color='skyblue', edgecolor='black')
    axs[1].set_title("Phân bố số lần sử dụng template")
    axs[1].set_xlabel("Số lần sử dụng")
    axs[1].set_ylabel("Số lượng template")
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    if output_prefix:
        plt.savefig(f"{output_prefix}_template_analysis.png", bbox_inches='tight', dpi=300)
    
    plt.close()

def analyze_blocks(compression_data: Dict[str, Any], output_prefix: str = None):
    """
    Phân tích và tạo biểu đồ về các khối dữ liệu
    
    Args:
        compression_data: Dict chứa dữ liệu nén
        output_prefix: Tiền tố cho tên file biểu đồ
    """
    # Trích xuất thông tin khối
    compressed_data_json = compression_data.get("compressed_data", {})
    if isinstance(compressed_data_json, str):
        try:
            compressed_data = json.loads(compressed_data_json)
        except json.JSONDecodeError:
            logger.error("Lỗi giải mã JSON từ trường compressed_data")
            return
    else:
        compressed_data = compressed_data_json
    
    encoded_stream = compressed_data.get("encoded_stream", [])
    
    # Kiểm tra nếu không có khối
    if not encoded_stream:
        logger.warning("Không có khối dữ liệu để phân tích")
        return
    
    # Chuyển đổi khối thành DataFrame để dễ phân tích
    block_info = []
    for block in encoded_stream:
        # Cập nhật để đọc trường similarity_score thay vì is_match từ phiên bản cũ
        similarity_score = block.get("similarity_score", 0.0)
        # Xác định block có phải template mới hay là template được tái sử dụng
        is_template_match = similarity_score < 1.0  # Nếu similarity < 1.0 thì là template match, không phải template mới
        
        block_info.append({
            "template_id": block.get("template_id", -1),
            "is_template_match": is_template_match,  # Dùng để xác định nếu block là template mới hoặc tái sử dụng
            "similarity_score": similarity_score,
            "cer": block.get("cer", 0.0),
            "length": block.get("length", 0)
        })
    
    block_df = pd.DataFrame(block_info)
    
    # Tạo biểu đồ phân tích khối (2x2 grid để hiển thị thêm thông tin)
    fig, axs = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Biểu đồ tỷ lệ template mới/tái sử dụng
    template_reuse = block_df['is_template_match'].value_counts()
    axs[0, 0].pie(template_reuse, 
                labels=['Template tái sử dụng', 'Template mới'] if len(template_reuse) > 1 else ['Template tái sử dụng'],
                autopct='%1.1f%%', 
                colors=['lightgreen', 'lightcoral'] if len(template_reuse) > 1 else ['lightgreen'])
    axs[0, 0].set_title("Tỷ lệ tái sử dụng template")
    
    # 2. Biểu đồ phân bố điểm tương đồng (similarity score)
    axs[0, 1].hist(block_df['similarity_score'], bins=20, color='skyblue', edgecolor='black')
    axs[0, 1].set_title("Phân bố điểm tương đồng (Similarity Score)")
    axs[0, 1].set_xlabel("Điểm tương đồng")
    axs[0, 1].set_ylabel("Số lượng khối")
    
    # 3. Biểu đồ phân bố CER
    axs[1, 0].hist(block_df['cer'], bins=20, color='lightgreen', edgecolor='black')
    axs[1, 0].set_title("Phân bố Compression Error Rate (CER)")
    axs[1, 0].set_xlabel("CER")
    axs[1, 0].set_ylabel("Số lượng khối")
    
    # 4. Biểu đồ phân bố kích thước khối
    axs[1, 1].hist(block_df['length'], bins=20, color='orange', edgecolor='black')
    axs[1, 1].set_title("Phân bố kích thước khối")
    axs[1, 1].set_xlabel("Kích thước khối")
    axs[1, 1].set_ylabel("Số lượng khối")
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    if output_prefix:
        plt.savefig(f"{output_prefix}_block_analysis.png", bbox_inches='tight', dpi=300)
    
    plt.close()
    
    # Tạo biểu đồ bổ sung cho phân tích điểm tương đồng
    plt.figure(figsize=(10, 6))
    if len(compression_data.get("similarity_scores", [])) > 0:
        similarity_scores = compression_data.get("similarity_scores", [])
        plt.plot(range(len(similarity_scores)), similarity_scores, marker='o', linestyle='-', color='blue', alpha=0.5)
        plt.axhline(y=compression_data.get("avg_similarity", 0.0), color='r', linestyle='-', label=f'Trung bình: {compression_data.get("avg_similarity", 0.0):.4f}')
        plt.title("Điểm tương đồng theo thời gian")
        plt.xlabel("Số thứ tự template match")
        plt.ylabel("Điểm tương đồng")
        plt.grid(True)
        plt.legend()
        
        if output_prefix:
            plt.savefig(f"{output_prefix}_similarity_trend.png", bbox_inches='tight', dpi=300)
        
    plt.close()

def analyze_parameter_adjustments(compression_data: Dict[str, Any], output_prefix: str = None):
    """
    Phân tích và tạo biểu đồ về sự điều chỉnh tham số kích thước khối
    
    Args:
        compression_data: Dict chứa dữ liệu nén
        output_prefix: Tiền tố cho tên file biểu đồ
    """
    # Trích xuất thông tin điều chỉnh tham số
    compressed_data_json = compression_data.get("compressed_data", {})
    if isinstance(compressed_data_json, str):
        try:
            compressed_data = json.loads(compressed_data_json)
        except json.JSONDecodeError:
            logger.error("Lỗi giải mã JSON từ trường compressed_data")
            return
    else:
        compressed_data = compressed_data_json
    
    block_size_history = compressed_data.get("block_size_history", [])
    
    # Kiểm tra nếu không có lịch sử điều chỉnh
    if not block_size_history:
        logger.warning("Không có lịch sử điều chỉnh kích thước khối để phân tích")
        return
    
    # Trích xuất dữ liệu từ lịch sử điều chỉnh
    blocks = []
    sizes = []
    hit_ratios = []
    cers = []
    similarities = []
    
    for item in block_size_history:
        if isinstance(item, dict):
            # Cấu trúc mới với block_number và các thông số khác
            blocks.append(item.get('block_number', 0))
            sizes.append(item.get('new_size', 0))
            hit_ratios.append(item.get('hit_ratio', 0))
            cers.append(item.get('recent_cer', 0))
            similarities.append(item.get('recent_similarity', 0))
        else:
            # Cấu trúc cũ là tuple (block_number, size)
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                blocks.append(item[0])
                sizes.append(item[1])
    
    # Tạo biểu đồ nhiều panel để phân tích đầy đủ hơn
    fig, axs = plt.subplots(2, 1, figsize=(14, 12), sharex=True)
    
    # 1. Biểu đồ điều chỉnh kích thước khối
    axs[0].plot(blocks, sizes, marker='o', linestyle='-', color='green', label='Kích thước khối')
    axs[0].set_title("Điều chỉnh kích thước khối")
    axs[0].set_ylabel("Kích thước khối")
    axs[0].grid(True)
    axs[0].legend(loc='upper left')
    
    # 2. Biểu đồ tỷ lệ hit và điểm tương đồng theo thời gian
    if hit_ratios and similarities:
        ax_hr = axs[1]
        ax_hr.plot(blocks, hit_ratios, marker='s', linestyle='-', color='blue', label='Tỷ lệ hit')
        ax_hr.set_xlabel("Số khối đã xử lý")
        ax_hr.set_ylabel("Tỷ lệ hit", color='blue')
        ax_hr.tick_params(axis='y', labelcolor='blue')
        ax_hr.set_ylim(0, 1.1)
        
        # Thêm trục thứ hai cho điểm tương đồng
        ax_sim = ax_hr.twinx()
        ax_sim.plot(blocks, similarities, marker='^', linestyle='-', color='red', label='Điểm tương đồng')
        ax_sim.set_ylabel("Điểm tương đồng", color='red')
        ax_sim.tick_params(axis='y', labelcolor='red')
        ax_sim.set_ylim(0, 1.1)
        
        # Thêm legend cho cả hai trục
        lines1, labels1 = ax_hr.get_legend_handles_labels()
        lines2, labels2 = ax_sim.get_legend_handles_labels()
        ax_hr.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    if output_prefix:
        plt.savefig(f"{output_prefix}_block_size_adjustments.png", bbox_inches='tight', dpi=300)
    
    plt.close()

def analyze_memory_usage(compression_data: Dict[str, Any], output_prefix: str = None):
    """
    Phân tích sử dụng bộ nhớ của dữ liệu nén, lấy kích thước trực tiếp từ database.
    
    Args:
        compression_data: Dữ liệu nén từ database
        output_prefix: Tiền tố cho tên file đầu ra
    """
    try:
        # Lấy thông tin từ compression_data
        compression_id = compression_data.get('id')
        device_id = compression_data.get('device_id')
        
        if not compression_id or not device_id:
            logger.error("Không tìm thấy compression_id hoặc device_id")
            return
            
        # Kết nối đến database
        engine = get_database_connection()
        if not engine:
            logger.error("Không thể kết nối đến database")
            return
            
        # Lấy kích thước dữ liệu nén từ bảng compressed_data_optimized
        query_compressed = """
        SELECT 
            pg_column_size(templates) as templates_size,
            pg_column_size(encoded_stream) as encoded_size,
            pg_column_size(compression_metadata) as metadata_size
        FROM compressed_data_optimized
        WHERE id = :compression_id
        """
        
        # Lấy kích thước dữ liệu gốc từ bảng original_samples
        query_original = """
        SELECT 
            SUM(pg_column_size(original_data)) as original_size
        FROM original_samples
        WHERE device_id = :device_id
        """
        
        # Thực hiện truy vấn
        with engine.connect() as conn:
            # Lấy kích thước dữ liệu nén
            result_compressed = conn.execute(text(query_compressed), {"compression_id": compression_id})
            row_compressed = result_compressed.fetchone()
            
            if not row_compressed:
                logger.error(f"Không tìm thấy dữ liệu nén cho compression_id: {compression_id}")
                return
                
            templates_size = row_compressed[0] or 0
            encoded_size = row_compressed[1] or 0
            metadata_size = row_compressed[2] or 0
            compressed_size = templates_size + encoded_size + metadata_size
            
            # Lấy kích thước dữ liệu gốc
            result_original = conn.execute(text(query_original), {"device_id": device_id})
            row_original = result_original.fetchone()
            
            if not row_original:
                logger.error(f"Không tìm thấy dữ liệu gốc cho device_id: {device_id}")
                return
                
            original_size = row_original[0] or 0
            
            # Tính tỷ lệ nén thực tế từ database
            compression_ratio = original_size / compressed_size if compressed_size > 0 else 1.0
            
            # Chuyển đổi sang KB và MB
            original_kb = original_size / 1024
            compressed_kb = compressed_size / 1024
            
            original_mb = original_kb / 1024
            compressed_mb = compressed_kb / 1024
            
            # Tạo biểu đồ
            fig, axs = plt.subplots(1, 2, figsize=(16, 6))
            
            # 1. Biểu đồ so sánh kích thước bytes
            bars1 = axs[0].bar(["Original Data", "Compressed Data"], 
                              [original_size, compressed_size], 
                              color=['blue', 'green'])
            
            axs[0].set_title("Compare data (Bytes)")
            axs[0].set_ylabel("Size (Bytes)")
            
            # Thêm số liệu lên biểu đồ
            for bar in bars1:
                height = bar.get_height()
                axs[0].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                          f'{height:.0f}',
                          ha='center', va='bottom', fontweight='bold')
            
            # 2. Biểu đồ so sánh kích thước MB
            bars2 = axs[1].bar(["Dữ liệu gốc", "Dữ liệu nén"], 
                              [original_mb, compressed_mb], 
                              color=['blue', 'green'])
            
            axs[1].set_title("So sánh kích thước dữ liệu (MB)")
            axs[1].set_ylabel("Kích thước (MB)")
            
            # Thêm số liệu lên biểu đồ
            for bar in bars2:
                height = bar.get_height()
                axs[1].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                          f'{height:.2f}',
                          ha='center', va='bottom', fontweight='bold')
            
            # Thêm thông tin chi tiết về kích thước
            details_text = f"""
            Chi tiết kích thước:
            - Templates: {templates_size/1024:.2f} KB
            - Encoded Stream: {encoded_size/1024:.2f} KB
            - Metadata: {metadata_size/1024:.2f} KB
            - Tổng kích thước nén: {compressed_kb:.2f} KB
            - Tổng kích thước gốc: {original_kb:.2f} KB
            - Tỷ lệ nén thực tế: {compression_ratio:.2f}x
            """
            
            plt.figtext(0.02, 0.02, details_text, fontsize=10, va='bottom')
            
            # Lưu biểu đồ
            if output_prefix:
                output_file = f"{output_prefix}_memory_usage.png"
            else:
                output_file = "memory_usage.png"
                
            plt.tight_layout()
            plt.savefig(output_file)
            plt.close()
            
            logger.info(f"Đã tạo biểu đồ phân tích sử dụng bộ nhớ: {output_file}")
            
    except Exception as e:
        logger.error(f"Lỗi khi phân tích sử dụng bộ nhớ: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def create_summary_chart(compression_data: Dict[str, Any], output_prefix: str = None):
    """
    Tạo biểu đồ tổng hợp về kết quả nén
    
    Args:
        compression_data: Dict chứa dữ liệu nén
        output_prefix: Tiền tố cho tên file biểu đồ
    """
    # Trích xuất thông tin cần thiết
    total_values = compression_data.get("total_values", 0)
    templates_count = compression_data.get("templates_count", 0)
    blocks_processed = compression_data.get("blocks_processed", 0)
    hit_ratio = compression_data.get("hit_ratio", 0)
    compression_ratio = compression_data.get("compression_ratio", 0)
    avg_cer = compression_data.get("avg_cer", 0.0)
    avg_similarity = compression_data.get("avg_similarity", 0.0)
    cost = compression_data.get("cost", 0.0)
    
    # Tạo biểu đồ tổng hợp với 3x2 grid
    fig, axs = plt.subplots(3, 2, figsize=(18, 15))
    
    # 1. Biểu đồ tỷ lệ nén
    bars1 = axs[0, 0].bar(["Dữ liệu gốc", "Dữ liệu nén"], 
                        [1, 1/compression_ratio if compression_ratio else 0], 
                        color=['blue', 'green'])
    
    # Thêm số liệu lên biểu đồ
    for bar in bars1:
        height = bar.get_height()
        axs[0, 0].text(bar.get_x() + bar.get_width()/2., height + 0.01,
                     f'{height:.4f}',
                     ha='center', va='bottom', fontweight='bold')
    
    axs[0, 0].set_title(f"Tỷ lệ kích thước (Gốc = 1)")
    axs[0, 0].set_ylim(0, 1.2)
    
    # 2. Biểu đồ số lượng khối và template
    bars2 = axs[0, 1].bar(["Templates", "Khối"], 
                        [templates_count, blocks_processed], 
                        color=['orange', 'purple'])
    
    # Thêm số liệu lên biểu đồ
    for bar in bars2:
        height = bar.get_height()
        axs[0, 1].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                     f'{height:.0f}',
                     ha='center', va='bottom', fontweight='bold')
    
    axs[0, 1].set_title("Số lượng template và khối")
    
    # 3. Biểu đồ tỷ lệ hit
    axs[1, 0].pie([hit_ratio, 1-hit_ratio], 
                labels=['Trùng khớp', 'Không trùng khớp'],
                autopct='%1.1f%%', 
                colors=['lightgreen', 'lightcoral'])
    axs[1, 0].set_title(f"Tỷ lệ hit: {hit_ratio:.2f}")
    
    # 4. Biểu đồ CER và điểm tương đồng
    metrics = ['CER', 'Điểm tương đồng', 'Cost']
    values = [avg_cer, avg_similarity, cost]
    colors = ['red', 'blue', 'purple']
    
    bars3 = axs[1, 1].bar(metrics, values, color=colors)
    
    # Thêm số liệu lên biểu đồ
    for bar in bars3:
        height = bar.get_height()
        axs[1, 1].text(bar.get_x() + bar.get_width()/2., height + 0.01,
                     f'{height:.4f}',
                     ha='center', va='bottom', fontweight='bold')
                     
    axs[1, 1].set_title("Các chỉ số chất lượng nén")
    axs[1, 1].set_ylim(0, 1.2)
    
    # 5. Biểu đồ xu hướng nén lý tưởng
    # Tạo biểu đồ mục tiêu lý tưởng (high hit ratio, high similarity, low CER)
    ideal_metrics = ['Tỷ lệ Hit', 'Điểm tương đồng', 'Kháng CER']
    current_values = [hit_ratio, avg_similarity, 1.0 - min(1.0, avg_cer / 0.15)]  # Kháng CER = 1 - normalized CER
    
    # Tạo biểu đồ radar
    angles = np.linspace(0, 2*np.pi, len(ideal_metrics), endpoint=False).tolist()
    angles += angles[:1]  # Đóng vòng tròn
    
    current_values += current_values[:1]  # Đóng vòng tròn cho giá trị
    
    ax_radar = axs[2, 0]
    ax_radar.plot(angles, current_values, 'o-', linewidth=2, color='green')
    ax_radar.fill(angles, current_values, alpha=0.25, color='green')
    ax_radar.set_thetagrids(np.degrees(angles[:-1]), ideal_metrics)
    ax_radar.set_ylim(0, 1)
    ax_radar.set_title("Biểu đồ đánh giá hiệu suất nén")
    ax_radar.grid(True)
    
    # 6. Thông tin tổng hợp
    axs[2, 1].axis('off')  # Tắt trục tọa độ
    
    # Lấy thông tin thời gian từ timestamp nếu có
    timestamp = compression_data.get("timestamp", "N/A")
    device_id = compression_data.get("device_id", "N/A")
    
    # Tạo textbox thông tin
    compression_info = f"""
    THÔNG TIN TỔNG HỢP NÉN
    -----------------------
    ID nén: {compression_data.get('id', 'N/A')}
    Thiết bị: {device_id}
    
    Số lượng giá trị gốc: {total_values}
    Số lượng template: {templates_count}
    Số lượng khối: {blocks_processed}
    
    Tỷ lệ hit: {hit_ratio:.4f}
    Tỷ lệ nén: {compression_ratio:.4f}
    Điểm tương đồng: {avg_similarity:.4f}
    CER: {avg_cer:.4f}
    Cost: {cost:.4f}
    
    Ghi chú: {compression_data.get('notes', 'Sử dụng thuật toán nén đã cải tiến')}
    """
    
    axs[2, 1].text(0.5, 0.5, compression_info, 
                 horizontalalignment='center', 
                 verticalalignment='center', 
                 transform=axs[2, 1].transAxes,
                 fontsize=12,
                 family='monospace',
                 bbox=dict(boxstyle="round,pad=1", facecolor="white", alpha=0.8))
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    if output_prefix:
        plt.savefig(f"{output_prefix}_summary.png", bbox_inches='tight', dpi=300)
    
    plt.close()

def analyze_similarity_metrics(compression_data: Dict[str, Any], output_prefix: str = None):
    """
    Phân tích chi tiết về các chỉ số tương đồng và hiệu suất của thuật toán nén đã cải tiến
    
    Args:
        compression_data: Dict chứa dữ liệu nén
        output_prefix: Tiền tố cho tên file biểu đồ
    """
    # Trích xuất thông tin từ compressed_data
    compressed_data_json = compression_data.get("compressed_data", {})
    if isinstance(compressed_data_json, str):
        try:
            compressed_data = json.loads(compressed_data_json)
        except json.JSONDecodeError:
            logger.error("Lỗi giải mã JSON từ trường compressed_data")
            return
    else:
        compressed_data = compressed_data_json
        
    # Lấy các dữ liệu cần thiết
    similarity_scores = compression_data.get("similarity_scores", [])
    cer_values = compression_data.get("cer_values", [])
    cost_values = compressed_data.get("cost_values", [])
    
    # Kiểm tra nếu không có dữ liệu
    if not similarity_scores and not cer_values:
        logger.warning("Không có dữ liệu về các chỉ số tương đồng để phân tích")
        return
        
    # Tạo biểu đồ phân tích chi tiết
    fig, axs = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Biểu đồ tương quan giữa điểm tương đồng và CER
    if similarity_scores and cer_values:
        # Lấy số lượng mẫu tối thiểu từ hai danh sách
        min_len = min(len(similarity_scores), len(cer_values))
        if min_len > 0:
            x = similarity_scores[:min_len]
            y = cer_values[:min_len]
            
            # Vẽ biểu đồ scatter
            axs[0, 0].scatter(x, y, alpha=0.5, color='blue')
            axs[0, 0].set_title("Tương quan giữa Điểm tương đồng và CER")
            axs[0, 0].set_xlabel("Điểm tương đồng")
            axs[0, 0].set_ylabel("CER")
            
            # Thêm đường xu hướng
            try:
                z = np.polyfit(x, y, 1)
                p = np.poly1d(z)
                axs[0, 0].plot(x, p(x), "r--", alpha=0.8)
                
                # Tính hệ số tương quan
                corr = np.corrcoef(x, y)[0, 1]
                axs[0, 0].text(0.05, 0.95, f"Hệ số tương quan: {corr:.4f}", 
                            transform=axs[0, 0].transAxes,
                            fontsize=10, verticalalignment='top',
                            bbox=dict(boxstyle="round", alpha=0.1))
            except:
                logger.warning("Không thể tính đường xu hướng cho tương quan Similarity-CER")
                
    # 2. Biểu đồ phân bố điểm tương đồng
    if similarity_scores:
        axs[0, 1].hist(similarity_scores, bins=15, color='green', alpha=0.7)
        axs[0, 1].set_title("Phân bố điểm tương đồng")
        axs[0, 1].set_xlabel("Điểm tương đồng")
        axs[0, 1].set_ylabel("Số lượng")
        axs[0, 1].axvline(x=0.4, color='red', linestyle='--', label='Ngưỡng 0.4')
        axs[0, 1].legend()
    
    # 3. Biểu đồ theo dõi Cost function
    if cost_values:
        axs[1, 0].plot(range(len(cost_values)), cost_values, marker='o', linestyle='-', color='purple', alpha=0.6)
        axs[1, 0].set_title("Giá trị Cost function")
        axs[1, 0].set_xlabel("Lần tính cost")
        axs[1, 0].set_ylabel("Cost value")
        axs[1, 0].grid(True)
    
    # 4. Thêm bảng thống kê tóm tắt
    axs[1, 1].axis('off')  # Tắt trục tọa độ
    
    # Tính các thống kê
    sim_stats = {
        'Trung bình': np.mean(similarity_scores) if similarity_scores else "N/A",
        'Trung vị': np.median(similarity_scores) if similarity_scores else "N/A",
        'Min': np.min(similarity_scores) if similarity_scores else "N/A",
        'Max': np.max(similarity_scores) if similarity_scores else "N/A",
        'Std': np.std(similarity_scores) if similarity_scores else "N/A"
    }
    
    cer_stats = {
        'Trung bình': np.mean(cer_values) if cer_values else "N/A",
        'Trung vị': np.median(cer_values) if cer_values else "N/A",
        'Min': np.min(cer_values) if cer_values else "N/A",
        'Max': np.max(cer_values) if cer_values else "N/A",
        'Std': np.std(cer_values) if cer_values else "N/A"
    }
    
    # Tạo bảng thống kê
    stats_text = f"""
    THỐNG KÊ CHI TIẾT CÁC CHỈ SỐ
    ----------------------------
    
    Điểm tương đồng (Similarity):
       Trung bình: {sim_stats['Trung bình'] if isinstance(sim_stats['Trung bình'], str) else f"{sim_stats['Trung bình']:.4f}"}
       Trung vị: {sim_stats['Trung vị'] if isinstance(sim_stats['Trung vị'], str) else f"{sim_stats['Trung vị']:.4f}"}
       Min: {sim_stats['Min'] if isinstance(sim_stats['Min'], str) else f"{sim_stats['Min']:.4f}"}
       Max: {sim_stats['Max'] if isinstance(sim_stats['Max'], str) else f"{sim_stats['Max']:.4f}"}
       Độ lệch chuẩn: {sim_stats['Std'] if isinstance(sim_stats['Std'], str) else f"{sim_stats['Std']:.4f}"}
    
    CER (Compression Error Rate):
       Trung bình: {cer_stats['Trung bình'] if isinstance(cer_stats['Trung bình'], str) else f"{cer_stats['Trung bình']:.4f}"}
       Trung vị: {cer_stats['Trung vị'] if isinstance(cer_stats['Trung vị'], str) else f"{cer_stats['Trung vị']:.4f}"}
       Min: {cer_stats['Min'] if isinstance(cer_stats['Min'], str) else f"{cer_stats['Min']:.4f}"}
       Max: {cer_stats['Max'] if isinstance(cer_stats['Max'], str) else f"{cer_stats['Max']:.4f}"}
       Độ lệch chuẩn: {cer_stats['Std'] if isinstance(cer_stats['Std'], str) else f"{cer_stats['Std']:.4f}"}
       
    Mẫu dữ liệu:
       Số mẫu Similarity: {len(similarity_scores)}
       Số mẫu CER: {len(cer_values)}
       Số mẫu Cost: {len(cost_values)}
    """
    
    axs[1, 1].text(0.5, 0.5, stats_text, 
                 horizontalalignment='center', 
                 verticalalignment='center', 
                 transform=axs[1, 1].transAxes,
                 fontsize=10,
                 family='monospace',
                 bbox=dict(boxstyle="round,pad=1", facecolor="white", alpha=0.8))
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    if output_prefix:
        plt.savefig(f"{output_prefix}_similarity_metrics.png", bbox_inches='tight', dpi=300)
    
    plt.close()

def extract_time_info(compression_result):
    """
    Extract time information from compression_result
    
    Args:
        compression_result: Compression result dictionary
        
    Returns:
        str: Formatted time information string or empty string if not available
    """
    time_info = ""
    
    # Try to get time_range from compression_result
    if 'time_range' in compression_result:
        time_range = compression_result['time_range']
        
        # If time_range is a dictionary with min and max keys (new format)
        if isinstance(time_range, dict) and 'min' in time_range and 'max' in time_range:
            time_info = f" (Data: {time_range['min']} - {time_range['max']})"
        
        # If time_range is a string (old format)
        elif isinstance(time_range, str):
            # Handle PostgreSQL tsrange format [start,end]
            if time_range.startswith('[') and time_range.endswith(']'):
                times = time_range[1:-1].split(',')
                if len(times) == 2:
                    start_time = times[0].strip('"\'')
                    end_time = times[1].strip('"\'')
                    time_info = f" (Data: {start_time} - {end_time})"
    
    return time_info

def create_pattern_recognition_chart(data, compression_result, output_dir):
    """
    Create pattern recognition chart showing how templates are identified in the data
    
    Args:
        data: Original data
        compression_result: Compression result dictionary
        output_dir: Output directory for charts
        
    Returns:
        str: Path to the created chart, or None if no data available
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract information from compression_result
        templates = compression_result.get('templates', {})
        encoded_stream = compression_result.get('encoded_stream', [])
        
        # Get time range information
        time_info = extract_time_info(compression_result)
        
        # Create file path
        pattern_recognition_chart = os.path.join(output_dir, 'template_recognition.png')
        
        # Return early if no templates or encoded stream data available
        if not templates or not encoded_stream:
            logger.warning("No template or encoded stream data available for chart")
            return None
            
        # Prepare data for visualization
        primary_dim = None
        dimensions = {}
        
        # Detect available data dimensions
        if len(data) > 0 and isinstance(data[0], dict):
            # Multi-dimensional data, check available dimensions
            for record in data:
                for dim, value in record.items():
                    if value is not None:
                        dimensions[dim] = dimensions.get(dim, 0) + 1
            
            if dimensions:
                # Sort by value count descending
                sorted_dims = sorted(dimensions.items(), key=lambda x: x[1], reverse=True)
                primary_dim = sorted_dims[0][0]
                logger.info(f"Selected primary dimension for visualization: {primary_dim}")
        
        # Extract and prepare data for plotting
        if len(data) > 0 and isinstance(data[0], dict):
            # Multi-dimensional data
            primary_data = [record.get(primary_dim, None) for record in data]
            # Remove None values
            primary_data = [val for val in primary_data if val is not None]
        else:
            # One-dimensional data
            primary_data = data
            
        # Create figure for pattern recognition visualization
        plt.figure(figsize=(15, 8))
        
        # Draw original data
        plt.plot(primary_data, color='blue', alpha=0.5, label='Original Data')
        
        # Get data limits for label positioning
        y_min = min(primary_data) if primary_data else 0
        y_max = max(primary_data) if primary_data else 100
        
        # Create a colormap for templates
        cmap = plt.cm.tab20
        
        # Track templates for legend
        template_seen = {}
        
        # Highlight template regions
        for block in encoded_stream:
            template_id = block.get('template_id')
            start_idx = block.get('start_idx')
            length = block.get('length')
            similarity = block.get('similarity_score', 0)
            
            if template_id is not None and start_idx is not None and length is not None:
                # Find y-range for this region
                if start_idx + length <= len(primary_data):
                    segment_data = primary_data[start_idx:start_idx + length]
                    segment_min = min(segment_data) if segment_data else y_min
                    segment_max = max(segment_data) if segment_data else y_max
                    segment_middle = (segment_min + segment_max) / 2
                else:
                    segment_middle = (y_min + y_max) / 2
                
                # Choose a color for this template from the colormap
                color_idx = template_id % 20
                template_color = cmap(color_idx)
                
                # Mark the template region with slightly transparent color
                plt.axvspan(start_idx, start_idx + length, 
                           color=template_color, alpha=0.3)
                
                # Add a dotted line to emphasize template region
                if length > 5:  # Only draw marker line if block is large enough
                    middle_x = start_idx + length/2
                    plt.axvline(x=middle_x, color=template_color, linestyle='--', alpha=0.7)
                
                # Label the template with a prominent text box
                template_label = f'T{template_id}'
                
                # Add template label with background
                middle_x = start_idx + length/2
                
                plt.text(middle_x, segment_middle, template_label,
                        horizontalalignment='center', verticalalignment='center',
                        fontsize=11, fontweight='bold', color='black',
                        bbox=dict(facecolor=template_color, alpha=0.7, boxstyle='round,pad=0.5', 
                                 edgecolor='black', linewidth=1))
                
                # Track templates seen for legend
                if template_id not in template_seen:
                    template_seen[template_id] = template_color
        
        # Add titles and labels
        title = 'Pattern Recognition and Data Segmentation'
        if time_info:
            title += time_info
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel('Sample Index', fontsize=12)
        plt.ylabel('Data Value', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Add summary information at the bottom
        hit_ratio = compression_result.get('hit_ratio', 0)
        compression_ratio = compression_result.get('compression_ratio', 0)
        avg_similarity = compression_result.get('avg_similarity', 0)
        
        info_text = (f"Templates: {len(templates)} | Hit Ratio: {hit_ratio:.2f} | " 
                    f"Compression Ratio: {compression_ratio:.2f}x | Avg Similarity: {avg_similarity:.2f}")
        
        plt.figtext(0.5, 0.01, info_text, ha='center', va='bottom',
                   fontsize=11, fontweight='bold',
                   bbox=dict(facecolor='white', alpha=0.7, boxstyle='round,pad=0.5', edgecolor='gray'))
        
        # Add a light background color
        plt.gcf().patch.set_facecolor('#f8f9fa')
        
        # Adjust layout and save
        plt.tight_layout(rect=[0, 0.03, 1, 0.97])
        plt.savefig(pattern_recognition_chart, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Created template recognition chart: {pattern_recognition_chart}")
        return pattern_recognition_chart
        
    except Exception as e:
        logger.error(f"Error creating pattern recognition chart: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_block_size_chart(compression_result, output_dir):
    """
    Create block size adjustment chart
    
    Args:
        compression_result: Compression result
        output_dir: Output directory for charts
        
    Returns:
        str: Path to the created chart, or None if no block_sizes data available
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get block size information from compression_result
        # Check different keys that might contain block size information
        block_sizes = None
        
        # Try to get from 'block_sizes' (new format)
        if 'block_sizes' in compression_result and compression_result['block_sizes']:
            raw_block_sizes = compression_result['block_sizes']
            
            # Check if block_sizes is a list of dictionaries or a list of numbers
            if len(raw_block_sizes) > 0 and isinstance(raw_block_sizes[0], dict):
                # Extract 'size' from dictionaries
                block_sizes = [b.get('size', 0) if isinstance(b, dict) else (int(b) if isinstance(b, (int, float, str)) else 0) 
                              for b in raw_block_sizes]
                logger.info(f"Converted block_sizes from dictionary format to numeric list with {len(block_sizes)} values")
            else:
                # Ensure all elements are numbers
                block_sizes = [int(b) if isinstance(b, (int, float, str)) else 0 for b in raw_block_sizes]
                logger.info(f"Found block_sizes with {len(block_sizes)} values")
        
        # Try to get from 'block_size_history' (old format)
        elif 'block_size_history' in compression_result and compression_result['block_size_history']:
            block_size_history = compression_result['block_size_history']
            # Check if block_size_history is a list of dictionaries or a list of numbers
            if len(block_size_history) > 0 and isinstance(block_size_history[0], dict):
                # List of dictionaries
                block_sizes = [b.get('new_size', 0) if isinstance(b, dict) else 0 for b in block_size_history]
            else:
                # List of integers
                block_sizes = [int(b) if isinstance(b, (int, float, str)) else 0 for b in block_size_history]
            logger.info(f"Found block_size_history with {len(block_sizes)} values")
        
        if not block_sizes or len(block_sizes) == 0:
            logger.warning("No block size history data available for chart")
            return None
        
        # Get min block size from configuration
        min_size = compression_result.get('min_block_size', 10)
        
        # Process block sizes: Filter out zeros and use min_size instead
        processed_block_sizes = []
        last_valid_size = min_size  # Start with min_size as default
        indices = []  # Keep track of indices for plotting
        
        for i, size in enumerate(block_sizes):
            if size <= 0:  # Replace zeros and negative values with the last valid size
                if i > 0:  # If not the first element
                    processed_block_sizes.append(last_valid_size)
                else:
                    processed_block_sizes.append(min_size)  # Use min_size for the first element
            else:
                processed_block_sizes.append(size)
                last_valid_size = size
            indices.append(i)
            
        # Log the processed values to debug
        logger.info(f"Processed block sizes for charting: {processed_block_sizes[:10]}...")
            
        # Get time information
        time_info = extract_time_info(compression_result)
            
        # Create file path
        block_size_chart = os.path.join(output_dir, 'block_size_adjustment.png')
        
        plt.figure(figsize=(12, 6))
        
        # Plot block size adjustment history with processed values
        # Loại bỏ marker='o' và thêm linewidth để đường thẳng nổi bật hơn
        plt.plot(indices, processed_block_sizes, linestyle='-', color='green', linewidth=2.0, alpha=0.8)
        
        # Draw min and max thresholds from compression_result
        min_size = compression_result.get('min_block_size', 0)
        max_size = compression_result.get('max_block_size', 0)
        
        # If not found in compression_result, try to set defaults
        if (min_size == 0 or max_size == 0) and len(processed_block_sizes) > 0:
            # Set default values if not found
            min_size = min_size or 10  # Default value
            max_size = max_size or 120  # Default value
        
        if min_size > 0:
            plt.axhline(y=min_size, color='red', linestyle='--', label=f'Min: {min_size}')
        if max_size > 0:
            plt.axhline(y=max_size, color='blue', linestyle='--', label=f'Max: {max_size}')
        
        # Add time information to title
        title = f"Block Size Adjustment{time_info}"
        plt.title(title)
        plt.xlabel("Block Index")
        plt.ylabel("Block Size")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(block_size_chart, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Created block size adjustment chart: {block_size_chart}")
        return block_size_chart
        
    except Exception as e:
        logger.error(f"Error creating block size adjustment chart: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_size_comparison_chart(data, compression_result, output_dir='visualization', database_size_info=None):
    """
    Tạo biểu đồ so sánh kích thước giữa dữ liệu gốc và dữ liệu nén dựa trên thông tin từ database
    
    Args:
        data: Dữ liệu gốc (sử dụng cho tên file nếu không có device_id)
        compression_result: Kết quả nén (dùng để lấy device_id và compression_id)
        output_dir: Thư mục đầu ra
        database_size_info: Thông tin kích thước từ database (nếu đã có)
    
    Returns:
        str: Đường dẫn đến biểu đồ đã tạo hoặc None nếu không thể tạo biểu đồ
    """
    # Tạo thư mục đầu ra nếu chưa tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    # Hàm định dạng bytes cho hiển thị
    def format_bytes(bytes, pos):
        if bytes < 1024:
            return f"{bytes:.0f} B"
        elif bytes < 1024**2:
            return f"{bytes/1024:.1f} KB"
        elif bytes < 1024**3:
            return f"{bytes/1024**2:.1f} MB"
        else:
            return f"{bytes/1024**3:.1f} GB"
    
    # Nếu đã có thông tin kích thước từ database, sử dụng nó
    if database_size_info:
        original_size = database_size_info.get('original_size_bytes', 0)
        compressed_size = database_size_info.get('compressed_size_bytes', 0)
        compression_ratio = database_size_info.get('compression_ratio', 1.0)
        logger.info(f"Sử dụng kích thước từ database: Original={original_size/1024:.2f}KB, Compressed={compressed_size/1024:.2f}KB, Ratio={compression_ratio:.2f}x")
    else:
        # Nếu không có thông tin từ database, lấy từ database
        device_id = compression_result.get('device_id')
        compression_id = compression_result.get('compression_id')
        
        if not device_id or not compression_id:
            logger.error("Không có device_id hoặc compression_id, không thể lấy kích thước từ database")
            return None
        
        # Kết nối đến database
        engine = get_database_connection()
        if not engine:
            logger.error("Không thể kết nối đến database")
            return None
        
        # Lấy kích thước từ database
        database_info = get_data_size_from_database(compression_id, device_id)
        if not database_info:
            logger.error("Không thể lấy thông tin kích thước từ database")
            return None
            
        original_size = database_info.get('original_size_bytes', 0)
        compressed_size = database_info.get('compressed_size_bytes', 0)
        compression_ratio = database_info.get('compression_ratio', 1.0)
    
    # Kiểm tra nếu không có dữ liệu hợp lệ
    if original_size <= 0 or compressed_size <= 0:
        logger.error(f"Dữ liệu kích thước không hợp lệ: Original={original_size}, Compressed={compressed_size}")
        return None
    
    # Lấy thông tin thời gian
    time_info = extract_time_info(compression_result)
    
    # Tạo biểu đồ
    plt.figure(figsize=(10, 6))
    
    # Chuẩn bị màu sắc cầu vồng cho dữ liệu gốc
    rainbow_colors = ['#FF0000', '#FF7F00', '#FFFF00', '#00FF00', '#0000FF', '#4B0082', '#9400D3']  # Đỏ, Cam, Vàng, Lục, Lam, Chàm, Tím
    
    # Chia dữ liệu gốc thành 7 phần bằng nhau
    original_parts = [original_size / 7] * 7
    
    # Tạo vị trí cho các cột
    x = np.array([0, 1])  # Vị trí trục x cho hai cột
    width = 0.6  # Độ rộng cột
    
    # Vẽ 7 phần của dữ liệu gốc với màu sắc cầu vồng
    bottom = 0
    for i, (part, color) in enumerate(zip(original_parts, rainbow_colors)):
        plt.bar(x[0], part, width, bottom=bottom, color=color, edgecolor='white', linewidth=0.5)
        bottom += part
    
    # Vẽ cột dữ liệu nén
    bar_compressed = plt.bar(x[1], compressed_size, width, color='green')
    
    # Đặt nhãn cho trục x
    plt.xticks(x, ["Original data", "Compressed data"])
    
    plt.title(f"Compare data {time_info}", fontsize=14)
    plt.ylabel("Size")
    plt.grid(axis='y', alpha=0.3)
    
    # Format y-axis with bytes formatter
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_bytes))
    
    # Add source of size information
    size_info_text = f"(From database)"
    plt.annotate(size_info_text, xy=(0.05, 0.95), xycoords='axes fraction',
                ha='left', va='top', fontsize=9, 
                bbox=dict(boxstyle='round,pad=0.3', fc='white', alpha=0.7))
    
    # Thêm thông tin tỷ lệ nén và kích thước
    # Chú thích cho dữ liệu gốc
    plt.annotate(f"{format_bytes(original_size, 0)}",
                xy=(x[0], original_size),
                xytext=(0, 5),
                textcoords="offset points",
                ha='center', va='bottom',
                fontweight='bold')
    
    # Chú thích cho dữ liệu nén (kèm tỷ lệ nén)
    plt.annotate(f"{format_bytes(compressed_size, 0)}\n({compression_ratio:.2f}x)",
                xy=(x[1], compressed_size),
                xytext=(0, 5),
                textcoords="offset points",
                ha='center', va='bottom',
                fontweight='bold')

    
    # Save the chart
    chart_path = os.path.join(output_dir, 'size_comparison.png')
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
    plt.close()
    
    logger.info(f"Đã tạo biểu đồ so sánh kích thước tại: {chart_path}")
    return chart_path

def get_data_size_from_database(compression_id=None, device_id=None):
    """
    Truy vấn kích thước dữ liệu từ database cho một thiết bị cụ thể
    
    Args:
        compression_id: ID của bản ghi nén
        device_id: ID của thiết bị
        
    Returns:
        dict: Thông tin về kích thước gốc, kích thước nén và tỷ lệ nén
        None: Nếu không tìm thấy dữ liệu hoặc có lỗi
    """
    try:
        # Khởi tạo kết nối
        engine = get_database_connection()
        if not engine:
            logger.error("Không thể kết nối đến database để truy vấn kích thước")
            return None
            
        from sqlalchemy import text
        
        with engine.connect() as conn:
            # 1. Kiểm tra và lấy device_id từ compression_id nếu chưa có
            if compression_id and not device_id:
                query = """
                SELECT device_id FROM compressed_data_optimized WHERE id = :compression_id
                """
                result = conn.execute(text(query), {"compression_id": compression_id}).fetchone()
                if result:
                    device_id = result[0]
                    logger.info(f"Đã lấy device_id={device_id} từ compression_id={compression_id}")
                else:
                    logger.error(f"Không tìm thấy bản ghi nén với ID: {compression_id}")
                    return None
            
            # Nếu không có device_id, không thể tiếp tục
            if not device_id:
                logger.error("Không có device_id để truy vấn kích thước. Vui lòng chỉ định device_id.")
                return None
            
            # 2. Lấy thông tin bản nén từ compressed_data_optimized
            query_compressed = """
            SELECT 
                id,
                pg_column_size(templates) as templates_size,
                pg_column_size(encoded_stream) as encoded_size,
                pg_column_size(compression_metadata) as metadata_size
            FROM compressed_data_optimized 
            WHERE device_id = :device_id
            """
            
            # Nếu có compression_id, thêm điều kiện
            if compression_id:
                query_compressed += " AND id = :compression_id"
                params = {"device_id": device_id, "compression_id": compression_id}
            else:
                # Lấy bản ghi mới nhất
                query_compressed += " ORDER BY timestamp DESC LIMIT 1"
                params = {"device_id": device_id}
                
            result_compressed = conn.execute(text(query_compressed), params).fetchone()
            
            if not result_compressed:
                logger.error(f"Không tìm thấy bản ghi nén cho device_id: {device_id}")
                return None
                
            compression_id = result_compressed[0]
            templates_size = result_compressed[1] or 0
            encoded_size = result_compressed[2] or 0
            metadata_size = result_compressed[3] or 0
            
            # 3. Lấy kích thước dữ liệu gốc từ original_samples
            query_original = """
            SELECT 
                COUNT(*) as row_count,
                SUM(pg_column_size(original_data)) as original_size
            FROM original_samples 
            WHERE device_id = :device_id
            """
            
            result_original = conn.execute(text(query_original), {"device_id": device_id}).fetchone()
            
            if not result_original or not result_original[1]:
                logger.error(f"Không tìm thấy dữ liệu gốc cho device_id: {device_id}")
                return None
                
            row_count = result_original[0] or 0
            original_size = result_original[1] or 0
            
            # Tính tổng kích thước nén
            compressed_size = templates_size + encoded_size + metadata_size
            
            # Tính tỷ lệ nén thực tế
            compression_ratio = original_size / max(1, compressed_size)
            
            # Tạo kết quả
            result = {
                'compression_id': compression_id,
                'device_id': device_id,
                'original_size_bytes': original_size,
                'compressed_size_bytes': compressed_size,
                'compression_ratio': compression_ratio,
                'row_count': row_count,
                'templates_size': templates_size,
                'encoded_size': encoded_size,
                'metadata_size': metadata_size
            }
            
            logger.info(f"""
            Kích thước dữ liệu từ database:
            - Device ID: {device_id}
            - Compression ID: {compression_id}
            - Kích thước gốc: {original_size/1024:.2f} KB ({row_count} hàng)
            - Kích thước nén: {compressed_size/1024:.2f} KB (templates: {templates_size/1024:.2f} KB, encoded: {encoded_size/1024:.2f} KB, metadata: {metadata_size/1024:.2f} KB)
            - Tỷ lệ nén: {compression_ratio:.2f}x
            """)
            
            return result
            
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn kích thước dữ liệu từ database: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_visualizations(data, compression_result, output_dir='visualization', max_points=5000, sampling_method='adaptive', num_chunks=0, time_info=None, compression_id=None, device_id=None):
    """
    Tạo biểu đồ trực quan hóa từ kết quả nén
    
    Args:
        data: Dữ liệu gốc
        compression_result: Kết quả từ quá trình nén
        output_dir: Thư mục đầu ra cho biểu đồ trực quan hóa
        max_points: Số điểm tối đa để hiển thị trên biểu đồ
        sampling_method: Phương pháp lấy mẫu dữ liệu cho biểu đồ
        num_chunks: Số chunks để chia dữ liệu khi lấy mẫu
        time_info: Thông tin về thời gian (min_time, max_time)
        compression_id: ID của bản ghi nén trong database (để lấy kích thước thực tế)
        device_id: ID của thiết bị (để lấy kích thước thực tế)
        
    Returns:
        list: Danh sách các đường dẫn file biểu đồ đã tạo
    """
    import os
    import matplotlib.pyplot as plt
    
    # Khởi tạo danh sách tên file biểu đồ
    chart_files = []
    
    # Tạo thư mục đầu ra nếu chưa tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    # Ghi thêm thông tin về thời gian nếu có
    if time_info and isinstance(time_info, dict) and 'min_time' in time_info and 'max_time' in time_info:
        min_time = time_info['min_time']
        max_time = time_info['max_time']
        
        # Định dạng thời gian để hiển thị dễ đọc
        if min_time and max_time:
            min_time_str = min_time.strftime("%Y-%m-%d %H:%M:%S")
            max_time_str = max_time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Thêm phạm vi thời gian vào kết quả nén để sử dụng trong biểu đồ
            compression_result['time_range'] = {
                'min': min_time_str,
                'max': max_time_str
            }
            
            logger.info(f"Phạm vi thời gian dữ liệu: {min_time_str} đến {max_time_str}")
            
    # Lấy kích thước từ database nếu có compression_id hoặc device_id
    database_size_info = None
    if compression_id or device_id:
        logger.info(f"Đang truy vấn kích thước từ database (Compression ID: {compression_id}, Device ID: {device_id})")
        database_size_info = get_data_size_from_database(compression_id, device_id)
        
        if not database_size_info:
            logger.info(f"Sẽ sử dụng kích thước ước tính cho biểu đồ vì không thể lấy chính xác từ database")
            # Thiết lập nguồn kích thước để hiển thị trong biểu đồ
            compression_result['size_source'] = 'estimate'
        else:
            # Thiết lập nguồn kích thước là database nếu lấy thành công
            compression_result['size_source'] = 'database'
            logger.info(f"Đã lấy thông tin kích thước thành công từ database (ID: {database_size_info.get('compression_id')})")
    else:
        logger.info("Không có ID nén hoặc ID thiết bị, sẽ sử dụng kích thước ước tính cho biểu đồ")
        compression_result['size_source'] = 'estimate'
    
    # 1. Tạo biểu đồ nhận diện template
    try:
        chart_file = create_pattern_recognition_chart(data, compression_result, output_dir)
        if chart_file:
            chart_files.append(chart_file)
            logger.info(f"Đã tạo biểu đồ nhận diện template: {chart_file}")
    except Exception as e:
        logger.error(f"Lỗi khi tạo biểu đồ nhận diện template: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # 2. Tạo biểu đồ điều chỉnh kích thước block
    try:
        chart_file = create_block_size_chart(compression_result, output_dir)
        if chart_file:
            chart_files.append(chart_file)
            logger.info(f"Đã tạo biểu đồ điều chỉnh kích thước block: {chart_file}")
    except Exception as e:
        logger.error(f"Lỗi khi tạo biểu đồ điều chỉnh kích thước block: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # 3. Tạo biểu đồ so sánh kích thước
    try:
        chart_file = create_size_comparison_chart(data, compression_result, output_dir, database_size_info)
        if chart_file:
            chart_files.append(chart_file)
            logger.info(f"Đã tạo biểu đồ so sánh kích thước: {chart_file}")
    except Exception as e:
        logger.error(f"Lỗi khi tạo biểu đồ so sánh kích thước: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    return chart_files

def create_analysis_visualizations(compression_id: int):
    """
    Tạo tất cả các biểu đồ phân tích cho một bản ghi nén
    
    Args:
        compression_id: ID bản ghi nén
    """
    try:
        # Tạo thư mục lưu biểu đồ
        output_dir = f"compression_analysis_{compression_id}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Kết nối DB và lấy dữ liệu
        engine = get_database_connection()
        compression_data = get_compression_data(engine, compression_id)
        
        logger.info(f"Bắt đầu tạo biểu đồ phân tích cho bản ghi nén ID: {compression_id}")
        
        # Tạo tiền tố cho tên file biểu đồ
        output_prefix = f"{output_dir}/compression_{compression_id}"
        
        # Tạo các biểu đồ phân tích
        analyze_compression_ratio(compression_data, output_prefix)
        analyze_templates(compression_data, output_prefix)
        analyze_blocks(compression_data, output_prefix)
        analyze_parameter_adjustments(compression_data, output_prefix)
        analyze_memory_usage(compression_data, output_prefix)
        create_summary_chart(compression_data, output_prefix)
        analyze_similarity_metrics(compression_data, output_prefix)
        
        # Tạo các biểu đồ trực quan hóa
        visualizations = create_visualizations(compression_data.get('original_data', []), compression_data)
        
        logger.info(f"Đã tạo các biểu đồ phân tích và lưu vào thư mục: {output_dir}")
        
        return output_dir
        
    except Exception as e:
        logger.error(f"Lỗi khi tạo biểu đồ phân tích: {str(e)}")
        raise

def compare_compression_results(compression_ids: List[int], output_dir: str = None):
    """
    So sánh kết quả nén giữa nhiều bản nén khác nhau
    
    Args:
        compression_ids: Danh sách các ID bản ghi nén cần so sánh
        output_dir: Thư mục đầu ra để lưu biểu đồ
    
    Returns:
        Đường dẫn đến thư mục chứa biểu đồ so sánh
    """
    try:
        # Kiểm tra nếu không có ID nào được cung cấp
        if not compression_ids:
            logger.error("Không có ID bản ghi nén nào được cung cấp để so sánh")
            return None
            
        # Tạo thư mục đầu ra nếu chưa có
        if not output_dir:
            output_dir = f"comparison_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Kết nối DB
        engine = get_database_connection()
        
        # Lấy dữ liệu cho tất cả các bản ghi nén
        compression_data_list = []
        for comp_id in compression_ids:
            try:
                data = get_compression_data(engine, comp_id)
                compression_data_list.append(data)
                logger.info(f"Đã lấy dữ liệu nén ID: {comp_id}")
            except Exception as e:
                logger.warning(f"Không thể lấy dữ liệu cho ID {comp_id}: {str(e)}")
        
        # Kiểm tra nếu không thể lấy được bất kỳ dữ liệu nào
        if not compression_data_list:
            logger.error("Không thể lấy dữ liệu cho bất kỳ ID nào đã cung cấp")
            return None
            
        # Chuẩn bị dữ liệu để so sánh
        comp_ids = []
        hit_ratios = []
        comp_ratios = []
        template_counts = []
        avg_similarities = []
        avg_cers = []
        costs = []
        
        for data in compression_data_list:
            comp_id = data.get("id", "N/A")
            comp_ids.append(str(comp_id))
            hit_ratios.append(data.get("hit_ratio", 0))
            comp_ratios.append(data.get("compression_ratio", 0))
            template_counts.append(data.get("templates_count", 0))
            avg_similarities.append(data.get("avg_similarity", 0))
            avg_cers.append(data.get("avg_cer", 0))
            costs.append(data.get("cost", 0))
        
        # 1. Tạo biểu đồ so sánh tỷ lệ hit và tỷ lệ nén
        plt.figure(figsize=(14, 8))
        
        # Tính toán số lượng trụ cột và chiều rộng
        x = np.arange(len(comp_ids))
        width = 0.35
        
        # Trụ cột cho tỷ lệ hit
        plt.bar(x - width/2, hit_ratios, width, label='Tỷ lệ hit', color='skyblue')
        
        # Tạo trục y thứ hai cho tỷ lệ nén
        ax2 = plt.twinx()
        ax2.bar(x + width/2, comp_ratios, width, label='Tỷ lệ nén', color='lightgreen')
        
        # Thiết lập trục x
        plt.xticks(x, comp_ids, rotation=45)
        
        # Thêm nhãn và tiêu đề
        plt.xlabel('ID bản ghi nén')
        plt.ylabel('Tỷ lệ hit')
        ax2.set_ylabel('Tỷ lệ nén')
        
        # Thêm tiêu đề
        plt.title('So sánh tỷ lệ hit và tỷ lệ nén')
        
        # Thêm legend
        plt.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/comparison_hit_comp_ratio.png", bbox_inches='tight', dpi=300)
        plt.close()
        
        # 2. Biểu đồ so sánh chất lượng nén (CER và Similarity)
        plt.figure(figsize=(14, 8))
        
        # Trụ cột cho CER
        plt.bar(x - width/2, avg_cers, width, label='Trung bình CER', color='salmon')
        
        # Trục y thứ hai cho Similarity
        ax2 = plt.twinx()
        ax2.bar(x + width/2, avg_similarities, width, label='Trung bình Similarity', color='skyblue')
        
        # Thiết lập trục x
        plt.xticks(x, comp_ids, rotation=45)
        
        # Thêm nhãn và tiêu đề
        plt.xlabel('ID bản ghi nén')
        plt.ylabel('CER')
        ax2.set_ylabel('Similarity')
        
        # Thêm tiêu đề
        plt.title('So sánh chất lượng nén (CER và Similarity)')
        
        # Thêm legend
        plt.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/comparison_cer_similarity.png", bbox_inches='tight', dpi=300)
        plt.close()
        
        # 3. Biểu đồ so sánh số lượng template và cost
        plt.figure(figsize=(14, 8))
        
        # Trụ cột cho template count
        plt.bar(x - width/2, template_counts, width, label='Số lượng template', color='orchid')
        
        # Trục y thứ hai cho cost
        ax2 = plt.twinx()
        ax2.bar(x + width/2, costs, width, label='Cost', color='gold')
        
        # Thiết lập trục x
        plt.xticks(x, comp_ids, rotation=45)
        
        # Thêm nhãn và tiêu đề
        plt.xlabel('ID bản ghi nén')
        plt.ylabel('Số lượng template')
        ax2.set_ylabel('Cost')
        
        # Thêm tiêu đề
        plt.title('So sánh số lượng template và cost')
        
        # Thêm legend
        plt.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/comparison_templates_cost.png", bbox_inches='tight', dpi=300)
        plt.close()
        
        # 4. Biểu đồ radar cho so sánh tổng thể
        # Tạo dữ liệu radar chart
        fig = plt.figure(figsize=(14, 10))
        
        # Các chỉ số để so sánh
        metrics = ['Tỷ lệ hit', 'Tỷ lệ nén', 'Điểm tương đồng', 'Kháng CER', 'Kháng Cost']
        
        # Chuẩn hóa dữ liệu cho radar chart
        # Đảm bảo dữ liệu nằm trong khoảng [0, 1]
        radar_data = []
        for i, _ in enumerate(comp_ids):
            # Chuẩn hóa CER và Cost để có giá trị cao = tốt
            normalized_cer = 1.0 - min(1.0, avg_cers[i] / 0.15)  # Giả sử 0.15 là ngưỡng tối đa cho CER
            normalized_cost = 1.0 - min(1.0, costs[i])  # Giả sử cost đã nằm trong [0, 1]
            
            # Chuẩn hóa tỷ lệ nén để phù hợp với thang đo radar
            normalized_comp_ratio = min(1.0, comp_ratios[i] / 10.0)  # Giả sử 10x là tỷ lệ nén tốt nhất
            
            radar_values = [
                hit_ratios[i],                # Tỷ lệ hit
                normalized_comp_ratio,        # Tỷ lệ nén chuẩn hóa
                avg_similarities[i],          # Điểm tương đồng
                normalized_cer,               # Kháng CER
                normalized_cost               # Kháng Cost
            ]
            radar_data.append(radar_values)
        
        # Góc cho mỗi trục
        angles = np.linspace(0, 2*np.pi, len(metrics), endpoint=False).tolist()
        
        # Đóng vòng tròn
        angles += angles[:1]
        
        # Tạo biểu đồ con
        ax = plt.subplot(111, polar=True)
        
        # Thêm dữ liệu cho mỗi bản ghi nén
        for i, values in enumerate(radar_data):
            values_closed = values + values[:1]  # Đóng vòng tròn cho giá trị
            ax.plot(angles, values_closed, linewidth=2, label=f'ID {comp_ids[i]}')
            ax.fill(angles, values_closed, alpha=0.1)
        
        # Thiết lập nhãn cho mỗi trục
        ax.set_thetagrids(np.degrees(angles[:-1]), metrics)
        
        # Thiết lập giới hạn trục r
        ax.set_ylim(0, 1)
        
        # Thêm tiêu đề
        plt.title('So sánh hiệu suất nén tổng thể')
        
        # Thêm legend
        plt.legend(loc='upper right', bbox_to_anchor=(1.2, 1.0))
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/comparison_radar.png", bbox_inches='tight', dpi=300)
        plt.close()
        
        # 5. Bảng so sánh chi tiết dạng văn bản
        # Tạo bảng thông tin chi tiết
        comparison_table = pd.DataFrame({
            'ID': comp_ids,
            'Tỷ lệ hit': hit_ratios,
            'Tỷ lệ nén': comp_ratios,
            'Số template': template_counts,
            'Avg Similarity': avg_similarities,
            'Avg CER': avg_cers,
            'Cost': costs
        })
        
        # Lưu bảng thành file CSV
        comparison_table.to_csv(f"{output_dir}/comparison_details.csv", index=False)
        
        # Tạo một bản tóm tắt dạng HTML để dễ xem
        html_table = comparison_table.to_html(index=False)
        with open(f"{output_dir}/comparison_details.html", 'w') as f:
            f.write(f"""
            <html>
            <head>
                <title>Bảng so sánh chi tiết</title>
                <style>
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ text-align: left; padding: 8px; border: 1px solid #ddd; }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    th {{ background-color: #4CAF50; color: white; }}
                </style>
            </head>
            <body>
                <h1>So sánh chi tiết các bản ghi nén</h1>
                {html_table}
                <p>Tạo lúc: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            </body>
            </html>
            """)
        
        logger.info(f"Đã tạo các biểu đồ so sánh và lưu vào thư mục: {output_dir}")
        return output_dir
        
    except Exception as e:
        logger.error(f"Lỗi khi so sánh kết quả nén: {str(e)}")
        return None

def main():
    """Hàm chính"""
    try:
        # Kiểm tra tham số dòng lệnh
        if len(sys.argv) < 2:
            print("Sử dụng: python visualization_analyzer.py <compression_id> [<compression_id2> ...]")
            print("hoặc:    python visualization_analyzer.py --compare <compression_id1> <compression_id2> [<compression_id3> ...]")
            print("Ví dụ:   python visualization_analyzer.py 1")
            print("Ví dụ:   python visualization_analyzer.py --compare 1 2 3")
            sys.exit(1)
            
        # Xác định chế độ hoạt động
        if sys.argv[1] == "--compare" and len(sys.argv) > 2:
            # Chế độ so sánh
            compression_ids = []
            for i in range(2, len(sys.argv)):
                try:
                    compression_id = int(sys.argv[i])
                    compression_ids.append(compression_id)
                except ValueError:
                    print(f"Lỗi: ID '{sys.argv[i]}' phải là số nguyên")
                    sys.exit(1)
            
            # Thực hiện so sánh
            output_dir = compare_compression_results(compression_ids)
            if output_dir:
                print(f"Đã tạo các biểu đồ so sánh và lưu vào thư mục: {output_dir}")
            else:
                print("Không thể tạo biểu đồ so sánh.")
                sys.exit(1)
        else:
            # Chế độ phân tích đơn lẻ
            try:
                compression_id = int(sys.argv[1])
            except ValueError:
                print("Lỗi: ID phải là số nguyên")
                sys.exit(1)
                
            # Tạo biểu đồ phân tích
            output_dir = create_analysis_visualizations(compression_id)
            print(f"Đã tạo các biểu đồ phân tích và lưu vào thư mục: {output_dir}")
            
    except Exception as e:
        logger.error(f"Lỗi: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
