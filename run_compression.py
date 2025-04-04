#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script trực tiếp để nén dữ liệu và tạo biểu đồ phân tích mà không cần kết nối database.
"""

import sys
import os
import argparse
import json
import logging
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# Thêm thư mục gốc vào sys.path để import các module cần thiết
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import thuật toán nén
from data_compression import DataCompressor

# Import module phân tích kết quả nén
try:
    import visualization_analyzer
except ImportError:
    visualization_analyzer = None

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("run_compression.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Lớp JSONEncoder tùy chỉnh cho việc chuyển đổi các kiểu dữ liệu NumPy và boolean
class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, bool):
            return int(obj)  # Chuyển đổi boolean thành 0/1
        elif isinstance(obj, set):
            return list(obj)  # Chuyển đổi set thành list
        return super(MyEncoder, self).default(obj)

def generate_test_data(n_points=1000, pattern='sine'):
    """
    Tạo dữ liệu test
    
    Args:
        n_points: Số điểm dữ liệu
        pattern: Loại dữ liệu ('sine', 'square', 'random')
        
    Returns:
        list: Danh sách các giá trị dữ liệu
    """
    x = np.linspace(0, 10, n_points)
    
    if pattern == 'sine':
        # Tạo sóng sin
        return np.sin(x) * 100 + np.random.normal(0, 5, n_points)
    elif pattern == 'square':
        # Tạo sóng vuông
        return np.sign(np.sin(x)) * 100 + np.random.normal(0, 5, n_points)
    else:
        # Dữ liệu ngẫu nhiên với một số pattern
        data = []
        pattern_length = 50
        num_patterns = n_points // pattern_length
        
        for i in range(num_patterns):
            if i % 3 == 0:
                # Mẫu 1: sine
                pattern_data = np.sin(np.linspace(0, 2*np.pi, pattern_length)) * 100
            elif i % 3 == 1:
                # Mẫu 2: square
                pattern_data = np.sign(np.sin(np.linspace(0, 2*np.pi, pattern_length))) * 100
            else:
                # Mẫu 3: random
                pattern_data = np.random.normal(0, 100, pattern_length)
                
            data.extend(pattern_data + np.random.normal(0, 5, pattern_length))
            
        return data[:n_points]

def load_data_from_file(file_path):
    """
    Đọc dữ liệu từ file
    
    Args:
        file_path: Đường dẫn đến file dữ liệu
        
    Returns:
        list: Danh sách các giá trị dữ liệu
    """
    try:
        # Kiểm tra định dạng file
        if file_path.endswith('.json'):
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            # Kiểm tra cấu trúc data
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'power' in data:
                return data['power']
            else:
                logger.error("Cấu trúc file JSON không hợp lệ. Cần danh sách giá trị hoặc dict với key 'power'.")
                return None
        elif file_path.endswith(('.csv', '.txt')):
            data = []
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        try:
                            value = float(line)
                            data.append(value)
                        except ValueError:
                            # Bỏ qua các dòng không phải số
                            pass
            return data
        else:
            logger.error(f"Định dạng file không được hỗ trợ: {file_path}")
            return None
    except Exception as e:
        logger.error(f"Lỗi khi đọc file dữ liệu: {str(e)}")
        return None

def save_compression_result(result, output_dir):
    """
    Lưu kết quả nén vào file JSON
    
    Args:
        result: Kết quả nén
        output_dir: Thư mục đầu ra
    """
    # Đảm bảo thư mục đầu ra tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    # Lưu kết quả dưới dạng JSON sử dụng MyEncoder tùy chỉnh
    with open(f"{output_dir}/compression_result.json", 'w') as f:
        json.dump(result, f, indent=2, cls=MyEncoder)
    
    logger.info(f"Đã lưu kết quả nén vào: {output_dir}/compression_result.json")

def create_original_data_chart(data, output_dir):
    """
    Tạo biểu đồ dữ liệu gốc
    
    Args:
        data: Dữ liệu gốc
        output_dir: Thư mục đầu ra
    """
    plt.figure(figsize=(10, 6))
    plt.plot(data)
    plt.title('Dữ liệu gốc')
    plt.xlabel('Chỉ số')
    plt.ylabel('Giá trị')
    plt.grid(True)
    
    # Lưu biểu đồ
    plt.savefig(f"{output_dir}/original_data.png")
    plt.close()
    
    logger.info(f"Đã tạo biểu đồ dữ liệu gốc: {output_dir}/original_data.png")

def create_custom_visualization(compression_result, output_dir):
    """
    Tạo biểu đồ phân tích kết quả nén tùy chỉnh
    
    Args:
        compression_result: Kết quả nén từ DataCompressor
        output_dir: Thư mục đầu ra
    """
    # Trích xuất dữ liệu từ kết quả nén
    templates = compression_result.get('templates', {})
    encoded_stream = compression_result.get('encoded_stream', [])
    compression_ratio = compression_result.get('compression_ratio', 1.0)
    blocks_processed = compression_result.get('blocks_processed', 0)
    hit_ratio = compression_result.get('hit_ratio', 0)
    total_values = compression_result.get('total_values', 0)
    avg_cer = compression_result.get('avg_cer', 0.0)
    avg_similarity = compression_result.get('avg_similarity', 0.0)
    
    # Biểu đồ phân tích
    fig, axs = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Biểu đồ so sánh kích thước
    original_size = total_values
    compressed_size = original_size / compression_ratio if compression_ratio else 0
    
    bars = axs[0, 0].bar(['Dữ liệu gốc', 'Dữ liệu đã nén'], [original_size, compressed_size], color=['blue', 'green'])
    axs[0, 0].set_title(f'So sánh kích thước dữ liệu (Tỷ lệ nén: {compression_ratio:.2f}x)')
    axs[0, 0].set_ylabel('Kích thước (giá trị)')
    
    # Thêm giá trị lên đầu thanh
    for bar in bars:
        height = bar.get_height()
        axs[0, 0].text(bar.get_x() + bar.get_width()/2., height, f'{int(height)}', ha='center', va='bottom')
    
    # 2. Biểu đồ CER và Similarity
    if 'cer_values' in compression_result and compression_result['cer_values']:
        cer_values = compression_result['cer_values']
        axs[0, 1].hist(cer_values, bins=20, color='red', alpha=0.7, label=f'Trung bình: {avg_cer:.4f}')
        axs[0, 1].set_title('Phân bố CER')
        axs[0, 1].set_xlabel('CER')
        axs[0, 1].set_ylabel('Số lượng')
        axs[0, 1].legend()
    
    if 'similarity_scores' in compression_result and compression_result['similarity_scores']:
        similarity_scores = compression_result['similarity_scores']
        axs[1, 0].hist(similarity_scores, bins=20, color='blue', alpha=0.7, label=f'Trung bình: {avg_similarity:.4f}')
        axs[1, 0].set_title('Phân bố điểm tương đồng')
        axs[1, 0].set_xlabel('Điểm tương đồng')
        axs[1, 0].set_ylabel('Số lượng')
        axs[1, 0].legend()
    
    # 3. Bảng thông tin tổng hợp
    axs[1, 1].axis('off')
    
    info_text = f"""
    THÔNG TIN TỔNG HỢP NÉN
    -----------------------
    Tổng số giá trị: {total_values}
    Số templates: {len(templates)}
    Số blocks: {blocks_processed}
    
    Tỷ lệ nén: {compression_ratio:.4f}x
    Tỷ lệ hit: {hit_ratio:.4f}
    Trung bình CER: {avg_cer:.4f}
    Trung bình Similarity: {avg_similarity:.4f}
    """
    
    axs[1, 1].text(0.5, 0.5, info_text, 
                ha='center', va='center', 
                bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.5), 
                fontsize=12, family='monospace')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/custom_analysis.png", dpi=300)
    plt.close()
    
    logger.info(f"Đã tạo biểu đồ phân tích tùy chỉnh: {output_dir}/custom_analysis.png")

def create_advanced_visualizations(compression_result, output_dir):
    """
    Tạo các biểu đồ phân tích nâng cao cho kết quả nén
    
    Args:
        compression_result: Kết quả nén từ DataCompressor
        output_dir: Thư mục đầu ra
    """
    # Sử dụng module visualization_analyzer để tạo biểu đồ
    # Tạo một đối tượng giả lập kết quả nén từ database
    compression_data = {
        'id': 0,
        'device_id': 'direct_compression',
        'template_id': 0,
        'compressed_data': compression_result,
        'total_values': compression_result.get('total_values', 0),
        'templates_count': len(compression_result.get('templates', {})),
        'blocks_processed': compression_result.get('blocks_processed', 0),
        'hit_ratio': compression_result.get('hit_ratio', 0),
        'compression_ratio': compression_result.get('compression_ratio', 1.0),
        'avg_cer': compression_result.get('avg_cer', 0.0),
        'avg_similarity': compression_result.get('avg_similarity', 0.0),
        'cost': compression_result.get('cost', 0.0),
        'similarity_scores': compression_result.get('similarity_scores', []),
        'cer_values': compression_result.get('cer_values', [])
    }
    
    # Tạo tiền tố cho tên file biểu đồ
    output_prefix = f"{output_dir}/compression"
    
    # Tạo các biểu đồ phân tích
    visualization_analyzer.analyze_compression_ratio(compression_data, output_dir)
    visualization_analyzer.analyze_templates(compression_data, output_prefix)
    visualization_analyzer.analyze_blocks(compression_data, output_prefix)
    visualization_analyzer.analyze_parameter_adjustments(compression_data, output_prefix)
    visualization_analyzer.analyze_memory_usage(compression_data, output_prefix)
    visualization_analyzer.create_summary_chart(compression_data, output_prefix)
    visualization_analyzer.analyze_similarity_metrics(compression_data, output_prefix)
    
    logger.info(f"Đã tạo các biểu đồ phân tích nâng cao và lưu vào thư mục: {output_dir}")

def create_pattern_recognition_chart(data, compression_result, output_dir):
    """
    Tạo một biểu đồ đơn giản thể hiện cách thuật toán nhận diện mẫu phân phối tương đồng
    và sự thay đổi kích thước khối theo thời gian
    
    Args:
        data: Dữ liệu gốc
        compression_result: Kết quả nén từ DataCompressor
        output_dir: Thư mục đầu ra
    """
    # Đảm bảo thư mục đầu ra tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    # Trích xuất thông tin từ kết quả nén
    encoded_stream = compression_result.get('encoded_stream', [])
    templates = compression_result.get('templates', {})
    hit_ratio = compression_result.get('hit_ratio', 0)
    compression_ratio = compression_result.get('compression_ratio', 1.0)
    avg_similarity = compression_result.get('avg_similarity', 0.0)
    block_size_history_raw = compression_result.get('block_size_history', [])
    
    # Xử lý block_size_history theo định dạng phù hợp
    block_size_history = []
    if block_size_history_raw:
        if isinstance(block_size_history_raw[0], dict):
            # Nếu là dictionary, lấy kích thước mới
            block_size_history = [item.get('new_size', 0) for item in block_size_history_raw]
        else:
            # Nếu đã là danh sách số
            block_size_history = block_size_history_raw
    
    # Tạo một figure với 2 hàng
    fig, axs = plt.subplots(2, 1, figsize=(16, 12), gridspec_kw={'height_ratios': [2, 1]})
    
    # --- Phần 1: Biểu đồ nhận diện mẫu template ---
    ax_main = axs[0]
    
    # Vẽ dữ liệu gốc
    ax_main.plot(data, 'b-', alpha=0.5, label='Dữ liệu gốc')
    
    # Đánh dấu các vị trí đã nhận diện template
    colors = ['red', 'green', 'purple', 'orange', 'brown', 'pink', 'gray', 'olive']
    
    # Sắp xếp encoded_stream theo template_id để các template giống nhau có cùng màu
    template_groups = {}
    for block in encoded_stream:
        tid = block.get('template_id')
        if tid not in template_groups:
            template_groups[tid] = []
        template_groups[tid].append(block)
    
    # Vẽ các vùng đánh dấu template
    for i, (tid, blocks) in enumerate(template_groups.items()):
        color_idx = i % len(colors)
        color = colors[color_idx]
        
        for block in blocks:
            start_idx = block.get('start_idx', 0)
            length = block.get('length', 0)
            similarity = block.get('similarity_score', 0)
            
            # Chỉ vẽ các block có độ tương đồng cao
            if similarity > 0.4:  # Ngưỡng để hiển thị
                end_idx = start_idx + length
                # Tạo một vùng highlight với màu tương ứng với template
                ax_main.axvspan(start_idx, end_idx, alpha=0.2, color=color)
                
                # Thêm text hiển thị template_id ở giữa block
                mid_point = start_idx + length/2
                y_pos = np.mean(data[start_idx:end_idx]) if end_idx <= len(data) else 0
                ax_main.text(mid_point, y_pos, f"T{tid}", 
                            fontsize=8, ha='center', va='center',
                            bbox=dict(boxstyle="round,pad=0.3", fc=color, alpha=0.3))
    
    ax_main.set_title(f'Nhận diện mẫu phân phối tương đồng trong dữ liệu\n(Tỷ lệ nhận diện: {hit_ratio:.2f}, Tỷ lệ nén: {compression_ratio:.2f}x, Điểm tương đồng TB: {avg_similarity:.3f})')
    ax_main.set_xlabel('Chỉ số dữ liệu')
    ax_main.set_ylabel('Giá trị')
    ax_main.grid(True, alpha=0.3)
    
    # Thêm chú thích cho các template chính
    legend_items = []
    for i, tid in enumerate(template_groups.keys()):
        if i < 10:  # Giới hạn số lượng template hiển thị trong legend
            color_idx = i % len(colors)
            color = colors[color_idx]
            legend_items.append(plt.Line2D([0], [0], color=color, lw=4, label=f'Template #{tid}'))
    
    if legend_items:
        ax_main.legend(handles=legend_items, loc='upper right', ncol=2)
    
    # --- Phần 2: Biểu đồ thay đổi kích thước khối theo thời gian ---
    ax_block = axs[1]
    
    if block_size_history:
        # Vẽ sự thay đổi kích thước khối theo thời gian
        x = range(len(block_size_history))
        ax_block.plot(x, block_size_history, 'ro-', linewidth=2)
        ax_block.set_title('Thay đổi kích thước khối (block) theo thời gian')
        ax_block.set_xlabel('Số lần điều chỉnh')
        ax_block.set_ylabel('Kích thước khối')
        ax_block.grid(True)
        
        # Thêm đường giới hạn trên/dưới
        if 'min_block_size' in compression_result:
            ax_block.axhline(y=compression_result['min_block_size'], color='b', linestyle='--', 
                          label=f"Min: {compression_result['min_block_size']}")
        if 'max_block_size' in compression_result:
            ax_block.axhline(y=compression_result['max_block_size'], color='g', linestyle='--',
                          label=f"Max: {compression_result['max_block_size']}") 
        
        # Đánh số các điểm
        for i, bs in enumerate(block_size_history):
            ax_block.annotate(f"{bs}", (i, bs), textcoords="offset points", 
                           xytext=(0,10), ha='center')
        
        ax_block.legend()
    else:
        ax_block.text(0.5, 0.5, "Không có dữ liệu thay đổi kích thước khối", 
                   ha='center', va='center', fontsize=12)
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    output_path = f"{output_dir}/compression_analysis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Đã tạo biểu đồ phân tích nén: {output_path}")
    return output_path

def run_compression(data, config=None, output_dir='compression_output'):
    """
    Thực hiện nén dữ liệu và tạo biểu đồ phân tích
    
    Args:
        data: Dữ liệu cần nén
        config: Cấu hình cho DataCompressor
        output_dir: Thư mục đầu ra
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    if data is None or len(data) == 0:
        logger.error("Không có dữ liệu để nén")
        return False
    
    try:
        # Khởi tạo compressor
        compressor = DataCompressor(config)
        
        # Thực hiện nén
        logger.info(f"Bắt đầu nén {len(data)} giá trị...")
        compression_result = compressor.compress(data)
        
        # Tạo thư mục đầu ra
        os.makedirs(output_dir, exist_ok=True)
        
        # Lưu kết quả nén
        save_compression_result(compression_result, output_dir)
        
        # Tạo một biểu đồ duy nhất thể hiện nhận diện mẫu phân phối tương đồng
        # và sự thay đổi kích thước khối theo thời gian
        chart_path = create_pattern_recognition_chart(data, compression_result, output_dir)
        
        # In thông tin tóm tắt
        logger.info(f"=== KẾT QUẢ NÉN ===")
        logger.info(f"Tổng số giá trị: {compression_result['total_values']}")
        logger.info(f"Số templates: {len(compression_result['templates'])}")
        logger.info(f"Số blocks: {compression_result['blocks_processed']}")
        logger.info(f"Tỷ lệ nén: {compression_result['compression_ratio']:.4f}x")
        logger.info(f"Tỷ lệ hit: {compression_result['hit_ratio']:.4f}")
        logger.info(f"CER trung bình: {compression_result['avg_cer']:.4f}")
        logger.info(f"Điểm tương đồng trung bình: {compression_result['avg_similarity']:.4f}")
        logger.info(f"Đã lưu kết quả và biểu đồ phân tích nén vào: {chart_path}")
        
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thực hiện nén dữ liệu: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Thiết lập parse tham số dòng lệnh
    parser = argparse.ArgumentParser(description='Nén dữ liệu trực tiếp và tạo biểu đồ phân tích.')
    parser.add_argument('--file', type=str, help='Đường dẫn đến file dữ liệu cần nén')
    parser.add_argument('--data-type', choices=['sine', 'square', 'random'], default='random',
                        help='Loại dữ liệu test nếu không cung cấp file')
    parser.add_argument('--num-points', type=int, default=1000, help='Số điểm dữ liệu test')
    parser.add_argument('--output-dir', type=str, default='compression_output',
                        help='Thư mục đầu ra cho kết quả và biểu đồ')
    
    # Parse tham số
    args = parser.parse_args()
    
    # Lấy dữ liệu
    if args.file:
        data = load_data_from_file(args.file)
        if data is None:
            logger.error("Không thể tải dữ liệu từ file. Sử dụng dữ liệu test thay thế.")
            data = generate_test_data(args.num_points, args.data_type)
    else:
        data = generate_test_data(args.num_points, args.data_type)
        logger.info(f"Đã tạo {len(data)} điểm dữ liệu test loại '{args.data_type}'")
    
    # Cấu hình nâng cao cho DataCompressor
    compression_config = {
        'p_threshold': 0.15,         # Ngưỡng p-value (cao hơn để dễ khớp template)
        'max_acceptable_cer': 0.15,  # Ngưỡng CER tối đa chấp nhận được
        'block_size': 30,            # Kích thước block mặc định
        'min_block_size': 20,        # Kích thước block tối thiểu
        'max_block_size': 120,       # Kích thước block tối đa
        'adaptive_block_size': True, # Tự động điều chỉnh kích thước block
        'correlation_threshold': 0.6, # Ngưỡng tương quan
        'similarity_weights': {      # Trọng số cho các phương pháp so sánh
            'ks_test': 0.15,         # Trọng số cho KS test
            'correlation': 0.6,      # Trọng số cho tương quan
            'cer': 0.25              # Trọng số cho CER
        },
        'max_templates': 300         # Số lượng template tối đa
    }
    
    # Thực hiện nén và tạo biểu đồ
    run_compression(data, compression_config, args.output_dir) 