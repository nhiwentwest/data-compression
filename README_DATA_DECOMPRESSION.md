# Nén và giải nén

## Trường hợp không cần giải nén mà muốn sử dụng gốc 

Vui lòng tham khảo bảng: original_samples

## Tổng quan nguồn và đầu ra dữ liệu

**compressed_data**: Dùng để lưu dữ liệu sau khi nén, chỉ tập trung vào các mẫu để so sánh
**<name_device>.json**: đầu ra của dữ liệu sau khi đã được giải nén

## Trước khi sử dụng nén và giải nén

Đảm bảo bảng original_samples có dữ liệu. 

Dùng lệnh tạo giả lập nếu cần: 


```bash
python templates/gentwo.py --device-id <name_device> --num-days <number>
```

Dữ liệu sẽ được lưu vào bảng original_samples.

## Sử dụng

### Sử dụng cơ bản cho nén

Muốn nén dữ liệu và lưu vào trong bảng compressed_data_optimized:

```bash
python compress.py --device-id <name_device>
```

Muốn nén dữ liệu và tạo biểu đồ so sánh: 

```bash
python compress.py --device-id <name_device> --visualize
```


### Sử dụng cơ bản cho giải nén

```bash
python decompress.py --device-id <name_device>
```

Lệnh này sẽ giải nén tất cả dữ liệu của thiết bị đó nằm trong bảng compressed_data_optimized và lưu kết quả vào file `<name_device>.json`.

### Tham số

- `--h`: help


## Lưu ý

- Quá trình giải nén hoạt động tốt nhất khi bảng `original_samples` có đầy đủ dữ liệu.
