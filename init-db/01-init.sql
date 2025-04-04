-- Tạo user cho AI Developer
CREATE USER ai_user WITH PASSWORD '1234';

-- Cấp quyền kết nối đến database
GRANT CONNECT ON DATABASE iot_db TO ai_user;

-- Cấp quyền sử dụng schema public
GRANT USAGE ON SCHEMA public TO ai_user;

-- Cấp quyền SELECT cho tất cả các bảng hiện tại
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ai_user;

-- Cấp quyền SELECT cho các bảng sẽ được tạo trong tương lai
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ai_user; 