1. Kết nối đến database: Người dùng nhập "Old System ID", chương trình sẽ tìm tất cả các bảng có dữ liệu với System ID này (danh sách bảng lấy từ file table_logic.txt).
2. Hiển thị danh sách bảng: Các bảng có dữ liệu sẽ được liệt kê trên giao diện.
3. Nhập các thông tin khác: Người dùng nhập "New System ID", "Current Max File ID" (nếu cần), và thư mục lưu file.
Xuất dữ liệu:
4. Export Data multi files: Tạo một file .sql cho mỗi bảng, chứa các lệnh INSERT/UPDATE/DELETE để chuyển dữ liệu từ Old System ID sang New System ID.
5. Export Data single file: Tạo một file all_tables.sql chứa toàn bộ lệnh cho tất cả các bảng.
6. Nếu có bảng đặc biệt (T_FILE_LINK_KIHON_PJ_GAMEN), chương trình sẽ xử lý thêm các file liên quan (T_FILE_DATA, S_NUMBER_FILE).
7. Lưu file: Các file .sql sẽ được lưu vào thư mục do người dùng chỉ định.
