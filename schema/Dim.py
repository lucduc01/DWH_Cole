from Connect_DB import mysql_conn, mysql_cursor, sql_server_conn, sql_server_cursor

# SQL commands to create tables
create_dim_thoi_gian = """
CREATE TABLE Dim_Thoi_gian (
    Ma_ngay_du date PRIMARY KEY,
    Ngay int NULL,
    Tuan int NULL,
    Thang int NULL,
    Quy int NULL,
    Nam int NULL
);
"""

create_dim_phan_loai = """
CREATE TABLE Dim_Phan_loai (
    Ma_phan_loai nvarchar(20) PRIMARY KEY
);
"""

create_dim_khoa_hoc = """
CREATE TABLE Dim_Khoa_hoc (
    Ma_khoa_hoc int PRIMARY KEY,
    Ten_khoa_hoc nvarchar(50),
    Ma_phan_loai nvarchar(20),
    Trang_thai int,
    FOREIGN KEY (Ma_phan_loai) REFERENCES Dim_Phan_loai(Ma_phan_loai)
);
"""

create_dim_lop_hoc = """
CREATE TABLE Dim_Lop_hoc (
    Ma_lop_hoc int PRIMARY KEY,
    Ten_lop_hoc nvarchar(50),
    Ngay_khai_giang date,
    Ngay_ket_thuc date,
    So_buoi_tuyen_sinh int,
    So_buoi_hoc int,
    Trang_thai nvarchar(20),
    Ma_khoa_hoc int NULL,
    FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc)
);
"""

create_dim_saler = """
CREATE TABLE Dim_Saler (
    Ma_nhan_vien int PRIMARY KEY,
    Ten_nhan_vien nvarchar(50),
    Ten_he_thong nvarchar(50),
    Trang_thai int
);
"""

create_dim_kenh = """
CREATE TABLE Dim_Kenh (
    Ma_kenh int NOT NULL,
    Ten_kenh varchar(50)
);
"""

create_dim_utm_kenh = """
CREATE TABLE Dim_Utm_kenh (
    Ma_utm_kenh varchar(10) PRIMARY KEY
);
"""

create_dim_nguon_marketing = """
CREATE TABLE Dim_Marketing(
    Nguon_marketing nvarchar(50) PRIMARY KEY
);
"""

# Execute SQL commands on SQL Server
sql_server_cursor.execute(create_dim_thoi_gian)
sql_server_cursor.execute(create_dim_phan_loai)
sql_server_cursor.execute(create_dim_khoa_hoc)
sql_server_cursor.execute(create_dim_lop_hoc)
sql_server_cursor.execute(create_dim_saler)
sql_server_cursor.execute(create_dim_kenh)
sql_server_cursor.execute(create_dim_utm_kenh)
sql_server_cursor.execute(create_dim_nguon_marketing)

# Commit the transaction
sql_server_conn.commit()

print("Tables created successfully on SQL Server.")