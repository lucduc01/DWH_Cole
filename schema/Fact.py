from Connect_DB import mysql_conn, mysql_cursor, sql_server_conn, sql_server_cursor

# SQL commands to create fact tables
create_fact_ke_hoach_toa = """
CREATE TABLE Fact_Ke_hoach_TOA (
    Id int PRIMARY KEY,
    Doanh_thu decimal(18,0),
    Chi_phi_Quang_cao decimal(18,0),
    L1 int,
    L1_L1C int,
    L2 int,
    L8 int,
    Ma_ngay_bat_dau date,
    Ma_ngay_ket_thuc date,
    Ma_lop_hoc int,
    FOREIGN KEY (Ma_ngay_bat_dau) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_ngay_ket_thuc) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_lop_hoc) REFERENCES Dim_Lop_hoc(Ma_lop_hoc)
);
"""

create_fact_ke_hoach_sale_tot = """
CREATE TABLE Fact_Ke_hoach_Sale_TOT (
    Id int PRIMARY KEY,
    Doanh_thu decimal(18,0),
    L1 int,
    L1_L1C int,
    L2 int,
    L8 int,
    Ma_ngay_bat_dau date,
    Ma_ngay_ket_thuc date,
    Ma_khoa_hoc int,
    Ma_nhan_vien int,
    FOREIGN KEY (Ma_ngay_bat_dau) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_ngay_ket_thuc) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc),
    FOREIGN KEY (Ma_nhan_vien) REFERENCES Dim_Nhan_vien(Ma_nhan_vien)
);
"""

create_fact_ke_hoach_marketing_tot = """
CREATE TABLE Fact_Ke_hoach_Marketing_TOT (
    Id int PRIMARY KEY,
    Doanh_thu decimal(18,0),
    Chi_phi_Quang_cao decimal(18,0),
    L1 int,
    L2 int,
    L8 int,
    Ma_ngay_bat_dau date,
    Ma_ngay_ket_thuc date,
    Ma_khoa_hoc int,
    Ma_nhan_vien int,
    FOREIGN KEY (Ma_ngay_bat_dau) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_ngay_ket_thuc) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc),
    FOREIGN KEY (Ma_nhan_vien) REFERENCES Dim_Nhan_vien(Ma_nhan_vien)
);
"""

create_fact_doanh_thu_sodh_tot = """
CREATE TABLE Fact_Doanh_thu_SoDH_TOT (
    Id int PRIMARY KEY,
    Doanh_thu bigint,
    So_don_hang int,
    Ngay_chuyen_tien date,
    Ma_khoa_hoc int,
    Ma_sale int,
    Ma_Marketing nvarchar(20),
    Ma_kenh int,
    FOREIGN KEY (Ngay_chuyen_tien) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc),
    FOREIGN KEY (Ma_sale) REFERENCES Dim_Nhan_vien(Ma_nhan_vien),
    FOREIGN KEY (Ma_kenh) REFERENCES Dim_Kenh(Ma_kenh)
);
"""

create_fact_doanh_so_1a_tot = """
CREATE TABLE Fact_Doanh_so_1A_TOT (
    ID int PRIMARY KEY,
    Doanh_so bigint,
    Ngay_chuyen_tien date,
    Ma_khoa_hoc int,
    Ma_sale int,
    Ma_Marketing nvarchar(20),
    Ma_kenh int,
    FOREIGN KEY (Ngay_chuyen_tien) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc),
    FOREIGN KEY (Ma_sale) REFERENCES Dim_Nhan_vien(Ma_nhan_vien),
    FOREIGN KEY (Ma_kenh) REFERENCES Dim_Kenh(Ma_kenh)
);
"""

create_fact_cf_marketing_tot = """
CREATE TABLE Fact_CF_Marketing_TOT (
    ID int PRIMARY KEY,
    CF_Marketing bigint,
    Ngay_bat_dau date,
    Ma_khoa_hoc int,
    Ma_utm_kenh varchar(10),
    FOREIGN KEY (Ngay_bat_dau) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
    FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc),
    FOREIGN KEY (Ma_utm_kenh) REFERENCES Dim_Utm_kenh(Ma_utm_kenh)
);
"""

# Execute SQL commands on SQL Server
sql_server_cursor.execute(create_fact_ke_hoach_toa)
sql_server_cursor.execute(create_fact_ke_hoach_sale_tot)
sql_server_cursor.execute(create_fact_ke_hoach_marketing_tot)
sql_server_cursor.execute(create_fact_doanh_thu_sodh_tot)
sql_server_cursor.execute(create_fact_doanh_so_1a_tot)
sql_server_cursor.execute(create_fact_cf_marketing_tot)

# Commit the transaction
sql_server_conn.commit()

print("Fact tables created successfully on SQL Server.")