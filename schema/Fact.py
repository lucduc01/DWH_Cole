from Connect_DB import mysql_conn, mysql_cursor, sql_server_conn, sql_server_cursor

# SQL commands to create fact tables
create_fact_doanh_thu_sodh_tot = """
      create table Fact_Doanh_thu_TOT (
        Id int primary key,
        Doanh_thu bigint,
        So_don_hang int,
        Ngay_chuyen_tien date, 
        Ma_khoa_hoc int, 
        Ma_saler int, 
        Ma_marketer nvarchar(30), 
        Ma_kenh int,
        FOREIGN KEY (Ngay_chuyen_tien) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
        FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc),
        FOREIGN KEY (Ma_saler) REFERENCES Dim_Saler(Ma_nhan_vien),
        FOREIGN KEY (Ma_marketer) REFERENCES Dim_Marketing(Nguon_marketing),
        FOREIGN KEY (Ma_kenh) REFERENCES Dim_Kenh(Ma_kenh)
        )
);
"""

create_fact_doanh_so_tot = """
        create table Fact_Doanh_so_TOT (
        Id int primary key,
        Doanh_so_1A bigint,
        Doanh_so_2A bigint,
        Ngay_chuyen_tien date, 
        Ma_khoa_hoc int, 
        Ma_saler int, 
        Ma_marketer nvarchar(30), 
        Ma_kenh int,
        FOREIGN KEY (Ngay_chuyen_tien) REFERENCES Dim_Thoi_gian(Ma_ngay_du),
        FOREIGN KEY (Ma_khoa_hoc) REFERENCES Dim_Khoa_hoc(Ma_khoa_hoc),
        FOREIGN KEY (Ma_saler) REFERENCES Dim_Saler(Ma_nhan_vien),
        FOREIGN KEY (Ma_marketer) REFERENCES Dim_Marketing(Nguon_marketing),
        FOREIGN KEY (Ma_kenh) REFERENCES Dim_Kenh(Ma_kenh)
        )
        
"""

# Execute SQL commands on SQL Server
sql_server_cursor.execute(create_fact_doanh_thu_sodh_tot)
sql_server_cursor.execute(create_fact_doanh_so_tot)

# Commit the transaction
sql_server_conn.commit()

print("Fact tables created successfully on SQL Server.")