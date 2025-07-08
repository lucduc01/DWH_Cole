from Connect_DB import mysql_conn, mysql_cursor, sql_server_conn, sql_server_cursor

# SQL commands to create fact tables
create_Chien_dich_Meta = """
        CREATE TABLE Chien_dich_Meta (
            STT bigINT,
            Account VARCHAR(10),
            Chien_dich NVARCHAR(200),
            Ngay_bat_dau DATE,
            Trang_thai VARCHAR(10) CHECK (Trang_thai IN ('PAUSED', 'ACTIVE')),
            PRIMARY KEY (STT, Account)  -- Cặp khóa chính
        );
"""

# Execute SQL commands on SQL Server
sql_server_cursor.execute(create_Chien_dich_Meta)


# Commit the transaction
sql_server_conn.commit()

print("Raw tables created successfully on SQL Server.")