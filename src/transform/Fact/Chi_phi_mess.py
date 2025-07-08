import pandas as pd
import os
from src.Get_data_DB import DataTransformer

transformer=DataTransformer()

# Lấy số mess theo thời gian, khoá học trên ME
Query_mess= """ select DATE_ADD(lp.created_at, INTERVAL 7 HOUR) as Thoi_gian,
                       p.code as Ten_khoa_hoc,
                       count(*) as So_mess
                from leads l 
                join leads_products lp on lp.lead_id = l.id 
                join products p on lp.product_id =p.id
                where l.utm_source ='mess' 
                  and lp.created_at >= '2024-01-01'
                group by Thoi_gian, Ten_khoa_hoc
                """
df_count_mess=transformer.fetch_from_mysql(Query_mess)

df_cf=pd.read_csv("~/DWH_Cole_Project/data_tmp/spend_Mkt_PAUSED.csv")

