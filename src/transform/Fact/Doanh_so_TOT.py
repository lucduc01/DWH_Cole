import pandas as pd
from src.Process_utm import ColumnStandardizer

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/Doanh_so_TOT.csv")
df_saler=pd.read_csv("~/DWH_Cole_Project/data_result/saler_transformed.csv")

#Chuẩn hoá dữ liệu
def process_grouped_sales(df: pd.DataFrame) -> pd.DataFrame:

    df=df.fillna(0)
    df['Ngay_chuyen_tien'] = pd.to_datetime(df['Ngay_chuyen_tien'])
    # 1. Nhóm theo 'Ma_khoa_hoc', 'o_customer_id' → lấy bản ghi có Ngay_chuyen_tien nhỏ nhất
    idx = df.groupby(['Ma_khoa_hoc', 'o_customer_id'])['Ngay_chuyen_tien'].idxmin()
    df_grouped = df.loc[idx].reset_index(drop=True)

    # 2. DataFrame 1A: Bỏ 3 cột
    df_1A = df_grouped.drop(columns=[ 'o_customer_id', 'current_sale_order_level_id'])

    # 3. DataFrame 2A: lọc current_sale_order_level_id >= 19 rồi bỏ 3 cột
    df_2A = df_grouped[df_grouped['current_sale_order_level_id'] >= 19]
    df_2A = df_2A.drop(columns=[ 'o_customer_id', 'current_sale_order_level_id'])

    # 4. Gộp theo các cột còn lại (trừ Doanh_so)
    group_cols = [col for col in df_1A.columns if col != 'Doanh_so']
    df_1A_sum = df_1A.groupby(group_cols, dropna=False)['Doanh_so'].sum().reset_index()
    df_2A_sum = df_2A.groupby(group_cols, dropna=False)['Doanh_so'].sum().reset_index()

    # 5. Đổi tên cột Doanh_so
    df_1A_sum = df_1A_sum.rename(columns={'Doanh_so': 'Doanh_so_1A'})
    df_2A_sum = df_2A_sum.rename(columns={'Doanh_so': 'Doanh_so_2A'})

    # 6. Outer join theo các trường còn lại (trừ Doanh_so)
    df_final = pd.merge(df_1A_sum, df_2A_sum, on=group_cols, how='outer')

    # 7. Điền 0 nếu NaN
    df_final['Doanh_so_1A'] = df_final['Doanh_so_1A'].fillna(0)
    df_final['Doanh_so_2A'] = df_final['Doanh_so_2A'].fillna(0)
    
    return df_final

standardizer = ColumnStandardizer(
    threshold=75,
    preserve_if_low_similarity=['QuanNV']
)
df['Ma_marketer'] = standardizer.transform(df['Ma_marketer'])
df_result = process_grouped_sales(df)

df_result.to_csv("~/DWH_Cole_Project/data_result/Doanh_so_TOT_transformed.csv", index=False)