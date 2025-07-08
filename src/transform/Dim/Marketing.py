import pandas as pd
from src.Process_utm import ColumnStandardizer
standardizer = ColumnStandardizer(
    threshold=80,
    preserve_if_low_similarity=['QuanNV']
)

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/marketing.csv")
df['utm_medium'] = standardizer.transform(df['utm_medium'])
df=df.drop_duplicates()
df = df.rename(columns={'utm_medium': 'Nguon_marketing'})
df.to_csv("~/DWH_Cole_Project/data_result/marketing_transformed.csv",index=False)

