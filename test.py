import pandas as pd

df_filter = pd.read_parquet(r'C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS\test\ini_with_filter__BoltUp.parquet', engine='pyarrow')

df_no_filter = pd.read_parquet(r'C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS\test\ini_without_filter__BoltUp.parquet', engine='pyarrow')

df = df_filter.compare(df_no_filter)
print(df)