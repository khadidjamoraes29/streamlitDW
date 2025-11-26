from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mysql+pymysql://root:Khadidja@127.0.0.1/filmes_dw")

df = pd.read_sql("SELECT 1 AS teste", engine)
print(df)
