import sqlite3
from pathlib import Path
import pandas as pd


data_path = Path(__file__).parent / "data"
data_path.mkdir(exist_ok=True)

con = sqlite3.connect(data_path / "banshee.db")
cur = con.cursor()

table_list = [
    a[0] for a in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
]
print(table_list)

for table in table_list:
    df = pd.read_sql_query(f"SELECT * FROM {table}", con)
    df.to_parquet(data_path / f"{table}.parquet")

con.close()
