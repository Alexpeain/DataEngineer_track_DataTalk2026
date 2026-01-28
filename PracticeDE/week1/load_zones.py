import pandas as pd
from sqlalchemy import create_engine

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

df = pd.read_csv("taxi_zone_lookup.csv")  # or pd.read_csv(URL)

engine = create_engine("postgresql://postgres:root@localhost:5433/ny_taxi")

df.to_sql(
    name="zones",
    con=engine,
    if_exists="replace",
    index=False,
)
print("Zones data has been loaded into the 'zones' table.")
