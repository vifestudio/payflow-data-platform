import duckdb
import os
from dotenv import load_dotenv

load_dotenv("C:/Users/vife.boy/Desktop/payflow-data-platform/.env")

endpoint = os.environ["R2_ENDPOINT"].replace("https://", "")
key_id = os.environ["R2_ACCESS_KEY_ID"]
secret = os.environ["R2_SECRET_ACCESS_KEY"]

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{endpoint}';
    SET s3_access_key_id='{key_id}';
    SET s3_secret_access_key='{secret}';
    SET s3_region='us-east-1';
    SET s3_use_ssl=true;
    SET s3_url_style='path';
""")

files = con.execute("SELECT file FROM glob('s3://payflow-lake/bronze/**/*.parquet') LIMIT 30").fetchall()
for f in files:
    print(f[0])
