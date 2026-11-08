"""Abort all incomplete multipart uploads in the R2 bucket."""
import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["R2_ENDPOINT"],
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name="auto",
)

bucket = os.environ.get("R2_BUCKET", "payflow-lake")

resp = s3.list_multipart_uploads(Bucket=bucket)
uploads = resp.get("Uploads", [])

if not uploads:
    print("No incomplete multipart uploads found.")
else:
    for u in uploads:
        s3.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])
        print(f"Aborted: {u['Key']}")
    print(f"Done. {len(uploads)} upload(s) aborted.")
