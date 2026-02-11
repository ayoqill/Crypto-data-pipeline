import os
import json
from minio import Minio
from minio.error import S3Error
from io import BytesIO

def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )

def ensure_bucket(bucket_name: str) -> None:
    client = get_minio_client()
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)

def put_json(bucket_name: str, object_name: str, data: dict | list) -> None:
    client = get_minio_client()
    payload = json.dumps(data).encode("utf-8")
    bio = BytesIO(payload)
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=bio,
        length=len(payload),
        content_type="application/json",
    )
