import os
import json
import logging
from minio import Minio
from io import BytesIO

logger = logging.getLogger(__name__)

def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )

def ensure_bucket(bucket_name: str) -> None:
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info("Created bucket: %s", bucket_name)
    else:
        logger.info("Bucket already exists: %s", bucket_name)

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

    logger.info("Uploaded object to MinIO: %s/%s", bucket_name, object_name)

def get_json(bucket_name: str, object_name: str):
    client = get_minio_client()
    response = None
    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read().decode("utf-8")
        logger.info("Downloaded object from MinIO: %s/%s", bucket_name, object_name)
        return json.loads(data)
    finally:
        if response is not None:
            response.close()
            response.release_conn()
