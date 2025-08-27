import configparser
import pandas as pd
import boto3
from io import StringIO, BytesIO
import logging

import constants

logger = logging.getLogger(__name__)
# Workaround https://github.com/boto/boto3/issues/4400#issuecomment-2600742103
# boto3.set_stream_logger('')
import os

os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"


def get_client():
    # Read AWS credentials from ~/.aws/credentials
    config = configparser.ConfigParser()
    config.read("/home/mapr/.aws/credentials")

    return boto3.client(
        "s3",
        aws_access_key_id=config["default"]["aws_access_key_id"],
        aws_secret_access_key=config["default"]["aws_secret_access_key"],
        endpoint_url=f"https://{constants.CLUSTER_NAME}:9000",
        use_ssl=True,
        # verify=False
        verify="/root/.mc/certs/CAs/chain-ca.pem",
    )


def put(df: pd.DataFrame, bucket_name: str, file_key: str, content_type: str):
    client = get_client()

    # Create bucket if missing
    if not bucket_name in list_buckets():
        try:
            client.create_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.error(e)
            raise e

    # Convert DataFrame to the appropriate format based on content type
    if content_type == "text/csv":
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        body = csv_buffer.getvalue()

    elif content_type == "application/json":
        json_buffer = StringIO()
        df.to_json(json_buffer, orient="records", lines=True)
        body = json_buffer.getvalue()

    elif content_type == "application/octet-stream":  # Parquet format
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        body = parquet_buffer.getvalue()

    else:
        raise ValueError(f"Unsupported content type: {content_type}")

    try:
        logger.info("Uploading file with key: %s", file_key)
        # Write the file to S3
        client.put_object(
            Bucket=bucket_name, Key=file_key, Body=body, ContentType=content_type
        )
        logger.info("Successfully uploaded %s to bucket %s.", file_key, bucket_name)
        return True
    except Exception as e:
        logger.error(e)
        raise e


def list_buckets():
    try:
        client = get_client()
        buckets = client.list_buckets()
        logger.debug(buckets)
        return [b["Name"] for b in buckets["Buckets"]]

    except Exception as e:
        logger.error(e)
        raise e


def list_bucket(bucket: str):
    try:
        client = get_client()
        objects = client.list_objects(Bucket=bucket)
        logger.debug(objects)
        if "Contents" in objects:
            return [
                {
                    "Object Key": o["Key"],
                    "Size": o["Size"],
                    "Modified": o["LastModified"],
                }
                for o in objects["Contents"]
            ]
        else:
            return []

    except Exception as e:
        logger.error(e)
        raise e


def summarize_s3_folder(bucket, prefix):
    try:
        if not prefix.endswith("/"):
            prefix += "/"
        client = get_client()
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        total_size = 0
        object_count = 0

        for page in pages:
            for obj in page.get("Contents", []):
                total_size += obj["Size"]
                object_count += 1
                logger.debug(obj)

        return {
            "📁 Folder": f"s3://{bucket}/{prefix}",
            "📦 Objects": str(object_count),
            "🧮 Total size": f"{total_size / (1024**2):.2f} MB",
        }

    except Exception as error:
        logger.error(error)
