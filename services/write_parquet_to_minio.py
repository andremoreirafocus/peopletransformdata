from minio import Minio


def write_parquet_to_minio(
    buffer,
    destination_bucket_name,
    destination_object_name,
    minio_endpoint,
    access_key,
    secret_key,
    secure=False,
):
    """
    Writes a Parquet buffer to MinIO at the specified bucket and object name.
    """
    client = Minio(
        minio_endpoint, access_key=access_key, secret_key=secret_key, secure=secure
    )
    if not client.bucket_exists(destination_bucket_name):
        client.make_bucket(destination_bucket_name)
    client.put_object(
        bucket_name=destination_bucket_name,
        object_name=destination_object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )
    print(
        f"Aggregated Parquet file uploaded to MinIO bucket '{destination_bucket_name}' as '{destination_object_name}'"
    )
