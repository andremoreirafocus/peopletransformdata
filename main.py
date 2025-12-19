import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from io import BytesIO
import json
import time


def list_objects_in_minio_folder(
    bucket_name,
    prefix,
    minio_endpoint,
    access_key,
    secret_key,
    secure=True,
):
    """
    Lists files in a MinIO folder (prefix).
    :param bucket_name: MinIO bucket name
    :param prefix: Folder path (prefix) in the bucket
    :param minio_endpoint: MinIO server endpoint
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    :return: List of file object names
    """
    try:
        client = Minio(
            minio_endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except Exception as e:
        print(f"Error listing files in MinIO folder: {e}")
        return []


def flatten_json_string(json_str, sep="_"):
    """
    Flattens a nested JSON string to a one-level dictionary.
    :param json_str: JSON string (object)
    :param sep: Separator for nested keys (default: '.')
    :return: Flattened dictionary
    """

    def _flatten(obj, parent_key="", sep=sep):
        items = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                items.update(_flatten(v, new_key, sep=sep))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                items.update(_flatten(v, new_key, sep=sep))
        else:
            items[parent_key] = obj
        return items

    try:
        data = json.loads(json_str)
        return _flatten(data)
    except Exception as e:
        print(f"Error flattening JSON string: {e}")
        return None


def read_json_from_minio(
    bucket_name,
    object_name,
    minio_endpoint,
    access_key,
    secret_key,
    secure=True,
):
    """
    Reads a JSON file from MinIO and returns its contents as a string.
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :param minio_endpoint: MinIO server endpoint
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    :return: JSON file contents as a string
    """
    try:
        client = Minio(
            minio_endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )
        response = client.get_object(bucket_name, object_name)
        json_str = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return json_str
    except Exception as e:
        print(f"Error reading JSON from MinIO: {e}")
        return None


def json_string_to_parquet_minio(
    json_str,
    bucket_name,
    object_name,
    minio_endpoint,
    access_key,
    secret_key,
    secure=True,
):
    """
    Converts a JSON-formatted string to Parquet and uploads it to MinIO.
    :param json_str: JSON string (representing an object or array)
    :param source_bucket_name: MinIO source bucket name (not used for writing)
    :param destination_bucket_name: MinIO destination bucket name (Parquet will be written here)
    :param object_name: Object name for the Parquet file in MinIO
    :param minio_endpoint: MinIO server endpoint
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    """
    try:
        data = json.loads(json_str)
        if isinstance(data, dict):
            data = [data["results"][0]]
        # Flatten each record
        flattened_data = []
        for record in data:
            # Convert each record to a JSON string, then flatten
            flat = flatten_json_string(json.dumps(record))
            if flat is not None:
                flattened_data.append(flat)
        df = pd.DataFrame(flattened_data)
        table = pa.Table.from_pandas(df)
        out_buffer = BytesIO()
        pq.write_table(table, out_buffer)
        out_buffer.seek(0)
        client = Minio(
            minio_endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=out_buffer,
            length=out_buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        print(
            f"Parquet file uploaded to MinIO bucket '{bucket_name}' as '{object_name}'"
        )
    except Exception as e:
        print(f"Error converting JSON to Parquet and uploading to MinIO: {e}")


def main():
    minio_endpoint = "localhost:9000"
    access_key = "datalake"
    secret_key = "datalake"
    source_bucket_name = "raw"
    destination_bucket_name = "processed"
    year = 2025
    month = 12
    day = 19
    hour = 14
    start_time = time.time()
    objects_to_be_transformed = list_objects_in_minio_folder(
        bucket_name=source_bucket_name,
        prefix=f"year={year}/month={month}/day={day}/hour={hour}/",
        minio_endpoint=minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    print(f"Files to be transformed: {objects_to_be_transformed}")
    for object_name in objects_to_be_transformed:
        source_object_name = object_name

        print(f"Reading JSON from MinIO: {source_object_name}")
        json_str = read_json_from_minio(
            bucket_name=source_bucket_name,
            object_name=source_object_name,
            minio_endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )
        # destination_filename = "person_0329.parquet"
        destination_object_name = f"year={year}/month={month}/day={day}/hour={hour}/{source_object_name.split('/')[-1].replace('.json', '.parquet')}"
        print(
            f"Converting {source_object_name} to Parquet and uploading to MinIO as: {destination_object_name}"
        )
        json_string_to_parquet_minio(
            json_str=json_str,
            bucket_name=destination_bucket_name,
            object_name=destination_object_name,
            minio_endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )
        elapsed = time.time() - start_time
        print(f"Elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
