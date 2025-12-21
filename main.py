import time
from services.write_parquet_to_minio import write_parquet_to_minio
from services.list_objects_in_minio_folder import list_objects_in_minio_folder
from services.read_and_flatten_jsons_from_minio import read_and_flatten_jsons_from_minio
from services.get_parquet_buffer_from_flattened_records import (
    get_parquet_buffer_from_flattened_records,
)


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
    prefix = f"year={year}/month={month}/day={day}/hour={hour}/"
    objects_to_be_transformed = list_objects_in_minio_folder(
        bucket_name=source_bucket_name,
        prefix=prefix,
        minio_endpoint=minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    print(f"Files to be transformed: {objects_to_be_transformed}")

    # Aggregate all records from all JSON files
    all_flattened_records = read_and_flatten_jsons_from_minio(
        object_names=objects_to_be_transformed,
        source_bucket_name=source_bucket_name,
        minio_endpoint=minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    if all_flattened_records:
        out_buffer = get_parquet_buffer_from_flattened_records(all_flattened_records)
        destination_object_name = (
            f"{prefix}consolidated-{year}{month}{day}{hour}.parquet"
        )
        write_parquet_to_minio(
            buffer=out_buffer,
            destination_bucket_name=destination_bucket_name,
            destination_object_name=destination_object_name,
            minio_endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )
    else:
        print("No records found to write to Parquet.")
    elapsed = time.time() - start_time
    print(f"Elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
