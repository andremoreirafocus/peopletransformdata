import time
from services.write_processed_to_minio import write_processed_to_minio
from services.list_objects_in_minio_folder import list_objects_in_minio_folder
from services.get_raw_files_from_minio import get_raw_files_from_minio
from services.get_parquet_buffer_from_flattened_records import (
    get_parquet_buffer_from_flattened_records,
)


def main():
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
    )
    print(f"Files to be transformed: {objects_to_be_transformed}")

    # Aggregate all records from all JSON files
    all_flattened_records = get_raw_files_from_minio(
        object_names=objects_to_be_transformed,
        source_bucket_name=source_bucket_name,
    )

    if all_flattened_records:
        out_buffer = get_parquet_buffer_from_flattened_records(all_flattened_records)
        destination_object_name = (
            f"{prefix}consolidated-{year}{month}{day}{hour}.parquet"
        )
        write_processed_to_minio(
            buffer=out_buffer,
            destination_bucket_name=destination_bucket_name,
            destination_object_name=destination_object_name,
        )
    else:
        print("No records found to write to the destination bucket.")
    elapsed = time.time() - start_time
    print(f"Elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
