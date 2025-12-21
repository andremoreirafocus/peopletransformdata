graph TD
    A["main()"]
    B["list_objects_in_minio_folder()"]
    C["read_and_flatten_jsons_from_minio()"]
    D["flattened_records_to_parquet_buffer()"]
    E["write_parquet_to_minio()"]
    
    F["read_json_from_minio()"]
    G["flatten_json_string()"]
    H["json_string_to_parquet_minio<br/>(unused)"]
    
    A --> B
    A --> C
    A --> D
    A --> E
    
    C --> F
    C --> G
    
    H --> G
    
    F -.->|Minio Client| I["Minio"]
    B -.->|Minio Client| I
    E -.->|Minio Client| I
    
    D -.->|pandas/pyarrow| J["Parquet Buffer"]
    H -.->|pandas/pyarrow| J
    
    style A fill:#ff6b6b
    style E fill:#4ecdc4
    style H fill:#ffcccc
    style I fill:#95e1d3
    style J fill:#f8b500
