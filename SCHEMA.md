CREATE EXTERNAL TABLE default.people (
    gender varchar(10), 
    name_title varchar(100),
    name_first varchar(100),
    name_last varchar(100),
    location_street_number varchar(100),
    location_street_name varchar(100),
    location_city varchar(100),
    location_state varchar(100),
    location_country varchar(100),
    location_coordinates_latitude decimal,
    location_coordinates_longitude decimal,
    location_timezone_offset varchar(10),
    location_timezone_description varchar(100),
    email varchar(100),
    login_uuid varchar(100),
    login_username varchar(100),
    login_password varchar(100),
    login_salt varchar(100),
    login_md5 varchar(100),
    login_sha1 varchar(100),
    login_sha256 varchar(100),
    dob_date varchar(100),
    dob_age int,
    registered_date varchar(100),
    registered_age int,
    phone varchar(100),
    cell varchar(100),
    id_name varchar(100),
    id_value varchar(100),
    nat varchar(100)
)
PARTITIONED BY (
    year string, 
    month string, 
    day string, 
    hour string
)
STORED AS PARQUET
LOCATION 's3a://processed/people'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
