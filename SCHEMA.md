# Comandos no Hive para criação da tabela externa

CREATE EXTERNAL TABLE default.people (
    gender string, 
    name_title string,
    name_first string,
    name_last string,
    location_street_number bigint,
    location_street_name string,
    location_city string,
    location_state string,
    location_country string,
    location_coordinates_latitude string,
    location_coordinates_longitude string,
    location_timezone_offset string,
    location_timezone_description string,
    email string,
    login_uuid string,
    login_username string,
    login_password string,
    login_salt string,
    login_md5 string,
    login_sha1 string,
    login_sha256 string,
    dob_date string,
    dob_age bigint,
    registered_date string,
    registered_age bigint,
    phone string,
    cell string,
    id_name string,
    id_value string,
    nat string
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

MSCK REPAIR TABLE default.people;

## Queries no Hive
select * from default.people 
	where nat = 'US' and year='2025';

## Queries no Presto
select * from hive.default.people 
	where nat = 'US' and year='2025';

