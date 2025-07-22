create table md_ob_extract_config
(
process_name varchar(100),
table_name varchar(100),
extract_location varchar(300),
file_prefix varchar(50),
extract_type varchar(20),
extract_sql varchar,
is_active varchar(1),
dt_created timestamp,
dt_modified timestamp
);
