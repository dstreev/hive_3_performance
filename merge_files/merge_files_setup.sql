DROP DATABASE IF EXISTS ${DB} CASCADE;

CREATE DATABASE IF NOT EXISTS ${DB};

USE ${DB};

DROP TABLE IF EXISTS merge_files_source_stats;

CREATE EXTERNAL TABLE merge_files_source_stats
(
    region      STRING,
    country     STRING,
    report_date TIMESTAMP,
    output      STRING,
    amount      DOUBLE,
    longs       BIGINT,
    types       STRING,
    count       STRING,
    now         DATE,
    seq         STRING,
    field1      STRING,
    field2      STRING,
    field3      STRING,
    field4      BIGINT,
    field5      STRING,
    field6      STRING,
    field7      STRING,
    field8      STRING
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION "/user/dstreev/datasets/merge-files/default";

-- Build stats (for columns) on source data
ANALYZE TABLE merge_files_source_stats COMPUTE STATISTICS FOR COLUMNS;

DROP TABLE IF EXISTS merge_files_source_nostats;

CREATE EXTERNAL TABLE merge_files_source_nostats
(
    region      STRING,
    country     STRING,
    report_date TIMESTAMP,
    output      STRING,
    amount      DOUBLE,
    longs       BIGINT,
    types       STRING,
    count       STRING,
    now         DATE,
    seq         STRING,
    field1      STRING,
    field2      STRING,
    field3      STRING,
    field4      BIGINT,
    field5      STRING,
    field6      STRING,
    field7      STRING,
    field8      STRING
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION "/user/dstreev/datasets/merge-files/default";

DROP TABLE IF EXISTS merge_files_part;

CREATE EXTERNAL TABLE merge_files_part
(
    report_timestamp TIMESTAMP,
    output STRING,
    amount DOUBLE,
    longs  BIGINT,
    types  STRING,
    count  STRING,
    now    DATE,
    seq    STRING,
    field1 STRING,
    field2 STRING,
    field3 STRING,
    field4 BIGINT,
    field5 STRING,
    field6 STRING,
    field7 STRING,
    field8 STRING
)
    PARTITIONED BY (
        region STRING, country STRING, report_date STRING
        )
    STORED AS ORC
    TBLPROPERTIES (
        'external.table.purge' = 'true'
        );