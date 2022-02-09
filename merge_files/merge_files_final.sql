CREATE DATABASE IF NOT EXISTS ${DB};

USE ${DB};

SHOW TABLES;

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

SHOW TABLES;

set hive.merge.tezfiles;
set hive.optimize.sort.dynamic.partition;
set hive.optimize.sort.dynamic.partition.threshold;
set mapred.reduce.tasks;
set hive.stats.autogather;
set hive.stats.column.autogather;

set hive.optimize.sort.dynamic.partition=false;
set hive.optimize.sort.dynamic.partition.threshold=-1;
-- 0 Does a map only job
set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition.threshold=0;
-- 1 Does a map with reduce phase...  But reduce phase is too low (2 reducers).
set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition.threshold=1;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;

set;

-- EXPLAIN ANALYZE
FROM
    merge_files_source_${version}
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    ;

set;

EXPLAIN -- EXTENDED
FROM
    merge_files_source_${version}
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
DISTRIBUTE BY
    region, country, report_date
    ;

set;

set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition.threshold=0;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;

set hive.optimize.sort.dynamic.partition=false;
set hive.optimize.sort.dynamic.partition.threshold=-1;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;


set hive.optimize.sort.dynamic.partition=false;
set hive.optimize.sort.dynamic.partition.threshold=-1;
-- 0 Does a map only job
set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition.threshold=0;
-- 1 Does a map with reduce phase...  But reduce phase is too low (2 reducers).
set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition.threshold=1;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;

-- For best results and CBO, the 'source' should have column stats.
-- Column stats allow the CBO the better allocate for the skew.
FROM
    merge_files_source_${version}
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    WHERE region='NA'
-- DISTRIBUTE BY
--     region,
--     country,
--     report_date
--     , MOD(hash(field8),10)
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    WHERE region!='NA'
-- DISTRIBUTE BY
--     region,
--     country,
--     report_date
    ;


set hive.optimize.sort.dynamic.partition=false;
set hive.optimize.sort.dynamic.partition.threshold=-1;
-- 0 Does a map only job
set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition.threshold=0;
-- 1 Does a map with reduce phase...  But reduce phase is too low (2 reducers).
set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition.threshold=1;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;

-- For best results and CBO, the 'source' should have column stats.
-- Column stats allow the CBO the better allocate for the skew.
FROM
    merge_files_source_${version}
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    WHERE region='NA'
DISTRIBUTE BY
    region,
    country,
    report_date
    , MOD(hash(field8),10)
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    WHERE region!='NA'
DISTRIBUTE BY
    region,
    country,
    report_date
    ;

WITH BASE AS (
    SELECT
        report_date,
        output,
        amount,
        longs,
        types,
        count,
        now,
        seq,
        field1,
        field2,
        field3,
        field4,
        field5,
        field6,
        field7,
        field8,
        region,
        country,
        to_date(report_date) as report_date1
    FROM
    merge_files_source_1
)
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
        output,
        amount,
        longs,
        types,
        count,
        now,
        seq,
        field1,
        field2,
        field3,
        field4,
        field5,
        field6,
        field7,
        field8,
        region,
        country,
        report_date1
FROM BASE
WHERE region='NA'
DISTRIBUTE BY
    region,
    country,
    report_date1
    , MOD(hash(field8),10)
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    report_date1
FROM BASE
    WHERE region!='NA'
DISTRIBUTE BY
    region,
    country,
    report_date1
    ;

FROM
    merge_files_source
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    WHERE region='NA'
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    WHERE region!='NA'
    ;

FROM
    merge_files_source
INSERT OVERWRITE TABLE
    merge_files_part PARTITION (region, country, report_date)
SELECT
    report_date,
    output,
    amount,
    longs,
    types,
    count,
    now,
    seq,
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    region,
    country,
    to_date(report_date) as report_date
    ;
