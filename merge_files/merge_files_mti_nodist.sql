
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
    to_date(report_date) as report_date1
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
    to_date(report_date) as report_date1
    WHERE region!='NA';
