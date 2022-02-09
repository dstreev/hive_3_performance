USE ${db};

set hive.optimize.sort.dynamic.partition=${dynamic};
set hive.optimize.sort.dynamic.partition.threshold=${dynamic_threshold};

set hive.merge.tezfiles=false;
set mapred.reduce.tasks=-1;
set hive.stats.autogather=${gather_stats};
set hive.stats.column.autogather=${gather_stats};
