#!/usr/bin/env sh

TARGET_FILE=$HOME/merge_files_`date +%y%m%d_%H%M`.txt
echo "Check output file: ${TARGET_FILE}"

# Setup
echo "Running merge_files Setup" > $TARGET_FILE
hive --hivevar db=merge_files -f merge_files_setup.sql >> $TARGET_FILE 2>&1
echo "Setup complete" >> $TARGET_FILE

for i in simple mti; do
    for dist in nodist dist; do
        for stats in nostats stats; do
            for d in false true; do
                if [ $d == "true" ]; then
                    for thres in -1 0 1; do
                        RUN_FILE="merge_files_${i}_${dist}.sql"
                        echo "START >> ${i} ${dist} ${stats} ${d} ${thres}: Run File: ${RUN_FILE}" >> "${TARGET_FILE}"
                        hive --hivevar db=merge_files --hivevar dynamic=$d --hivevar dynamic_threshold=$thres --hivevar gather_stats=false --hivevar version=$stats -i merge_files_init.sql -f $RUN_FILE >> $TARGET_FILE 2>&1
                        echo "Counts: "`hdfs dfs -count -h /warehouse/tablespace/external/hive/merge_files.db/merge_files_part` >> $TARGET_FILE
                        echo "STOP >> ${i} ${dist} ${stats} ${d} ${thres}" >> "${TARGET_FILE}"
                    done
                else
                    RUN_FILE="merge_files_${i}_${dist}.sql"
                    echo "START >> ${i} ${dist} ${stats} ${d}: Run File: ${RUN_FILE}" >> "${TARGET_FILE}"
                    hive --hivevar db=merge_files --hivevar dynamic=$d --hivevar dynamic_threshold=-1 --hivevar gather_stats=false --hivevar version=$stats -i merge_files_init.sql -f $RUN_FILE >> $TARGET_FILE 2>&1
                    echo "Counts: "`hdfs dfs -count -h /warehouse/tablespace/external/hive/merge_files.db/merge_files_part` >> $TARGET_FILE
                    echo "STOP >> ${i} ${dist} ${stats} ${d}" >> "${TARGET_FILE}"
                fi
            done
        done
    done
done
