#!/usr/bin/env sh

TARGET_FILE = $HOME/merge_files_test.txt

# Setup
echo "Running merge_files Setup" > $TARGET_FILE
hive --hivevar db=merge_files -f merge_files_setup.sql >> $TARGET_FILE
echo "Setup complete" >> $TARGET_FILE

for i in simple mti; do
    for dist in nodist dist; do
        for stats in nostats stats; do
            for d in false true; do
                if [ $d == "true" ]; then
                    for thres in -1 0 1; do
                        echo "START >> ${i} ${dist} ${stats} ${d} ${thres}" >> "${TARGET_FILE}"
                        hive --hivevar db=merge_files --hivevar dynamic=$d --hivevar dynamic_threshold=$thres \
                            --hivevar gather_stats=false --hivevar version=$stats -i merge_files_init.sql \
                            -f merge_files_$i_$dist.sql >> $TARGET_FILE
                        echo "STOP >> ${i} ${dist} ${stats} ${d} ${thres}" >> "${TARGET_FILE}"
                    done
                else
                    echo "START >> ${i} ${dist} ${stats} ${d}" >> "${TARGET_FILE}"
                    hive --hivevar db=merge_files --hivevar dynamic=$d --hivevar dynamic_threshold=-1 \
                        --hivevar gather_stats=false --hivevar version=$stats -i merge_files_init.sql \
                        -f merge_files_$i_$dist.sql >> $TARGET_FILE
                    echo "STOP >> ${i} ${dist} ${stats} ${d}" >> "${TARGET_FILE}"
                fi
            done
        done
    done
done
