#!/usr/bin/env sh

search_list=("dynamic\spartitions" \
"Executing\son\sYARN\scluster\swith\sApp\sid" \
"^START" \
"^STOP" \
"^Counts" \
"[0..9]\srows\saffected" \
"Completed\sexecuting\scommand" \
"Compile\sQuery" \
"Prepare\sPlan" \
"Submit\sPlan" \
"Start\sDAG" \
"Coordinator" \
"Run\sDAG" \
"CREATED_DYNAMIC_PARTITIONS" \
"CREATED_FILES" \
"CPU_MILLISECONDS" \
"SPILLED_RECORDS" \
"TOTAL_LAUNCHED_TASKS" \
"GC_TIME_MILLIS")

FULL_SEARCH=
for i in "${search_list[@]}"; do
  if [ "${FULL_SEARCH}x" == "x" ]; then
    FULL_SEARCH="${i}"
  else
    FULL_SEARCH="${FULL_SEARCH}\|${i}"
  fi
done

echo "${FULL_SEARCH} on $1"

grep "${FULL_SEARCH}" $1
# grep 'dynamic\spartitions\|Executing\son\sYARN\scluster\swith\sApp\sid\|^START\|^STOP\|^Counts\|[0..9]\srows\saffected\|Completed\sexecuting\scommand\|Compile\sQuery\|Prepare\sPlan\|Submit\sPlan\|Start\sDAG\|Coordinator\|Run\sDAG\|CREATED_DYNAMIC_PARTITIONS\|CREATED_FILES\|CPU_MILLISECONDS\|SPILLED_RECORDS\|TOTAL_LAUNCHED_TASKS\|GC_TIME_MILLIS' $1