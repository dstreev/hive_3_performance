#!/usr/bin/env sh

search_list=("dynamic\spartitions" \
"^START" \
"^STOP" \
"^Counts" \
"Completed\sexecuting\scommand" \
"Coordinator" \
"Run\sDAG" \
"CREATED_FILES" \
"CPU_MILLISECONDS" \
"^ERROR\s\:\sFAILED")

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
