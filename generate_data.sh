#!/bin/bash
cur_date=$(date +"%s") # get current datetime
rows=10000
groups=2

echo "Start generation..."

# generate csv file with structure [id, timestamp, value]
i=1
while [ "$i" -le "$rows" ]
do
  tmp_date=$(((cur_date + i * 10) * 1000)) # increment time by 10 seconds each iteration

  id=1
  while [ "$id" -le "$groups" ] # generate metric for each group
  do
    value=$((RANDOM % 10 * 10)) # generate random value
    echo "$id, $tmp_date, $value" >> log.csv
    id=$((id + 1))
  done

  i=$((i + 1))
done

echo "Finish generation"
