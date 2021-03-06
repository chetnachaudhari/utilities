#!/bin/bash
BODY=""
COUNT=0
if [ "$#" -lt 1 ]; then

  echo "Usage: $0  <max_life_in_mins>"

  exit 1

fi

yarn application -list 2>/dev/null | grep "RUNNING" | awk '{print $1}' | grep -v "Total" > job_list.txt

for jobId in `cat job_list.txt`

do

finish_time=`yarn application -status $jobId 2>/dev/null | grep "Finish-Time" | awk '{print $NF}'`

if [ $finish_time -ne 0 ]; then

  echo "App $jobId is not running"

  exit 1

fi

time_diff=`date +%s`-`yarn application -status $jobId 2>/dev/null | grep "Start-Time" | awk '{print $NF}' | sed 's!$!/1000!'`

time_diff_in_mins=`echo "("$time_diff")/60" | bc`

echo "App $jobId is running for $time_diff_in_mins min(s)"

if [ $time_diff_in_mins -gt $1 ]; then

  # echo "Killing app $jobId"

  # yarn application -kill $jobId
  COUNT=$(( $COUNT+1 ))
  BODY="$BODY"$'\n'"Application app $jobId is running for long time"

fi

done

if [ $COUNT -gt 0 ]
then
    echo "$BODY" | mailx -s "Long Running Applications" "chetnachaudhari@gmail.com"
fi
