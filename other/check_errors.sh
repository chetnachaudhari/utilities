ARGFILE=$1
MAILTO='chetnachaudhari@gmail.com'
SUBJECT="Job Failures Report for past 1 hour on $HOSTNAME"
MAILX='mailx'
FAILURES=0
BODY=""
COUNT=0

while read line; do
    SNAME=$(echo $line|cut -d',' -f1)
    FNAME=$(echo $line|cut -d',' -f2)
    for entry in $(find $FNAME -type f -mmin -60)
    do
        COUNT=$(grep -c -i "^error\|exception" $entry)
        echo "$entry $COUNT"
        if [ $COUNT -gt 0 ]
        then
                BODY="$BODY"$'\n'"There were $COUNT error or exceptions while running \"$SNAME\" jobs in file \"$entry\""
                FAILURES=$(( $FAILURES+$COUNT ))
        fi
    done

done < $ARGFILE

if [ $FAILURES -gt 0 ]
then
        echo "$BODY" | $MAILX -s "$SUBJECT" "$MAILTO"
fi
