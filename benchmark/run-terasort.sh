#!/bin/bash

EXAMPLES_JAR=/usr/hdp/2.4.0.0-169/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-2.7.1.2.4.0.0-169.jar
# rows are 100 bytes each
NUMBER_OF_ROWS=$1
OUTPUT_DIR=/user/hdfs/terasort_out
INPUT_DIR=/user/hdfs/terasort_in
TERAVALIDATE_OUT=/user/hdfs/teravalidate_out

for DIRECTORY in $OUTPUT_DIR $INPUT_DIR $TERAVALIDATE_OUT; do 
	if ( hdfs dfs -test -d $DIRECTORY ) ; then
    		echo "Removing ${DIRECTORY}"
		sudo -u hdfs hdfs dfs -rm -r -skipTrash $DIRECTORY
	fi
done 

echo 3 | sudo tee /proc/sys/vm/drop_caches
# Generate the data using teragen
echo "Teragen Start: "`date`
sudo -u hdfs yarn jar $EXAMPLES_JAR teragen $NUMBER_OF_ROWS $INPUT_DIR
echo "Teragen End: "`date`

echo 3 | sudo tee /proc/sys/vm/drop_caches
# Sort data using terasort
echo "TeraSort Start: "`date`
sudo -u hdfs yarn jar $EXAMPLES_JAR terasort $INPUT_DIR $OUTPUT_DIR
echo "TeraSort End: "`date`

echo 3 | sudo tee /proc/sys/vm/drop_caches
# Validate data using teravalidate
echo "TeraValidate Start: "`date`
sudo -u hdfs yarn jar $EXAMPLES_JAR teravalidate $OUTPUT_DIR $TERAVALIDATE_OUT
echo "TeraValidate End: "`date`
