#!/bin/bash

EXAMPLES_JAR=/usr/hdp/2.4.0.0-169/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-2.7.1.2.4.0.0-169.jar
NUMBER_OF_FILES=$1
FILESIZE=$2
TESTDFSIO_DIR=benchmarks/TestDFSIO

if ( hdfs dfs -test -d $TESTDFSIO_DIR ) ; then
	echo "Removing ${TESTDFSIO_DIR}"
        sudo -u hdfs yarn jar $EXAMPLES_JAR TESTDFSIO -clean 
fi

echo 3 | sudo tee /proc/sys/vm/drop_caches
# Write the data
echo "DFSIO-Write Start: "`date`
sudo -u hdfs yarn jar $EXAMPLES_JAR TestDFSIO -write -nrFiles $NUMBER_OF_FILES -fileSize $FILESIZE
echo "DFSIO-Write End: "`date`


echo 3 | sudo tee /proc/sys/vm/drop_caches
# Read the data
echo "DFSIO-Read Start: "`date`
sudo -u hdfs yarn jar $EXAMPLES_JAR TestDFSIO -read -nrFiles $NUMBER_OF_FILES -fileSize $FILESIZE
echo "DFSIO-Read End: "`date`

