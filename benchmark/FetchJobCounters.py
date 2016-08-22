#!/usr/bin/python

import sys
import json
import urllib2
import time

def getHistoryUrl(clusterName):
    return "http://fdp-dev-compute-"+clusterName+"-master-0003:19888/ws/v1/history/mapreduce/jobs"

def processedCounters(mr_job, mr_job_counters, jobId):
    mr_job_metrics = {}
    mr_job_stiched_counters = {}
    try:
        for counter_group in mr_job_counters['jobCounters']['counterGroup']:
            for counter in counter_group['counter']:
                mr_job_stiched_counters[counter['name'] + '_mapCounterValue'] = counter['mapCounterValue']
                mr_job_stiched_counters[counter['name'] + '_reduceCounterValue'] = counter['reduceCounterValue']
                mr_job_stiched_counters[counter['name'] + '_totalCounterValue'] = counter['totalCounterValue']

        mr_job_metrics.update(mr_job['job'])
        mr_job_metrics.update(mr_job_stiched_counters)

        return mr_job_metrics
        # print json.dumps(mr_job_metrics)

    except:
        print 'Failed to process ' + jobId

def getJobHistoryUrl(historyUrl, jobId):
    return historyUrl + "/" + jobId

def getJobHistoryCountersUrl(historyUrl, jobId):
    return historyUrl + "/" + jobId +"/counters"

def getJsonRestGet(url):
    response = urllib2.urlopen(url)
    data = response.read()
    return data

def getCounters(clusterName, jobId):
    historyUrl = getHistoryUrl(clusterName)
    jobInfo = getJsonRestGet(getJobHistoryUrl(historyUrl, jobId))
    jobCounters = getJsonRestGet(getJobHistoryCountersUrl(historyUrl, jobId))
    mr_job_metrics = processedCounters(json.loads(jobInfo), json.loads(jobCounters), jobId)
    return mr_job_metrics
    # print json.dumps(mr_job_metrics)

def printCounter(counters, counterName, array ):
    # print counterName, counters.get(counterName)
    if counterName == 'startTime' or counterName == 'finishTime':
        array += str(time.strftime("%d %b %Y %H:%M:%S", time.localtime(counters.get(counterName) / 1000))) + ','
        return array
    if counters.get(counterName) is not None:
        array += str(counters.get(counterName)) +','
    else:
        array += '0,'
    return array

def printAsCsv(counters):
    ## Replace following code with OrderedList
    array = ''
    ## FileSystemCounters
    array = printCounter(counters, 'FILE_BYTES_READ_totalCounterValue', array)
    array = printCounter(counters, 'FILE_BYTES_WRITTEN_totalCounterValue', array)
    array = printCounter(counters, 'FILE_READ_OPS_totalCounterValue', array)
    array = printCounter(counters, 'FILE_LARGE_READ_OPS_totalCounterValue', array)
    array = printCounter(counters, 'FILE_WRITE_OPS_totalCounterValue', array)

    ## HDFS Counters
    array = printCounter(counters, 'HDFS_BYTES_READ_totalCounterValue', array)
    array = printCounter(counters, 'HDFS_BYTES_WRITTEN_totalCounterValue', array)
    array = printCounter(counters, 'HDFS_READ_OPS_totalCounterValue', array)
    array = printCounter(counters, 'HDFS_LARGE_READ_OPS_totalCounterValue', array)
    array = printCounter(counters, 'HDFS_WRITE_OPS_totalCounterValue', array)

    ## Job Info Counters
    array = printCounter(counters, 'killedMapAttempts', array)
    array = printCounter(counters, 'TOTAL_LAUNCHED_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'TOTAL_LAUNCHED_REDUCES_totalCounterValue', array)
    array = printCounter(counters, 'DATA_LOCAL_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'RACK_LOCAL_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'OTHER_LOCAL_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'SLOTS_MILLIS_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'SLOTS_MILLIS_REDUCES_totalCounterValue', array)

    array = printCounter(counters, 'CPU_MILLISECONDS_mapCounterValue', array)
    array = printCounter(counters, 'CPU_MILLISECONDS_reduceCounterValue', array)
    array = printCounter(counters, 'VCORES_MILLIS_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'VCORES_MILLIS_REDUCES_totalCounterValue', array)
    array = printCounter(counters, 'MB_MILLIS_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'MB_MILLIS_REDUCES_totalCounterValue', array)

    ## MapReduce Counters
    array = printCounter(counters, 'MAP_INPUT_RECORDS_totalCounterValue', array)
    array = printCounter(counters, 'MAP_OUTPUT_RECORDS_totalCounterValue', array)
    array = printCounter(counters, 'MAP_OUTPUT_BYTES_totalCounterValue', array)
    array = printCounter(counters, 'MAP_OUTPUT_MATERIALIZED_BYTES_totalCounterValue', array)
    array = printCounter(counters, 'SPLIT_RAW_BYTES_totalCounterValue', array)
    array = printCounter(counters, 'COMBINE_INPUT_RECORDS_totalCounterValue', array)
    array = printCounter(counters, 'COMBINE_OUTPUT_RECORDS_totalCounterValue', array)

    array = printCounter(counters, 'REDUCE_INPUT_GROUPS_totalCounterValue', array)
    array = printCounter(counters, 'REDUCE_SHUFFLE_BYTES_totalCounterValue', array)
    array = printCounter(counters, 'REDUCE_INPUT_RECORDS_totalCounterValue', array)
    array = printCounter(counters, 'REDUCE_OUTPUT_RECORDS_totalCounterValue', array)
    array = printCounter(counters, 'SPILLED_RECORDS_totalCounterValue', array)
    array = printCounter(counters, 'SHUFFLED_MAPS_totalCounterValue', array)
    array = printCounter(counters, 'FAILED_SHUFFLE_totalCounterValue', array)
    array = printCounter(counters, 'MERGED_MAP_OUTPUTS_totalCounterValue', array)

    array = printCounter(counters, 'GC_TIME_MILLIS_totalCounterValue', array)
    array = printCounter(counters, 'CPU_MILLISECONDS_totalCounterValue', array)
    array = printCounter(counters, 'PHYSICAL_MEMORY_BYTES_totalCounterValue', array)
    array = printCounter(counters, 'VIRTUAL_MEMORY_BYTES_totalCounterValue', array)
    array = printCounter(counters, 'COMMITTED_HEAP_BYTES_totalCounterValue', array)

    ## SHUFFLE_ERROR counters
    array = printCounter(counters, 'BAD_ID_totalCounterValue', array)
    array = printCounter(counters, 'CONNECTION_totalCounterValue', array)
    array = printCounter(counters, 'IO_ERROR_totalCounterValue', array)
    array = printCounter(counters, 'WRONG_LENGTH_totalCounterValue', array)
    array = printCounter(counters, 'WRONG_MAP_totalCounterValue', array)
    array = printCounter(counters, 'WRONG_REDUCE_totalCounterValue', array)

    ## FS counters
    array = printCounter(counters, 'BYTES_READ_totalCounterValue', array)
    array = printCounter(counters, 'BYTES_WRITTEN_totalCounterValue', array)

    ## AM counters
    array = printCounter(counters, 'avgMapTime', array)
    array = printCounter(counters, 'avgMergeTime', array)
    array = printCounter(counters, 'avgShuffleTime', array)
    array = printCounter(counters, 'avgReduceTime', array)

    array = printCounter(counters, 'id', array)

    array = printCounter(counters, 'startTime', array)
    array = printCounter(counters, 'finishTime', array)

    print array

def main():
    if len(sys.argv) < 3:
        print "Usage: python FetchJobCounters.py clusterName jobId"

    clusterName = sys.argv[1]
    jobId = sys.argv[2]

    counters= getCounters(clusterName, jobId)
    # print json.dumps(counters)
    printAsCsv(counters)

if __name__ == "__main__":
    main()


