#!/usr/bin/python

import sys
import json
import urllib2
import time


def getHistoryUrl(clusterName):
    return "http://fdp-dev-compute-" + clusterName + "-master-0003:19888/ws/v1/history/mapreduce/jobs"


def getJobHistoryUrl(historyUrl, jobId):
    return historyUrl + "/" + jobId


def getJobHistoryCountersUrl(historyUrl, jobId):
    return historyUrl + "/" + jobId + "/counters"


def getTasksUrl(historyUrl, jobId):
    return historyUrl + "/" + jobId + "/tasks"


def getTaskInfoUrl(historyUrl, jobId, taskId):
    return historyUrl + "/" + jobId + "/tasks/" + taskId


def getTaskCountersUrl(historyUrl, jobId, taskId):
    return historyUrl + "/" + jobId + "/tasks/" + taskId + "/counters"


def getJsonRestGet(url):
    response = urllib2.urlopen(url)
    data = response.read()
    return data


def printCounter(counters, counterName, array):
    if counterName == 'startTime' or counterName == 'finishTime':
        array += str(time.strftime("%d %b %Y %H:%M:%S", time.localtime(counters.get(counterName) / 1000))) + ','
        return array
    if counters.get(counterName) is not None:
        array += str(counters.get(counterName)) + ','
    else:
        array += '0,'
    return array


def printAsCsv(counters, jobId):
    ## Replace following code with OrderedList
    array = ''
    array += jobId + ","
    array = printCounter(counters, 'id', array)
    array = printCounter(counters, 'type', array)
    array = printCounter(counters, 'startTime', array)
    array = printCounter(counters, 'finishTime', array)
    ## FileSystemCounters
    array = printCounter(counters, 'FILE_BYTES_READ', array)
    array = printCounter(counters, 'FILE_BYTES_WRITTEN', array)
    array = printCounter(counters, 'FILE_READ_OPS', array)
    array = printCounter(counters, 'FILE_LARGE_READ_OPS', array)
    array = printCounter(counters, 'FILE_WRITE_OPS', array)

    ## HDFS Counters
    array = printCounter(counters, 'HDFS_BYTES_READ', array)
    array = printCounter(counters, 'HDFS_BYTES_WRITTEN', array)
    array = printCounter(counters, 'HDFS_READ_OPS', array)
    array = printCounter(counters, 'HDFS_LARGE_READ_OPS', array)
    array = printCounter(counters, 'HDFS_WRITE_OPS', array)

    ## MapReduce Counters
    array = printCounter(counters, 'MAP_INPUT_RECORDS', array)
    array = printCounter(counters, 'MAP_OUTPUT_RECORDS', array)
    array = printCounter(counters, 'MAP_OUTPUT_BYTES', array)
    array = printCounter(counters, 'MAP_OUTPUT_MATERIALIZED_BYTES', array)
    array = printCounter(counters, 'SPLIT_RAW_BYTES', array)
    array = printCounter(counters, 'COMBINE_INPUT_RECORDS', array)
    array = printCounter(counters, 'COMBINE_OUTPUT_RECORDS', array)

    array = printCounter(counters, 'REDUCE_INPUT_GROUPS', array)
    array = printCounter(counters, 'REDUCE_SHUFFLE_BYTES', array)
    array = printCounter(counters, 'REDUCE_INPUT_RECORDS', array)
    array = printCounter(counters, 'REDUCE_OUTPUT_RECORDS', array)
    array = printCounter(counters, 'SPILLED_RECORDS', array)
    array = printCounter(counters, 'SHUFFLED_MAPS', array)
    array = printCounter(counters, 'FAILED_SHUFFLE', array)
    array = printCounter(counters, 'MERGED_MAP_OUTPUTS', array)

    array = printCounter(counters, 'GC_TIME_MILLIS', array)
    array = printCounter(counters, 'CPU_MILLISECONDS', array)
    array = printCounter(counters, 'PHYSICAL_MEMORY_BYTES', array)
    array = printCounter(counters, 'VIRTUAL_MEMORY_BYTES', array)
    array = printCounter(counters, 'COMMITTED_HEAP_BYTES', array)

    ## SHUFFLE_ERROR counters
    array = printCounter(counters, 'BAD_ID', array)
    array = printCounter(counters, 'CONNECTION', array)
    array = printCounter(counters, 'IO_ERROR', array)
    array = printCounter(counters, 'WRONG_LENGTH', array)
    array = printCounter(counters, 'WRONG_MAP', array)
    array = printCounter(counters, 'WRONG_REDUCE', array)

    ## FS counters
    array = printCounter(counters, 'BYTES_READ', array)
    array = printCounter(counters, 'BYTES_WRITTEN', array)

    print array


def getCounters(clusterName, jobId):
    historyUrl = getHistoryUrl(clusterName)
    tasks = json.loads(getJsonRestGet(getTasksUrl(historyUrl, jobId)))

    for task in tasks['tasks']['task']:
        try:
            taskId = task['id']
            taskInfo = json.loads(getJsonRestGet(getTaskInfoUrl(historyUrl, jobId, taskId)))
            taskCounters = json.loads(getJsonRestGet(getTaskCountersUrl(historyUrl, jobId, taskId)))

            mr_job_metrics = {}
            mr_job_stiched_counters = {}
            try:
                for counter_group in taskCounters['jobTaskCounters']['taskCounterGroup']:
                    for counter in counter_group['counter']:
                        mr_job_stiched_counters[counter['name']] = counter['value']

                mr_job_metrics.update(taskInfo['task'])
                mr_job_metrics.update(mr_job_stiched_counters)

                printAsCsv(mr_job_metrics, jobId)

            except:
                print 'Failed to process ' + job_id
        except:
            print 'Error occurred while fetching Job ID'


def main():
    if len(sys.argv) < 3:
        print "Usage: python FetchJobCounters.py clusterName jobId"

    clusterName = sys.argv[1]
    jobId = sys.argv[2]
    print "jobId,id,type,startTime,finishTime,FILE_BYTES_READ,FILE_BYTES_WRITTEN,FILE_READ_OPS,FILE_LARGE_READ_OPS,FILE_WRITE_OPS,HDFS_BYTES_READ,HDFS_BYTES_WRITTEN,HDFS_READ_OPS,HDFS_LARGE_READ_OPS,HDFS_WRITE_OPS,MAP_INPUT_RECORDS,MAP_OUTPUT_RECORDS,MAP_OUTPUT_BYTES,MAP_OUTPUT_MATERIALIZED_BYTES,SPLIT_RAW_BYTES,COMBINE_INPUT_RECORDS,COMBINE_OUTPUT_RECORDS,REDUCE_INPUT_GROUPS,REDUCE_SHUFFLE_BYTES,REDUCE_INPUT_RECORDS,REDUCE_OUTPUT_RECORDS,SPILLED_RECORDS,SHUFFLED_MAPS,FAILED_SHUFFLE,MERGED_MAP_OUTPUTS,GC_TIME_MILLIS,CPU_MILLISECONDS,PHYSICAL_MEMORY_BYTES,VIRTUAL_MEMORY_BYTES,COMMITTED_HEAP_BYTES,BAD_ID,CONNECTION,IO_ERROR,WRONG_LENGTH,WRONG_MAP,WRONG_REDUCE,BYTES_READ,BYTES_WRITTEN"
    getCounters(clusterName, jobId)


if __name__ == "__main__":
    main()
