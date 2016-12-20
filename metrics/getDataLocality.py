import urllib2
import json

ids = [line.strip() for line in open("/Users/chetna.chaudhari/Repositories/Chetna/utilities/metrics/jobList.txt", 'r')]
basepath = "http://gamma-bheema-master-0003:19888/ws/v1/history/mapreduce/jobs/"
#print ids
for jobId in ids:
   url = basepath + jobId + "/counters"
   response1 = urllib2.urlopen(url)
   counters = json.load(response1)
   jobCounters = counters
   print jobId, "|" , json.dumps(json.load(response1))
   #print jobId
