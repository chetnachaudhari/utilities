import urllib2
import json

response = urllib2.urlopen('http://gamma-bheema-master-0003:19888/ws/v1/history/mapreduce/jobs?user=rm-split')
data = json.load(response)  
joblist = data['jobs']['job']
ids = [job['id'] for job in joblist]
basepath = "http://gamma-bheema-master-0003:19888/ws/v1/history/mapreduce/jobs/"
#print ids
for jobId in ids:
   url = basepath + jobId + "/counters"
   response1 = urllib2.urlopen(url)
   print jobId, "," , json.dumps(json.load(response1))
   #print jobId
