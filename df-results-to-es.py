from elasticsearch import Elasticsearch,helpers
import os,subprocess,datetime,time

localtime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
index_timestamp = datetime.datetime.now().strftime("%Y-%m")
timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
epoch = time.time() # timestamp 
date = datetime.date.today()

actions = []
types = "_doc"
esIndexName = "datamgmt-fs-usage-"+str(index_timestamp)
es = Elasticsearch(hosts=['elastic-test.winstorm.nssl:9200'], timeout=30,http_auth=('user.name', 'password'))

response = es.indices.create(index=esIndexName,body={"settings":{"number_of_shards":3,"number_of_replicas":0}},ignore=400)
#print(response)

usage = {}
#print(timestamp,epoch)
dfResults = subprocess.check_output('df')
dfResults = dfResults.decode()
dfList =  dfResults.split('\n')

for fsInstance in dfList:
	fsList = fsInstance.split()
	#print(fsList)
	if len(fsList)>0:
		if fsList[0]!='Filesystem':
			svm = fsList[0].split('.')[0]	
			if svm in ['netapp-dataserver0','netapp-dataserver1','netapp-web1','netapp-repos']:
				#print(fsList)
				fs = fsList[0]
				volumeQtree = fs.split(':')[-1]
				
				volume = volumeQtree.split('/')[1]
				#print(volumeQtree,volume)
				if len(volume)==7:
					division = volume[0:4]
				elif len(volume)==6:
					division = volume[0:3]
				size = int(fsList[1])/1024.0**3
				used = int(fsList[2])/1024.0**3
				avail = int(fsList[3])/1024.0**3
				percentUsed = used/float(size)
					
				if division not in usage.keys():
					usage[division] = {volumeQtree:{'size':size,'used':used,'available':avail,'percent_used':percentUsed}}
				else:
					if volumeQtree not in usage[division].keys():
						usage[division][volumeQtree] = {'size':size,'used':used,'available':avail,'percent_used':percentUsed}
					else:
						pass

#print(usage)			

for div in usage.keys():
	for volumeQtree in usage[div].keys():
		volume = volumeQtree.split('/')[1]
		#print(volume)
		if volume not in ['audit','nfsaudit']:
			qtree = volumeQtree.split('/')[2]
			properties = usage[div][volumeQtree]
			available = round(properties['available'],3)
			used = round(properties['used'],3)
			percent_used = round(properties['percent_used'],2)
			size = round(properties['size'],3)
			#print( div, volume,qtree,volumeQtree,available,used,percent_used,size,str(timestamp))
			payload = {"_index":esIndexName,"_type":types,"division":div,"volume":volume,"qtree":qtree,"volume_qtree":volumeQtree,"available_tb":available,"used_tb":used,"percent_used":percent_used,"size_tb":size,"observed":str(timestamp),"localtime":str(localtime)}
			actions.append(payload)
#print(len(actions))
helpers.bulk(es,actions=actions)
