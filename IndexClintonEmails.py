__author__ = 'Mark'
import sys
import os
import csv
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch

script_dir = os.path.dirname(__file__)
incidentsFileName = os.path.join(script_dir, "Emails-ascii.csv")

uri = sys.argv[1] if sys.argv[1] else "http://localhost:9200"
es = Elasticsearch(uri)

actions = []
entityNum=0
indexSettings={
     "settings": {
            "number_of_replicas": "0",
            "number_of_shards": "1"
      },
      "mappings": {
         "email": {
            "properties": {
                "from":{
                    "type": "string",
                    "index":"not_analyzed"
                },
                "to":{
                    "type": "string",
                    "index":"not_analyzed"
                },
                "subject":{
                    "type": "string",
                    "index":"analyzed",
                    "fields": {
                        "raw": {
                            "index": "not_analyzed",
                            "ignore_above": 256,
                            "type": "string"
                        }
                    }
                },
                "participants":{
                    "type": "string",
                    "index":"not_analyzed"
                }
            }
         }
      }
   }
indexName="clintonemails"
es.indices.delete(index=indexName, ignore=[400, 404])
es.indices.create(index=indexName, body=indexSettings)
header=[]
with open(incidentsFileName, 'rb') as scsvfile:
    #spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    sreader = csv.reader(scsvfile)
    rowNum = 0
    for row in sreader:
        rowNum += 1
        if rowNum == 1:
            header=row
            print row
            #for idx, val in enumerate(row):
            #    print idx, val
            #exit
            continue
        mdSubject= row[2]
        mdTo= row[3]
        mdFrom= row[4]
        mdDate= row[6] if row[6] else "2008-01-01"
        extractedBody= row[20]
        rawText= row[21]
        print mdSubject
        #for idx,col in enumerate(header):
        #    print idx, col, "=", row[idx]
        #print

        ##Prepare the values that are ingredients for our keys
        #address=row[61]
        #if len(address)>0:
        #    # Cleanse address - some docs had dr. for "drive" while others had "dr"
        #    address=address.replace("."," ").strip()
        #if address=="HOMELESS":
        #    address=""
        #
        #incidentDate=row[28].split(" ")[0]
        #involvement=row[54]
        #OffIncident=row[7]
        #aptnumber=row[62]
        #zipcode=row[63]
        #fullname=row[56]
        #gender=row[58]
        #mo=row[95]
        #
        #age=0
        #if len(row[59])>0:
        #    age = int(row[59])
        #surname=""
        #forename=""
        #if gender  =="M" or gender=="F":
        #    names=fullname.split(", ")
        #    if len(names)>1:
        #        surname=names[0]
        #        forename=names[1]
        #else:
        #    continue
        #
        ##Initialize an entity reference doc for a person
        #complainantDoc = {}
        #complainantDoc["docID"] = row[0]
        #complainantDoc["entrefID"] = row[0]+":"+str(entityNum)
        #complainantDoc["entType"] = "Person"
        #complainantDoc["description"] = involvement+":"+OffIncident+":"+mo
        #complainantDoc["role"] = involvement
        #if len(zipcode)>0:
        #    complainantDoc["wherewhen"]=zipcode+" "+incidentDate
        #
        #personCKeys=[]
        #complainantDoc["keys"]=personCKeys
        #
        #
        ##Build the keys for a person entity reference doc
        #if len(address)>0:
        #    if len(surname)>0:
        #        personCKeys.append(address+"/"+surname)
        #    if len(forename)>0:
        #        if age>0:
        #            personCKeys.append(address+"/"+forename+"/"+str(age))
        #if len(zipcode)>0:
        #    personCKeys.append(zipcode+"/"+forename+"/"+surname+"/"+str(age))
        #
        #
        #actions.append({
        #    "_index": indexName,
        #    '_op_type': 'index',
        #    "_type": "entref",
        #    "_source": complainantDoc
        #})
        #
        #if len(address)>0:
        #    entityNum+=1
        #    addressDoc = {}
        #    addressDoc["docID"] = row[0]
        #    addressDoc["entrefID"] = row[0]+":"+str(entityNum)
        #    addressDoc["entType"] = "Address"
        #    addressDoc["role"] = involvement
        #    addressDoc["description"] = involvement+":"+OffIncident+":"+mo
        #    if len(zipcode)>0:
        #        addressDoc["wherewhen"]=zipcode+" "+incidentDate
        #
        #
        #    addressCKeys=[]
        #    addressDoc["keys"]=addressCKeys
        #    addressCKeys.append(aptnumber+"/"+address)
        #    actions.append({
        #        "_index": indexName,
        #        '_op_type': 'index',
        #        "_type": "entref",
        #        "_source": addressDoc
        #    })
        #
        #
        ## Flush bulk indexing action if necessary
        #if len(actions) >= 5000:
        #    print complainantDoc
        #    print rowNum
        #    helpers.bulk(es, actions)
        #    ## TO check for failures and take appropriate action
        #    del actions[0:len(actions)]
        doc={
            "from": mdFrom,
            "to": mdTo,
            "participants": [mdFrom, mdTo],
            "date":mdDate,
            "subject":mdSubject,
            "body":extractedBody,
            "rawBody":rawText
        }
        actions.append({
                "_index": indexName,
                '_op_type': 'index',
                "_type": "email",
                "_source": doc
        })

print "---------------------------------------------"

while len(actions) > 0:
    print len(actions)
    helpers.bulk(es, actions[:499])
    actions = actions[500:]
