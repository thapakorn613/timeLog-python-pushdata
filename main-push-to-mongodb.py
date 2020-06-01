import paho.mqtt.client as mqtt
import classes.postgesProcess as postges
import json
import time
import random
import pymongo

MONGODB_NAMEDB = 'mydb'
MONGODB_COLLECTION = 'timeLog'

VAR_ID = 0
VAR_SS = 0
VAR_TURBID = 0
VAR_COD = 0

hostMqtt = ("app01-stg.inwini.com")
portMqtt = 1883
gblData = []

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient[MONGODB_NAMEDB]
mycol = mydb[MONGODB_COLLECTION]

mydict = { "name": "John", "address": "Highway 37" }

def insertToMongoDB():
    pass

def on_connect(self, client, userdata, rc):
    self.subscribe("/turbid1")

def on_message(client, userdata, msg):
    global gblData
    global VAR_ID
    global VAR_SS
    global VAR_TURBID
    global VAR_COD
    global mycol
    data1 = (msg.payload.decode("utf-8", "strict"))
    data2 = json.loads(data1)
    VAR_ID = data2['id']
    VAR_SS = data2['turbid']
    VAR_TURBID = data2['ss']
    VAR_COD = data2['COD']
    ret_data = {"id": VAR_ID, "turbid": VAR_SS, "ss": VAR_TURBID, "COD": VAR_COD}
    x = mycol.insert(ret_data)
    print(x)
    client.disconnect()
    client.loop_stop()

client = mqtt.Client()
client.on_connect = on_connect

if __name__ == '__main__':
    print("Start")
    while True:
        client.on_message = on_message
        client.connect(hostMqtt)
        client.username_pw_set("usersb", "usersb")
        client.loop_forever()