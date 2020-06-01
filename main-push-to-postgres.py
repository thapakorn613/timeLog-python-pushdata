import paho.mqtt.client as mqtt
import classes.postgesProcess as postges
import json
import time
import random

POSTGES_NAME = 'xhoxmzpj'
POSTGES_USER = 'xhoxmzpj'
POSTGES_PASSWORD = 'IkIo-gYsydslbiT6RQUOSnlCpVy0OYyd'
POSTGES_HOST = 'rosie.db.elephantsql.com'
POSTGES_PORT = '5432'

VAR_ID = 0
VAR_SS = 0
VAR_TURBID = 0
VAR_COD = 0
hostMqtt = ("app01-stg.inwini.com")
portMqtt = 1883
gblData = []
idx = 0
pk_id = 0
pk_ss = 0
pk_turbid = 0
pk_cod = 0

myPostges = postges.PostgesProcess(POSTGES_NAME,POSTGES_USER,POSTGES_PASSWORD,POSTGES_HOST,POSTGES_PORT)
myPostges.pg_connect()

def on_connect(self, client, userdata, rc):
    self.subscribe("/turbid1")

def on_message(client, userdata, msg):
    global gblData
    global VAR_ID
    global VAR_SS
    global VAR_TURBID
    global VAR_COD
    global myPostges
    data1 = (msg.payload.decode("utf-8", "strict"))
    data2 = json.loads(data1)
    VAR_ID = data2['id']
    VAR_SS = data2['turbid']
    VAR_TURBID = data2['ss']
    VAR_COD = data2['COD']
    myPostges.pg_insert(VAR_ID, VAR_SS, VAR_TURBID, VAR_COD)
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
        
    
