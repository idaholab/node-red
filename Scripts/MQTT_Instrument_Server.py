#!/usr/bin/python           # This is server.py file 

import sys
import json
import struct
import time
import paho.mqtt.client as mqtt
import MQTT_Instrument_Lib
from datetime import datetime


#---------------- Communication client --------------------------
#this function sets up the connection to the MQTT Broker and subscribes to all the relevant data
def on_connect(client, userdata, flags, rc):
       print("connected to Broker for " + InstrConfig['EquipmentTags']['EquipmentName'])
       data = {"success?":"totally", "Instrument":InstrConfig['EquipmentTags']['EquipmentName']}
       jsondata = json.dumps(data)
       client.publish("Relay/Alive/", jsondata)
       client.subscribe("Relay/Request/" + InstrConfig['EquipmentTags']['EquipmentName']) #subscribe to node-red requests
       client.subscribe("Relay/PoisonPill/" + InstrConfig['EquipmentTags']['EquipmentName']) #subscribe to node-red poison pill

#this function responds when a message comes in, it decodes it and then processes the results using the instrument library 
def on_message(client, userdata, message):
    try:
        Request = message.payload.decode()
        RequestTopic= message.topic
        if RequestTopic  ==    "Relay/PoisonPill/" + InstrConfig['EquipmentTags']['EquipmentName']:
           Status = 'Poison pill arrived - server shutting down'
           print(Status)
           Status = json.dumps(Status)    
           client.publish("Relay/ServerIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status ) 
           sys.exit()       
        #print("Request_topic",RequestTopic)
        MQTT_Instrument_Lib.InstrumentSelect(Request, InstrConfig, InstrConn, RequestTopic, client)
    except Exception as e:
        Status = '[Received bad message or instrument comms: ' + str(e) + ']'
        print(Status)
        Status = json.dumps(Status)    
        client.publish("Relay/ServerIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )     

#------------------ Load Instrument Config  ------------------
try:
    InstrConfigString = open(sys.argv[1])
    InstrConfig = json.load(InstrConfigString)
except Exception as e:
    Status = '[Error with config loading: ' + str(e) + ']'
    print(Status)
    sys.exit()  

#------------------Initialize MQTT Instrument Server ------------------

try:
    client = mqtt.Client()#(mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect=on_connect
    client.on_message=on_message
    client.connect('localhost')
#    print("server",client)
except Exception as e:
    Status = '[Error with instrument server startup: ' + str(e) + ']'
    print(Status)
    Status = json.dumps(Status)
    client.publish("Relay/ServerIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
    sys.exit()       

#--------------Set up the correct communication protocol--------------
try:  
    InstrConn = MQTT_Instrument_Lib.InitializeInstrumentConnection(InstrConfig, client)
except Exception as e:
    Status = '[Error with instrument connection: ' + str(e) + ']'
    print(Status)
    Status = json.dumps(Status)    
    client.publish("Relay/ServerIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
    sys.exit() 

#--------------------Run Server ----------------------------
while True:
   try:
     client.loop_forever()
        #print("serverclient",client)
   except Exception as e:
     Status = "Something broke, not sure what" + str(e)
     print(Status)
     Status = json.dumps(Status)     
     client.publish("Relay/ServerIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
     sys.exit()  
    #Close out - if things break
