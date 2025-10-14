import numpy as np
from sqlalchemy import create_engine
import sys
import json
import struct
import paho.mqtt.client as mqtt
import pandas as pd
from io import StringIO
import csv
import time
import re

global Last_DB_Write
global DBTemp_df

#-----------------------helper functions------------------
#this is a special function to convert a pandas dataframe to direct writable CSV - it's referenced in the pandas docs
def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)
		
		
#---------------- Communication client --------------------------  Will need update/rework
#this function sets up the connection to the MQTT Broker and subscribes to all the relevant data
def on_connect(client, userdata, flags, rc):
       global Last_DB_Write
       global DBTemp_df
       global DBConfig
       print("connected to Broker for " + DBConfig['DatabaseTags']['DatabaseName'])
       data = {"success?":"totally", "Database":DBConfig['DatabaseTags']['DatabaseName']}
       jsondata = json.dumps(data)
       client.publish("Relay/Alive/", jsondata)
       client.subscribe("Relay/Data/#") #subscribe to all active intrument servers       
       client.subscribe("Relay/PoisonPill/" + DBConfig['DatabaseTags']['DatabaseName'])
       DBTemp_df = pd.DataFrame()
       Last_DB_Write = time.perf_counter()

#this function responds when a message comes in, it decodes it and then processes the results using the instrument library  
#at some point it might make sense to move the refactoring into another library, but not immediately
def on_message(client, userdata, message):
    try:
        global DBTemp_df
        global Last_DB_Write
        global DBConfig
        Request = message.payload.decode()
        print("Raw Request Payload:", repr(Request))
        RequestTopic= message.topic
        #check for shutdown request
        if RequestTopic  ==    "Relay/PoisonPill/" + DBConfig['DatabaseTags']['DatabaseName']:
           Status = 'Poison pill arrived - server shutting down'
           print(Status)
           Status = json.dumps(Status)    
           client.publish("Relay/ServerIssue/" + DBConfig['DatabaseTags']['DatabaseName'], Status ) 
           sys.exit()    
        try:    #build the data to write
            RequestDict = json.loads(Request)
            equipment = list(RequestDict.keys())[0]
            JSON_df = pd.DataFrame.from_dict(RequestDict[equipment], orient='index')
            DBRead_df = pd.DataFrame()
            DBRead_df['time'] = JSON_df['Time']
            DBRead_df['equipment'] = equipment
            DBRead_df['tagnum'] = JSON_df.index
            DBRead_df['value'] = JSON_df['Value']
            DBRead_df['value'] = DBRead_df['value'].replace(['NULL',False, True, 'OFF', 'ON', 'true', 'false', "OFF", "ON", '', ' '],[np.nan, 0, 1, 0, 1, 1, 0, 0, 1, None, None])   # changed 'OFF' to "OFF" 'ON' to "ON"
            # Step 2: Replace ISO 8601 timestamp strings from redLion with NaN
            DBRead_df['value'] = DBRead_df['value'].apply(
                lambda x: np.nan if isinstance(x, str) and re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', x) else x )
            TimeSince = time.perf_counter() - Last_DB_Write
            print(TimeSince)
            if TimeSince >= 1 :#check how long it has been since we wrote last, if it's more than one second write
                Last_DB_Write = time.perf_counter()
                DBTemp_df = pd.concat([DBTemp_df, DBRead_df], ignore_index=True)
                DBTemp_df.to_sql(DBConfig['DatabaseDetails']['DefaultTable'], engine, if_exists='append', index=False, method = psql_insert_copy)
                DBTemp_df = pd.DataFrame()
            else:
                DBTemp_df = pd.concat([DBTemp_df, DBRead_df], ignore_index=True)
        except Exception as d:
            Status = '[Received bad message or bad database write: ' + str(d) + ']'
            print(Status)
            pass #note - this should just throw out bad messages from Redlion
    except Exception as e:
        Status = '[Received bad message or bad database write: ' + str(e) + ']'
        print(Status)
        Status = json.dumps(Status)    
        client.publish("Relay/ServerIssue/" + DBConfig['DatabaseTags']['DatabaseName'], Status )        
       
#------------------ Load Database Config  ------------------
try:
    DBConfigString = open(sys.argv[1])
    global DBConfig
    DBConfig = json.load(DBConfigString)    
except Exception as e:
    Status = '[Error with config loading: ' + str(e) + ']'
    print(Status)
    sys.exit()  
       
#------------------Initialize DB MQTT Server ------------------       
try:
    client = mqtt.Client()#mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect=on_connect
    client.on_message=on_message
    client.connect('localhost')
    print("server",client)
except Exception as e:
    Status = '[Error with Database server startup: ' + str(e) + ']'
    print(Status)
    Status = json.dumps(Status)    
    client.publish("Relay/ServerIssue/" + DBConfig['DatabaseTags']['DatabaseName'], Status )
    sys.exit()  
    
#------------------Initialize DB Connection ------------------
try:
    engine = create_engine(DBConfig['DatabaseDetails']['Server'])
    pd.options.mode.chained_assignment = None
except Exception as e:
    Status = '[Error - unable to connect to postgresql: ', str(e), ']'
    print(Status)
    Status = json.dumps(Status)    
    client.publish("Relay/ServerIssue/" + DBConfig['DatabaseTags']['DatabaseName'], Status )    
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
     client.publish("Relay/ServerIssue/" + DBConfig['DatabaseTags']['DatabaseName'], Status )
     sys.exit()  
    #Close out - if things break
