#!/usr/bin/python           # This is server.py file 
#from threading import Thread

import json
import sys
import pandas as pd
import numpy as np
import math
import time
import struct
from datetime import datetime, timedelta 
import subprocess
import platform
from sqlalchemy import text

#-----------------------Helper functions------------------
# This script pulls the config file and returns it as a json dictonary file
def get_instrument_config():
    ConfigFile = "/home/SOEC-Data/SOEC-Code/Config/Facility-Opto-Config.json"
    #load config file
    with open(ConfigFile, 'r') as jsonconfig:
        config = json.load(jsonconfig)
    return config

# This function is used to setup the instruments from their config files. 
# The function is called in the instrument server for every equipment.
def InstrumentSelect(Request, InstrConfig, InstrConn, RequestTopic, client):
    # Check for poison pill first

    if RequestTopic  == "Relay/PoisonPill/" + InstrConfig['EquipmentTags']['EquipmentName']:
        sys.exit()
    # Defining the communication type used to interface with the instrument
    InstrType  = InstrConfig['EquipmentTags']['Communication'] 
    # For any non equipment not using MQTT as communication the function will load the request,
    # determine whether the intention is to read or write to an instrument and finaly compile
    # the function that will perform the request
    if InstrType != 'MQTT':
        RequestDetails = json.loads(Request)
        #print(RequestDetails)
        ReadWrite = RequestDetails['Read/Write']
        FunctionName = InstrType + ReadWrite
        func = globals()[FunctionName]
        ReturnData = func(RequestDetails, InstrConfig, InstrConn, client)   
    else:
        if RequestTopic == InstrConfig['EquipmentTags']['DataReadTopic']:
           ReturnData = MQTTR(Request, RequestTopic, InstrConfig, InstrConn, client)
        elif RequestTopic == "Relay/Request/" + InstrConfig['EquipmentTags']['EquipmentName']:
             RequestDetails = json.loads(Request)
             ReadWrite = RequestDetails['Read/Write']
             FunctionName = InstrType + ReadWrite
             func = globals()[FunctionName]
             ReturnData = func(Request, RequestTopic, InstrConfig, InstrConn, client)
    # The output from the function determined above will be dumped in json format to the jsondata
    jsondata = json.dumps(ReturnData)
    # The topic for the client publishing will be the Equipment name
    Topic = "Relay/Data/" + InstrConfig['EquipmentTags']['EquipmentName']
    # Publishing the data to the topic on the instrument client
    client.publish(Topic, jsondata)
    # Returning the data as an output from the function.
    return ReturnData


def SCPIConvert(DatatoConvert, row):
    #manage the potential for an array return (stupid NHR...)
    if ',' in DatatoConvert:
        TempArray = DatatoConvert.split(',')
        TempData = TempArray[int(row['ArrayPos'])]
    else:
        TempData = DatatoConvert
    #Convert NR1 and NR2
    TempData = TempData.strip().replace('\x00', '')  # remove any null characters
    if row['DataType'] == 'NR1':
        FValue = int(TempData)
    elif row['DataType'] == 'Bool':
        FValue=int(TempData)
    elif row['DataType'] == 'int16':
        FValue=int(TempData)
    else:
        FValue = float(TempData)
    return FValue

def SCPIInvert(Row):
    OriginVal = Row['Value']
    if Row['DataType'] == "HEX":
        # Inputing a decimal number and converting it to hex
        FormatVal = hex(OriginVal)
    elif Row['DataType'] == "bool" or Row['DataType'] == "bit":
        # Converts a boolean input to an integer for Modbus write
        FormatVal = int(OriginVal)
    elif Row['DataType'] == "int32" or Row['DataType'] == "NR2":
        # Unformatted int32
        FormatVal = OriginVal
    elif math.isnan(OriginVal):
        # Checks if the reading is numeric or not
        FormatVal == "NULL"
    else:
        # Scaling the input and format to register format
        OriginValF = np.array(OriginVal, dtype="float32")
        RawVal = (OriginValF/Row['Scalar']) - Row['Offset']
        NpInt = np.array(RawVal, dtype=Row['DataType'])
        FormatVal = NpInt.tolist()
    return FormatVal


# This function is used to convert the received data from a Modbus Read to the requested data type
# found in the config files for the instruments.     

def ModbusConvert(Row):
    if Row['DataType'] == "HEX":
        # Converts a hexadecimal input to an integer
        ValueString = '0x' + str(Row['Value'])
        FullVal = int(ValueString, 16)
    elif Row['DataType'] == "bool" or Row['DataType'] == "bit":
        # Converts a register containing 1 or 0 to a bool
        FullVal = Row['Value']      
    elif Row['DataType'] == "int32":
        # Reads the value directly only used for the RDTS server
        FullVal = Row['Value']
    elif math.isnan(Row['Value']):
        # If the value isn't a number an exception is called
        FullVal = "NULL"
    else:
        # Converts the value to a float and apply the scaling before rounding it to decimal
        NpInt = np.array(Row['Value'], Row['DataType'])
        Raw = NpInt.astype('float32')
        OffsetVal = Raw + Row['Offset']
        ScaledVal = OffsetVal * Row['Scalar']
        FullVal =  round(ScaledVal, int(Row['Decimal']))  
    return FullVal 
    
def ModbusInvert(Row):
    OriginVal = Row['Value']
    if Row['DataType'] == "HEX":
        # Inputing a decimal number and converting it to hex
        FormatVal = hex(OriginVal)
    elif Row['DataType'] == "bool" or Row['DataType'] == "bit":
        # Converts a boolean input to an integer for Modbus write
        FormatVal = int(OriginVal)
    elif Row['DataType'] == "int32":
        # Unformatted int32
        FormatVal = OriginVal
    elif math.isnan(OriginVal):
        # Checks if the reading is numeric or not
        FormatVal == "NULL"
    else:
        # Scaling the input and format to register format
        OriginValF = np.array(OriginVal, dtype="float32")
        RawVal = (OriginValF/Row['Scalar']) - Row['Offset']
        NpInt = np.array(RawVal, dtype=Row['DataType'])
        FormatVal = NpInt.tolist()
    return FormatVal
    
def ModbusBuilder(builder,Row,FormatVal):
    if Row['DataType'] == "bool" or Row['DataType'] == "bit":
        # Adds a byte to the payload
        payload = builder.add_bits([FormatVal])        
    elif Row['DataType'] == "int16":
        # Reads the value directly only used for the RDTS server
        payload = builder.add_16bit_int(FormatVal)
    elif Row['DataType'] == "uint16":
        # Reads the value directly only used for the RDTS server
        payload = builder.add_16bit_uint(FormatVal) 
    elif Row['DataType'] == "float16":
        # Reads the value directly only used for the RDTS server
        payload = builder.add_16bit_float(FormatVal) 
    elif Row['DataType'] == "int32":
        # Reads the value directly only used for the RDTS server
        payload = builder.add_32bit_int(FormatVal)
    elif Row['DataType'] == "float32":
        # Reads the value directly only used for the RDTS server
        payload = builder.add_32bit_float(FormatVal)    
    else:
        payload = 0
    return payload
    
def ModbusDecoder(BinValue, Row):
    if Row['DataType'] == "bool":
        # Converts a register containing 1 or 0 to a bool
        FullVal = BinValue.decode_bits()[0]
    elif Row['DataType'] == "bit":
        # Reads a specific bit from a reading specified by the BitNum property
        # Note: Requires the BitStart and BitEnd inputs in the config file
        IntVal = BinValue.decode_16bit_int()
        BinVal = bin(InVal)
        BinVal = BinVal[2:]
        FullVal = BinVal[Row['BitStart']:Row['BitEnd']+1]
    elif Row['DataType'] == "int16":
        # Reads the value directly only used for the RDTS server
        FullVal = BinValue.decode_16bit_int()
    elif Row['DataType'] == "uint16":
        # Reads the value directly only used for the RDTS server
        FullVal = BinValue.decode_16bit_uint() 
    elif Row['DataType'] == "float16":
        # Reads the value directly only used for the RDTS server
        FullVal = BinValue.decode_16bit_float() 
    elif Row['DataType'] == "int32":
        # Reads the value directly only used for the RDTS server
        FullVal = BinValue.decode_32bit_int()
    elif Row['DataType'] == "float32":
        # Reads the value directly only used for the RDTS server
        FullVal = BinValue.decode_32bit_float()    
    else:
        FullVal = BinValue
    return FullVal
    
#Channel Dataframe Buildout
def ChanDFBuild(RequestDetails, InstrConfig):
    #build channel short list  
    ChannelList = []
    KeyList = []
    ValueList = []
    for key in RequestDetails['Channels']:    
        ChannelList.append(InstrConfig['Channels'][key])
        KeyList.append(key)
        ValueList.append(RequestDetails['Channels'][key])
    ChanDF = pd.DataFrame(ChannelList)
    ChanDF['Tagnum'] = KeyList
    ChanDF['Value'] = ValueList
    return ChanDF


#-----------------------Communication functions------------------
def InitializeInstrumentConnection(InstrConfig, client): 
    try:
        if InstrConfig['EquipmentTags']['Communication'] == 'ModbusTcp' or InstrConfig['EquipmentTags']['Communication'] == 'SingleModbusTcp' or InstrConfig['EquipmentTags']['Communication'] == 'DoubleModbusTcp' or InstrConfig['EquipmentTags']['Communication'] == 'ModbusTcpKRBH' or InstrConfig['EquipmentTags']['Communication'] == 'ModbusTcpGeneric':
            #fire up modbus server
            #from pymodbus.client.sync import ModbusTcpClient
            from pymodbus.client import ModbusTcpClient  #expensive library, don't load unless you need it
            InstrConn = ModbusTcpClient(InstrConfig['EquipmentTags']['Connection']['IP'],InstrConfig['EquipmentTags']['PortNum']) # I added the PortNum Entry for devices using a different Port number from the default "502"
            InstrConn.connect()
        elif InstrConfig['EquipmentTags']['Communication'] == 'ModbusRTU' or InstrConfig['EquipmentTags']['Communication'] == 'ModbusRTUDouble':
            #from pymodbus.client.sync import ModbusSerialClient as ModbusClient #initialize$
            from pymodbus.client import ModbusSerialClient as ModbusClient #initialize$
            from pymodbus.transaction import ModbusRtuFramer
            InstrConn = ModbusClient(method = "rtu", port=InstrConfig['EquipmentTags']['Connection']['Port'] ,stopbits = int(InstrConfig['EquipmentTags']['Connection']['Stopbits']), bytesize = int(InstrConfig['EquipmentTags']['Connection']['Bytesize']) , parity = InstrConfig['EquipmentTags']['Connection']['Parity'], baudrate= int(InstrConfig['EquipmentTags']['Connection']['Baudrate']))
            InstrConn.connect()
        elif InstrConfig['EquipmentTags']['Communication'] == 'Opto':
            #fire up opto 
            import optommp
            InstrConn =  optommp.O22MMP()
        elif InstrConfig['EquipmentTags']['Communication'] == 'SCPI':
            import pyvisa
            rm = pyvisa.ResourceManager()
            #Connect via pyvisa
            InstrConn = rm.open_resource(InstrConfig['EquipmentTags']['VISA_address'])
            InstrConn.read_termination = InstrConfig['EquipmentTags']['ReadTerminalChar']
            InstrConn.write_termination = InstrConfig['EquipmentTags']['WriteTerminalChar']
            #InstrConn.timeout = 5000   # 5 seconds
        elif InstrConfig['EquipmentTags']['Communication'] == 'MQTT':
            RequestTopic = InstrConfig['EquipmentTags']['DataReadTopic'] #subscribe to node-red
            client.subscribe(RequestTopic)
            InstrConn = client
        elif InstrConfig['EquipmentTags']['Communication'] == 'Ping':
            InstrConn = InstrConfig['EquipmentTags']['TargetIP'] #Set Ping address
        elif InstrConfig['EquipmentTags']['Communication'] == 'PostGreSQL':
            from sqlalchemy import create_engine
            engine = create_engine(InstrConfig['EquipmentTags']['Server'])
            InstrConn = engine.connect()
        else:
            #Do nothing
            InstrConn =  'Virtual'
        return(InstrConn)
    except Exception as e:
        Status = 'Issue with Instrument connection' + str(e)
        print(Status)
        Status = json.dumps(Status)
        client.publish("Relay/ServerIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
        sys.exit()   

def PostGreSQLR(RequestDetails, InstrConfig, InstrConn, client): 
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    for index, row in ChanDF.iterrows():      
        try: #note - this is the first draft! we're only doing direct SQL queries, later we might add fancy select statments or similar, but that's a future me problem
           querybuild = InstrConn.execute(text(row['QueryText']))
           row['Value'] = querybuild.fetchone()[0]
           FValue = ModbusConvert(row)           
           ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']] = {'Value': FValue, 'Time': row['Time']} 
        except Exception as e:
            Status = 'Issue with Database Read' + str(e)
            print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass     
    return(ReturnData)   

def VirtualR(RequestDetails, InstrConfig, InstrConn, client): #One register at a time reads
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    for index, row in ChanDF.iterrows():        
        try:
            #row['Value'] = 15 
            FValue = ModbusConvert(row)
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}            
        except Exception as e:
            Status = 'Issue with Virtual Read' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData)

def VirtualW(RequestDetails, InstrConfig, InstrConn, client): #One register at a time writess
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    for index, row in ChanDF.iterrows():        
        try:
            #row['Value'] = 12   
            FValue = row['Value'] #ModbusConvert(row)
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}            
        except Exception as e:
            Status = 'Issue with Virtual write' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData)   

def ModbusRTUR(RequestDetails, InstrConfig, InstrConn, client):
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']: {}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    
    for index, row in ChanDF.iterrows():
        try:
            if row['Type'] == 'HRegister' and row['Registers'] == 1:
                row['Value'] = InstrConn.read_holding_registers(row['IOPoint'], 1, int(row['Unit'])).registers[0]
                FValue = ModbusConvert(row)
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']] = {'Value': FValue, 'Time': row['Time']} 

            elif row['Type'] == 'HRegister' and row['Registers'] == 2:
                Ftemp = InstrConn.read_holding_registers(row['IOPoint'], 2, int(row['Unit'])).registers
                LSB = int(Ftemp[0])
                MSB = int(Ftemp[1])
                Fvar = struct.unpack('!f', bytes.fromhex('{0:04x}'.format(LSB) + '{0:04x}'.format(MSB)))
                FValue = round(Fvar[0], 2)
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']] = {'Value': FValue, 'Time': row['Time']} 

        except Exception as e:
            Status = 'Issue with Single modbusRTU Read: ' + str(e)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status)
            pass

    return(ReturnData)
        
def ModbusRTUW(RequestDetails, InstrConfig, InstrConn, client):  # One register at a time write
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']: {}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

    for index, row in ChanDF.iterrows():        
        try:

            ScaledOutput = ModbusInvert(row)
            from pymodbus.payload import BinaryPayloadBuilder
            from pymodbus.payload import BinaryPayloadDecoder
            
            bo = '<' if 'ByteOrder' in InstrConfig['EquipmentTags'] and InstrConfig['EquipmentTags']['ByteOrder'] == "Little" else '>'
            wo = '<' if 'WordOrder' in InstrConfig['EquipmentTags'] and InstrConfig['EquipmentTags']['WordOrder'] == "Little" else '>'
           
            builder = BinaryPayloadBuilder(byteorder=bo, wordorder=wo) 
                        
            payload = ModbusBuilder(builder,row,ScaledOutput)
                        
            payload = builder.build()

            #### use row['Unit'] because unlike the TCP version, a serial connection will have multiple units
            print("number of registers is " + str(int(row['Unit'])))          
            if row['Type'] == 'HRegister' :
                InstrConn.write_registers(row['IOPoint'], payload, int(row['Unit']), skip_encode=True)
                DataArray = InstrConn.read_holding_registers(row['IOPoint'], row['Registers'], int(row['Unit'])).registers
                print(DataArray)
                BinValue =  BinaryPayloadDecoder.fromRegisters(DataArray, byteorder=bo, wordorder=wo) 
                row['Value'] = ModbusDecoder(BinValue,row)
            elif row['Type'] == 'IRegister':
                #print("this is an input register")
                InstrConn.write_registers(row['IOPoint'], payload, int(row['Unit']), skip_encode=True)  
                DataArray = InstrConn.read_input_registers(row['IOPoint'], row['Registers'], int(row['Unit'])).registers     
                BinValue =  BinaryPayloadDecoder.fromRegisters(DataArray, byteorder=bo, wordorder=wo) 
                row['Value'] = ModbusDecoder(BinValue,row)
            else: # write coil
                InstrConn.write_coils(row['IOPoint'], row['Value'], int(row['Unit']), skip_encode=True)               
                row['Value'] = InstrConn.read_coils(row['IOPoint'], row['Registers'], int(row['Unit'])).bits
            
            
            FValue = ModbusConvert(row)
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}            

        except Exception as e:
            Status = 'Issue with Single modbusRTU Write: ' + str(e)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status)
            pass

    return(ReturnData)

def ModbusTcpGenericR(RequestDetails, InstrConfig, InstrConn, client): #One register at a time reads
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    for index, row in ChanDF.iterrows():        
        try:
            
            from pymodbus.payload import BinaryPayloadDecoder
            bo = '<' if 'ByteOrder' in InstrConfig['EquipmentTags'] and InstrConfig['EquipmentTags']['ByteOrder'] == "Little" else '>'
            wo = '<' if 'WordOrder' in InstrConfig['EquipmentTags'] and InstrConfig['EquipmentTags']['WordOrder'] == "Little" else '>'
            
            if row['Type'] == 'HRegister':
                DataArray = InstrConn.read_holding_registers(row['IOPoint'], row['Registers'], int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers       
                BinValue =  BinaryPayloadDecoder.fromRegisters(DataArray, byteorder=bo, wordorder=wo) 
                row['Value'] = ModbusDecoder(BinValue,row)
            elif row['Type'] == 'IRegister':            
                DataArray = InstrConn.read_input_registers(row['IOPoint'], row['Registers'], int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers     
                BinValue =  BinaryPayloadDecoder.fromRegisters(DataArray, byteorder=bo, wordorder=wo) 
                row['Value'] = ModbusDecoder(BinValue,row)
            elif row['Type'] == 'DiscreteInputs':
                row['Value'] = InstrConn.read_discrete_inputs(row['IOPoint'], row['Registers'], int(InstrConfig['EquipmentTags']['Connection']['Unit'])).bits[0]
            else: #read coils
                row['Value'] = InstrConn.read_coils(row['IOPoint'], row['Registers'], int(InstrConfig['EquipmentTags']['Connection']['Unit'])).bits[0]
            
            FValue = ModbusConvert(row)
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}            
        except Exception as e:
            Status = 'Issue with Updated modbus read' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData)   

def ModbusTcpGenericW(RequestDetails, InstrConfig, InstrConn, client): #One register at a time reads
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    for index, row in ChanDF.iterrows():        
        try:
            
            ScaledOutput = ModbusInvert(row)
            
            from pymodbus.payload import BinaryPayloadBuilder
            from pymodbus.payload import BinaryPayloadDecoder
            
            bo = '<' if 'ByteOrder' in InstrConfig['EquipmentTags'] and InstrConfig['EquipmentTags']['ByteOrder'] == "Little" else '>'
            wo = '<' if 'WordOrder' in InstrConfig['EquipmentTags'] and InstrConfig['EquipmentTags']['WordOrder'] == "Little" else '>'
           
            builder = BinaryPayloadBuilder(byteorder=bo, wordorder=wo) 
                        
            payload = ModbusBuilder(builder,row,ScaledOutput)
                        
            payload = builder.build()
          
            if row['Type'] == 'HRegister' :
                InstrConn.write_registers(row['IOPoint'], payload, int(InstrConfig['EquipmentTags']['Connection']['Unit']), skip_encode=True)
                DataArray = InstrConn.read_holding_registers(row['IOPoint'], row['Registers'], int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers
                BinValue =  BinaryPayloadDecoder.fromRegisters(DataArray, byteorder=bo, wordorder=wo) 
                row['Value'] = ModbusDecoder(BinValue,row)
            elif row['Type'] == 'IRegister':
                InstrConn.write_registers(row['IOPoint'], payload, int(InstrConfig['EquipmentTags']['Connection']['Unit']), skip_encode=True)  
                DataArray = InstrConn.read_input_registers(row['IOPoint'], row['Registers'], int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers     
                BinValue =  BinaryPayloadDecoder.fromRegisters(DataArray, byteorder=bo, wordorder=wo) 
                row['Value'] = ModbusDecoder(BinValue,row)
            else: # write coil
                InstrConn.write_coils(row['IOPoint'], row['Value'], int(InstrConfig['EquipmentTags']['Connection']['Unit']), skip_encode=True)               
                row['Value'] = InstrConn.read_coils(row['IOPoint'], row['Registers'], int(InstrConfig['EquipmentTags']['Connection']['Unit'])).bits
            
            
            FValue = ModbusConvert(row)
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}            
        except Exception as e:
            Status = 'Issue with single modbus write' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData)  

def ModbusTcpR(RequestDetails, InstrConfig, InstrConn, client): #Multi-register reads
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    ChanGroups = ChanDF.groupby('Type')
    for key, data in ChanGroups:    
        IOmax = ChanGroups['IOPoint'].max().values[0]
        IOmin = ChanGroups['IOPoint'].min().values[0]
        try:
            if key == 'HRegister':             
                DataArray = InstrConn.read_holding_registers(IOmin, IOmax - IOmin + 1, unit = int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers
            elif key == 'IRegister':
                DataArray = InstrConn.read_input_registers(IOmin, IOmax - IOmin + 1, unit = int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers        
            else: #read coils
                DataArray = InstrConn.read_coils(IOmin, IOmax - IOmin + 1, unit = int(InstrConfig['EquipmentTags']['Connection']['Unit'])).bits         
            DataDF= pd.DataFrame(DataArray, columns=['Values'])
            DataDF['Regnum']= range(IOmin, IOmax+1)
            DataDict = DataDF.set_index('Regnum')['Values'].T.to_dict()
            data['Value'] = data['IOPoint'].map(DataDict)
            data['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']]
            for index, row in data.iterrows():
                FValue = ModbusConvert(row)
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
        except Exception as e:
            Status = 'Issue with modbus read generic ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData)                

def ModbusTcpW(RequestDetails, InstrConfig, InstrConn, client): #One register at a time reads
    #print('modbus write')
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    for index, row in ChanDF.iterrows():        
        try:
            if row['Type'] == 'HRegister' :
                InstrConn.write_register(row['IOPoint'], row['Value'], unit = int(InstrConfig['EquipmentTags']['Connection']['Unit']))
                row['Value'] = InstrConn.read_holding_registers(row['IOPoint'], 1, unit = int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers[0]
            elif row['Type'] == 'IRegister':
                InstrConn.write_register(row['IOPoint'], row['Value'], unit = int(InstrConfig['EquipmentTags']['Connection']['Unit']))  
                row['Value'] = InstrConn.read_input_registers(row['IOPoint'], 1, unit = int(InstrConfig['EquipmentTags']['Connection']['Unit'])).registers[0]      
            else: #read coils
                InstrConn.write_coil(row['IOPoint'], row['Value'], unit = int(InstrConfig['EquipmentTags']['Connection']['Unit']))               
                row['Value'] = InstrConn.read_coils(row['IOPoint'], 1, unit = int(InstrConfig['EquipmentTags']['Connection']['Unit'])).bits[0]   
            FValue = ModbusConvert(row)
          #ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}    
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}            
        except Exception as e:
            Status = 'Issue with modbus write generic ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData) 

def OptoR(RequestDetails, InstrConfig, InstrConn, client): 
    #read channels
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")   
    for index, row in  ChanDF.iterrows():
        try:
            #read the register and scale it appropriately                     
            if row['Type'] == 'Analog':
               row['Value'] = InstrConn.GetAnalogPointValue(int(row['Module']), int(row['Channel']))   
            #read the coil
            elif row['Type'] == 'Feature':    
               memoryread = InstrConn.ReadBlock(0xF01E0010+0x1000*(int(row['Module']))+0x40*int(row['Channel']),4)
               memoryread_float=struct.unpack(">fffff",memoryread)
               row['Value'] = memoryread_float[4]
            else:
                row['Value'] = InstrConn.GetDigitalPointState(int(row['Module']), int(row['Channel']))
            FValue = ModbusConvert(row)
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
        except Exception as e:
            Status = 'Issue with Opto Read ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return ReturnData  

def OptoW(RequestDetails, InstrConfig, InstrConn, client): 
    #read channels
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")   
    for index, row in  ChanDF.iterrows():
        try:
            if row['Type'] == 'Analog':
               InstrConn.SetAnalogPointValue(int(row['Module']), int(row['Channel']), int(row['Value']))                
               row['Value'] = InstrConn.GetAnalogPointValue(int(row['Module']), int(row['Channel']))     
            #read the coil
            else:    
               InstrConn.SetDigitalPointState(int(row['Module']), int(row['Channel']), int(row['Value']))
               row['Value'] = InstrConn.GetDigitalPointState(int(row['Module']), int(row['Channel']))
            FValue = ModbusConvert(row)
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
        except Exception as e:
            Status = 'Issue with Opto Write ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return ReturnData   

def MQTTR(Request, RequestTopic, InstrConfig, InstrConn, client):
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    Request_mod = json.loads(Request)
    RequestDetails = json.loads(Request)
    if RequestTopic == InstrConfig['EquipmentTags']['DataReadTopic']:
        for key, value in Request_mod.items():
            Time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            FValue = value
            Tagnum = key
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][Tagnum]={"Value": FValue, "Time": Time}
        #print("ReturnData_Relay_1",ReturnData)
    else:
        ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
        ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Time = ChanDF['Time'][0]
        First_Tag = ChanDF['Tagnum'][0]
        ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][First_Tag] = {'Value': 1, 'Time': Time}
    return ReturnData
       
def MQTTW(Request, RequestTopic, InstrConfig, InstrConn, client):
    Topic = InstrConfig['EquipmentTags']['DataWriteTopic']
    ReturnData = {} 
    Time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    Request_mod = json.loads(Request)
    del Request_mod['Read/Write']
    #print("data_requested_mod",Request_mod)
    for key, value in Request_mod.items():
        FValue = value
        ReturnData.update(FValue)
        #print("Request_requested",ReturnData, type(FValue))
    client.publish(Topic, json.dumps(ReturnData))
#    print("data_from_mqttW",ReturnData)
    return ReturnData

def SCPIR(RequestDetails, InstrConfig, InstrConn, client):
    # Read channels
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']: {}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    InitFlag = False
    for index, row in ChanDF.iterrows():
        try:
            # row['Type'] == 'Analog' #confirm data types
            if (row['Init_Func'] == True and InitFlag == False):
                QueryText = row['Init_Text'] + row['IOPoint'] + '?'
                DatatoConvert = InstrConn.query(QueryText)  # read and return the data
                InitFlag = True
            else:
                QueryText = row['RequestText'] + row['IOPoint'] + '?'
                DatatoConvert = InstrConn.query(QueryText)  # read and return the data
            FValue = SCPIConvert(DatatoConvert, row) #convert to relevant values
            ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']] = {'Value': FValue, 'Time': row['Time']}
        except Exception as e:
            Status = 'Issue with SCPI Read ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status)
            pass
            
    return ReturnData

def SCPIW(RequestDetails, InstrConfig, InstrConn, client): 
    #read channels
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")   
    for index, row in  ChanDF.iterrows():
        try:
            if row['Type'] == 'Analog': #confirm data types
                WriteText = "OUTP" + row['IOPoint']
                params = row['Value'].split(',')
                mode, enable, voltage, current, power, resistance = map(float, params)
                command = f"{WriteText} {mode},{enable},{voltage},{current},{power},{resistance}"
                InstrConn.write(command)
                read_cmd = f"{row['IOPoint']}?"
                FValue = InstrConn.query(read_cmd) #write, and read the data has been written
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
            else:
                value = SCPIInvert(row)                
                command = row['RequestText'] + row['IOPoint'] + " " + str(value)
                InstrConn.write(command)
                QueryText = row['RequestText'] + row['IOPoint'] + '?'      
                DatatoConvert = InstrConn.query(QueryText)
                FValue = SCPIConvert(DatatoConvert, row) #convert to relevant values          
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']] = {'Value': FValue, 'Time': row['Time']}                
        except Exception as e:
            Status = 'Issue with SCPI Write ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    #InstrConn.close()
    return ReturnData

def PingW(RequestDetails, InstrConfig, InstrConn, client): 
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    global last_ping_time
    for index, row in ChanDF.iterrows():        
        try:
            state = row['Value']
            if state == 0:
                count=4
                command = ["ping", "-c", str(count), InstrConn]
                result = subprocess.run(command, capture_output=True, text=True, check=True)
                last_ping_time = datetime.now()
                print('last_ping_time_W', last_ping_time )
                FValue = result.returncode;
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
            else: 
                FValue = state;
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
        except Exception as e:
            Status = 'Issue with ping read generic ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData)
    

def PingR(RequestDetails, InstrConfig, InstrConn, client): 
    ChanDF = ChanDFBuild(RequestDetails, InstrConfig)
    ReturnData = {InstrConfig['EquipmentTags']['EquipmentName']:{}}
    ChanDF['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    for index, row in ChanDF.iterrows():        
        try:
            if last_ping_time is None:  #No ping has ever occurred
                FValue = 1;
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
            elif datetime.now() - last_ping_time > timedelta(seconds=2): #Missed ping! No ping in the last 2 seconds.
                print('last_ping_time_R', last_ping_time )
                FValue = 1;
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
            else:
                FValue = 0;
                ReturnData[InstrConfig['EquipmentTags']['EquipmentName']][row['Tagnum']]={'Value': FValue, 'Time': row['Time']}
        except Exception as e:
            Status = 'Issue with ping read generic ' + str(e)
            #print(Status)
            Status = json.dumps(Status)
            client.publish("Relay/CommIssue/" + InstrConfig['EquipmentTags']['EquipmentName'], Status )
            pass
    return(ReturnData)
