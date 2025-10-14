# Solid Oxide Electrolysis Cell (SOEC) Control System

## Overview
This repository implements a **Node-RED–driven control for high temperature electrolysis systems** ** platform. The system coordinates communication between **field instruments (instrument servers)**, **process controllers**, and a **PostgreSQL database** for logging.  

The system architecture is modular:
- **Node-RED flows** orchestrate control logic and sequencing.
- **Python instrument servers** provide protocol-specific interfaces (e.g., MODBUS, MQTT, SCPI).
- **JSON configuration files** define equipment configuration, alarms, system states, and control personalities.
- **Database logger** archives high- and medium-speed data streams for analysis and reporting.

---

## System Architecture

### 1. Node-RED Flows
- Serve as the **top-level control orchestrator**.
- Dynamically launch and monitor Python instrument servers.
- Derive the local **Python script directory** from flow configuration and call corresponding servers.
- Handle **state transitions** (start-up, steady-state, shutdown) and map them to personality profiles.

### 2. Instrument Servers
Each instrument (e.g., VFDs, power converters, flow meters,) runs under a Python instrument server:

- **`MQTT_Instrument_Server.py`**  
  - Launches a device-specific server from a JSON config (e.g., `instrument.json`).  
  - Subscribes to `Relay/Request` topics from Node-RED.  
  - Executes read/write operations via `MQTT_Instrument_Lib.py`.  
  - Publishes measurements to `Relay/Data/<instrument>`.

- **`MQTT_Instrument_Lib.py`**  
  - Provides common routines for MODBUS, MQTT, SCPI, OptoMMP, and Ping communications.  
  - Implements request parsing, data type conversion, Modbus payload builders, and channel DataFrame generation.

- **Instrument Configs**  
  - Example: `instrument.json` defines IP, port, byte order, Modbus registers, scaling factors, and monitoring tags.  
  - Equipment types are registered in `System-Config.json`.

### 3. Control Personalities
The **“personality” JSONs** define operating logic tailored actual system:

- **`System_Personality.json`**: Root profile mapping system states, alarms, control states, and database linkage.  
- **`System_Personality_AlarmStates.json`**: Encodes alarm logic (e.g., emergency stop, loss of steam, low gas flow, ventilation loss).  
- **`System_Personality_ControlStates.json`**: Defines automatic/manual control rules, shutdown logic, and transitions (e.g., Heat-Up, Hot Standby, Ready-to-Start).  
- **`System_Personality_SystemStates.json`**: Tracks thermal and flow system states (heaters, stack sets).

### 4. Database Logger
- **`MQTT_PostgreSQL_Server.py`** subscribes to `Relay/Data/#` and writes data to PostgreSQL.  
- Configuration from **`System_Database.json`**:  
  - Database: `system` at `postgresql://postgres:username@"IP_Adress":5432/"data_base_name"`.  
  - Default table: `database_table`.  
- Data are cleaned (boolean and NULL conversion, timestamp filtering) before insertion.  
- Implements efficient writes via Pandas `to_sql` with PostgreSQL `COPY`.

---

## Instrument Configuration File Structure

Each instrument (e.g., flow meter, VFD, power converters) is represented by a dedicated **JSON configuration file**. These files allow the control framework (Node-RED + Python instrument servers) to automatically discover, connect, and interact with devices without changing source code.

### 1. EquipmentTags Section
This section defines the **metadata and connection details** of the instrument:

```json
"EquipmentTags": {
    "EquipmentName": "VFD_1",
    "EquipmentType": "Motor Drives",
    "ByteOrder": "Big",
    "WordOrder": "Big",
    "Communication": "ModbusTcpGeneric",
    "PortNum": 502,
    "Connection": {
        "IP": "192.168.0.155",
        "Unit": "1"
    },
    "EquipmentLocation": "ESL D100",
    "Emails": "micah.casteel@inl.gov",
    "Reference": "Device manual or link"
}
```

- **EquipmentName** → Unique identifier used across MQTT topics and database logs.  
- **EquipmentType** → Category (e.g., Motor Drive, Flow Meter, Power Converter).  
- **Communication** → Protocol driver (e.g., `ModbusTcpGeneric`, `MQTT`, `SCPI`, `Opto`).  
- **Connection** → IP address, unit ID, or serial/VISA parameters depending on protocol.  
- **ByteOrder/WordOrder** → Endianness settings for Modbus.  
- **Reference/Emails** → Documentation and responsible engineer.

### 2. Channels Section
The **channels** map each control/measurement point on the instrument into a structured format:

```json
"Channels": {
    "RUN_STOP": {
        "Active": true,
        "Type": "HRegister",
        "Registers": 1,
        "DataType": "bool",
        "ReadWrite": "RW",
        "IOPoint": 8192,
        "Scalar": 1,
        "Offset": 0,
        "Decimal": 1,
        "Units": "NA",
        "Tags": ["VFD", "Monitor", "MediumSpeed"]
    },
    "FREQUENCY_SETPT": {
        "Active": true,
        "Type": "HRegister",
        "Registers": 1,
        "DataType": "int16",
        "ReadWrite": "RW",
        "IOPoint": 8193,
        "Scalar": 0.01,
        "Units": "Hz",
        "Tags": ["VFD", "Monitor", "Control", "LowSpeed"]
    }
}
```

Each channel includes:
- **Type** → Register type (Holding, Input, etc.) or MQTT topic.  
- **DataType** → e.g., `float32`, `int16`, `bool`.  
- **ReadWrite** → `R`, `W`, or `RW` depending on device capability.  
- **IOPoint** → Register address or topic ID.  
- **Scalar/Offset/Decimal** → Conversion factors for engineering units.  
- **Units** → Measurement units (Hz, A, V, psia, SLPM, etc.).  
- **Tags** → Used to organize signals into *monitoring*, *control*, or *sampling speed categories* (`HighSpeed`, `MediumSpeed`, `LowSpeed`).

### Example Usage
- `Alicat_FT_203_N2.json` defines channels for **pressure**, **temperature**, **mass flow**, and **volumetric flow** with Modbus addresses and scaling.  
- `VFD_1.json` defines channels for **run/stop control**, **frequency setpoint**, **readbacks**, and **fault monitoring**.  

By keeping this schema consistent, **new instruments can be added** simply by creating a JSON file with the same format. Node-RED flows and Python servers automatically integrate the new device into the control and logging pipeline.

---

## Communication Protocols
- **MODBUS TCP/RTU**: Power supplies, flow controllers.  
- **MQTT**: Node-RED ↔ instrument servers.  
- **SCPI**: Precision instruments (e.g., NHR loads).  
- **OptoMMP**: Opto22 field controllers.  
- **Ping**: Device connectivity checks.

---

## Data Flow
1. Node-RED issues a control command → MQTT request.  
2. Instrument server receives the request → translates to device protocol.  
3. Device response is formatted → published on `Relay/Data/<device>`.  
4. Database logger aggregates all `Relay/Data/#` topics → PostgreSQL.  
5. System state engines (AlarmStates, ControlStates, SystemStates) evaluate live telemetry and issue new commands.  

---

## Deployment Instructions
1. **Install Dependencies**  
   - Python ≥ 3.9  
   - Libraries: `paho-mqtt`, `sqlalchemy`, `pandas`, `numpy`, `pymodbus`, `pyvisa`, `optommp`  
   - PostgreSQL server  

2. **Set Up Database**  
   - Load schema in `oxeon_rsoc`.  
   - Ensure default table `default_oxeon` exists.

3. **Deploy Node-RED Flows**  
   - Import provided `.json` flow into Node-RED editor.  
   - Update paths to Python scripts if required.

4. **Start Instrument Servers**  
   ```bash
   python3 MQTT_Instrument_Server.py instrument.json
   ...
   ```

5. **Run Database Logger**  
   ```bash
   python3 MQTT_PostgreSQL_Server.py System_Database.json
   ```

6. **Monitor Node-RED Dashboard** for alarms, state transitions, and process values.

---

## Key Topics
- `Relay/Request/<device>` → Node-RED issues a read/write request.  
- `Relay/Data/<device>` → Device publishes live data.  
- `Relay/ServerIssue/<device>` → Fault/communication errors.  
- `Relay/PoisonPill/<device>` → Triggers controlled server shutdown.  

---

## Safety Considerations
- **Emergency Stop (`E_Stop`)** is hard-coded as highest-priority alarm.  
- Automatic controls enforce **shutdown on hydrogen fire**, **loss of ventilation**, **loss of steam**, or **stack overtemperature**.  
- Manual and automatic shutdown paths are distinct in ControlStates.

---

## Directory Structure
```
├── NodeRED_Flows/
│   └── <flow>.json
├── Config/
│   ├── System-Config.json
│   ├── System_Personality.json
│   ├── System_Personality_AlarmStates.json
│   ├── System_Personality_ControlStates.json
│   ├── System_Personality_SystemStates.json
│   ├── System_Database.json
│   └── Instrument configs (instrument1.json, instrument2.json, etc.)
├── Python_Servers/
│   ├── MQTT_Instrument_Lib.py
│   ├── MQTT_Instrument_Server.py
│   ├── MQTT_PostgreSQL_Server.py
└── Database/
    └── PostgreSQL tables, logs
```

---

## Authors
- System designed at **Idaho National Laboratory (INL)**.  
- Key contributors: Micah Casteel.  
