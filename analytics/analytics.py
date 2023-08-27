from os import environ
from time import time, sleep
from sqlalchemy import (Column, Integer, Float, String, create_engine,insert, MetaData, text, Table, TIMESTAMP)
from sqlalchemy.exc import OperationalError
from geopy.distance import distance as geopy_distance
from datetime import datetime, timezone
import json

print('Waiting for the data generator...')
sleep(3)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10, future=True)
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10,future=True)
        psql_connection = psql_engine.connect()
        mysql_connection = mysql_engine.connect()

        # Define the table schema
        psql_metadata = MetaData()
        mysql_metadata = MetaData()

        aggregated_data = Table('aggregated_data', mysql_metadata,
                                Column('id', Integer, primary_key=True),
                                Column('device_id', String(length=50)),
                                Column('date', TIMESTAMP),
                                Column('max_temperature', Integer),
                                Column('data_points', Integer),
                                Column('distance', Float))

        aggregated_data.drop(mysql_engine, checkfirst=True)
        mysql_metadata.create_all(mysql_engine)
        break
    except OperationalError:
        sleep(0.1)

print('Connection to PostgresSQL successful.')
LastExceutionTime = 0
def prev_hour_from_unix_time(unix_time):
    time = datetime.fromtimestamp(unix_time)
    hour = time.strftime('%Y-%m-%d %H:00:00')
    hourUtc = datetime.strptime(hour, '%Y-%m-%d %H:%M:%S')
    return hourUtc

# Task 1: The maximum temperatures measured for every device per hours.
def calculate_max_temperature(LastExceutionTime,startCurrentHour): #alternative approach would be to use dictionaries
    psql_metadata.reflect(bind=psql_engine, only=['devices'])
    max_temp_stmt = text(
        "SELECT distinct device_id, max(tmp) as max_temperature,"
        " extract(hour from utc_time) as hourUtc from ("
        "SELECT devices.device_id, max(devices.temperature) as tmp, "
        " TO_TIMESTAMP(devices.time::numeric)::timestamp as utc_time FROM devices"
        " WHERE CAST(devices.time AS INTEGER) > :processed_time"
        " AND CAST(devices.time AS INTEGER) < :hour_start"
        " group by devices.device_id,devices.time"
        " ORDER BY devices.time ASC)max_tmp group by 1,hourUtc"
    )
    results = psql_connection.execute(max_temp_stmt,dict(processed_time=LastExceutionTime, hour_start=startCurrentHour))
    return results

def compute_dataPoints(LastExceutionTime):
    psql_metadata.reflect(bind=psql_engine, only=['devices'])
    dataPoints_stmt = text(
        "SELECT  distinct device_id, count(*) as dataPoints,"
        " extract(hour from utc_time) as hourUtc from ("
        "SELECT devices.device_id, count(*) as count_data_point, "
        " TO_TIMESTAMP(devices.time::numeric)::timestamp as utc_time FROM devices"
        " WHERE CAST(devices.time AS INTEGER) > :processed_time"
        " group by devices.device_id,devices.time"
        " ORDER BY devices.time ASC)datapoint group by 1,hourUtc"
    )
    datapoints = psql_connection.execute(dataPoints_stmt,dict(processed_time=LastExceutionTime))
    return datapoints

#Total distance of device movement for every device per hours.
def compute_distance(distances,device_id,hourUtc,location,previousLocation):
    if device_id not in distances:
        distances[device_id] = {}
    if hourUtc not in distances[device_id]:
        distances[device_id][hourUtc] = 0


    current_location_str = list(json.loads(location).values())
    current_location = (current_location_str[0], current_location_str[1])

    if previousLocation:
        distances[device_id][hourUtc] += (geopy_distance(previousLocation, current_location).km)
    previousLocation = current_location
    return distances,previousLocation

def data_aggregation(deviceResults, LastExceutionTime,startCurrentHour): #aggregate max_temp, data points, and distance
    distances = {}
    maxTemperatures = []
    datapoints=[]
    previousLocation=None

    maxTemp=calculate_max_temperature(LastExceutionTime,startCurrentHour) #Calculating max temp of each device per hour
    datapoint=compute_dataPoints(LastExceutionTime) #calculating data points of each device per hour

    for row in maxTemp.fetchall():
        maxTemperatures.append(row._asdict())

    for row in datapoint.fetchall():
        datapoints.append(row._asdict())

    for row in deviceResults.fetchall():
        device_id=row.device_id
        location=row.location
        hourUtc = prev_hour_from_unix_time(int(row.time))
        distances,previousLocation=compute_distance(distances,device_id,hourUtc,location,previousLocation)
        LastExceutionTime = int(row.time)

    return maxTemperatures, datapoints, distances, LastExceutionTime


def insert_results(max_temperatures, dataPoints, distances):
    print("dataPoints",dataPoints)
    key_device_id = "device_id"
    key_max_temperature = "max_temperature"
    list_device_id = []
    list_max_temperature = []
    list_hour_utc = []
    locationDistances=[]
    list_data_points=[]

    for key,value in distances.items():
        for distance in value.items():
            locationDistances.append(distance[1])
            list_hour_utc.append(distance[0])

    for i in range(0,len(dataPoints)):
        for key, value in dataPoints[i].items():
            if key == 'datapoints':
                list_data_points.append(value)


    for i in range(0,len(max_temperatures)):
        for key, value in max_temperatures[i].items():
            if key == key_device_id:
                list_device_id.append(value)
            if key == key_max_temperature:
                list_max_temperature.append(value)

        mysql_connection.execute(insert(aggregated_data).values(
            device_id=list_device_id[i],
            date=list_hour_utc[i],
            max_temperature=list_max_temperature[i],
            data_points=list_data_points[i],
            distance=locationDistances[i]
        ))
    mysql_connection.commit()

while(1):
    psql_metadata.reflect(bind=psql_engine, only=['devices'])
    devices = psql_metadata.tables['devices']
    selectDevicesStmt = text("SELECT devices.device_id, devices.temperature, "
                             "devices.location,devices.time FROM devices"
                             " WHERE CAST(devices.time AS INTEGER) > :processed_time"
                             " AND CAST(devices.time AS INTEGER) < :hour_start"
                             " ORDER BY devices.time ASC"
                             )  #fetch devices from postgres
    currentTime = int(time())
    startCurrentHour = currentTime - (currentTime % 3600) #1 hour has 3600 mins
    deviceResults = psql_connection.execute(selectDevicesStmt,dict(processed_time=LastExceutionTime, hour_start=startCurrentHour))
    max_temperatures, dataPoints, distances, LastExceutionTime =data_aggregation(deviceResults, LastExceutionTime,startCurrentHour)
    insert_results(max_temperatures, dataPoints, distances)
    currentTime_utc = datetime.now(timezone.utc)
    next_hour = currentTime_utc.replace(hour=currentTime_utc.hour+1, minute=0, second=0, microsecond=0)
    sleep_duration = (next_hour - currentTime_utc).seconds
    sleep(sleep_duration) #wait until next hour
