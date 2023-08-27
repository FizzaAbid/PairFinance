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
last_exceution_time = 0
def prev_hour_from_unix_time(unix_time):
    time = datetime.fromtimestamp(unix_time)
    hour = time.strftime('%Y-%m-%d %H:00:00')
    hourUtc = datetime.strptime(hour, '%Y-%m-%d %H:%M:%S')
    return hourUtc

# Task 1: The maximum temperatures measured for every device per hours.
def calculate_max_temperature(last_exceution_time,current_hour_start): #alternative approach would be to use dictionaries
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
    results = psql_connection.execute(max_temp_stmt,dict(processed_time=last_exceution_time, hour_start=current_hour_start))
    return results

def compute_data_points(last_exceution_time):
    psql_metadata.reflect(bind=psql_engine, only=['devices'])
    data_points_stmt = text(
        "SELECT  distinct device_id, count(*) as data_points,"
        " extract(hour from utc_time) as hourUtc from ("
        "SELECT devices.device_id, count(*) as count_data_point, "
        " TO_TIMESTAMP(devices.time::numeric)::timestamp as utc_time FROM devices"
        " WHERE CAST(devices.time AS INTEGER) > :processed_time"
        " group by devices.device_id,devices.time"
        " ORDER BY devices.time ASC)datapoint group by 1,hourUtc"
    )
    data_points = psql_connection.execute(data_points_stmt,dict(processed_time=last_exceution_time))
    return data_points

#Total distance of device movement for every device per hours.
def compute_distance(distances,device_id,hourUtc,location,previous_location):
    if device_id not in distances:
        distances[device_id] = {}
    if hourUtc not in distances[device_id]:
        distances[device_id][hourUtc] = 0


    current_location_str = list(json.loads(location).values())
    current_location = (current_location_str[0], current_location_str[1])

    if previous_location:
        distances[device_id][hourUtc] += (geopy_distance(previous_location, current_location).km)
    previous_location = current_location
    return distances,previous_location

def data_aggregation(device_results, last_exceution_time,current_hour_start): #aggregate max_temp, data points, and distance
    distances = {}
    maximum_temp = []
    data_points=[]
    previous_location=None

    maxTemp=calculate_max_temperature(last_exceution_time,current_hour_start) #Calculating max temp of each device per hour
    datapoint=compute_data_points(last_exceution_time) #calculating data points of each device per hour

    for row in maxTemp.fetchall():
        maximum_temp.append(row._asdict())

    for row in datapoint.fetchall():
        data_points.append(row._asdict())

    for row in device_results.fetchall():
        device_id=row.device_id
        location=row.location
        hourUtc = prev_hour_from_unix_time(int(row.time))
        distances,previous_location=compute_distance(distances,device_id,hourUtc,location,previous_location)
        last_exceution_time = int(row.time)

    return maximum_temp, data_points, distances, last_exceution_time


def insert_results(max_temperatures, data_points, distances):
    key_device_id = "device_id"
    key_max_temperature = "max_temperature"
    list_device_id = []
    list_max_temperature = []
    list_hour_utc = []
    list_location_distances=[]
    list_data_points=[]

    for key,value in distances.items():
        for distance in value.items():
            list_location_distances.append(distance[1])
            list_hour_utc.append(distance[0])

    for i in range(0,len(data_points)):
        for key, value in data_points[i].items():
            if key == 'data_points':
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
            distance=list_location_distances[i]
        ))
    mysql_connection.commit()

while(1):
    psql_metadata.reflect(bind=psql_engine, only=['devices'])
    devices = psql_metadata.tables['devices']
    select_devices_stmt = text("SELECT devices.device_id, devices.temperature, "
                             "devices.location,devices.time FROM devices"
                             " WHERE CAST(devices.time AS INTEGER) > :processed_time"
                             " AND CAST(devices.time AS INTEGER) < :hour_start"
                             " ORDER BY devices.time ASC"
                             )  #fetch devices from postgres
    current_time = int(time())
    current_hour_start = current_time - (current_time % 3600) #1 hour has 3600 mins
    device_results = psql_connection.execute(select_devices_stmt,dict(processed_time=last_exceution_time, hour_start=current_hour_start))
    max_temperatures, data_points, distances, last_exceution_time =data_aggregation(device_results, last_exceution_time,current_hour_start)
    insert_results(max_temperatures, data_points, distances)
    current_time_utc = datetime.now(timezone.utc)
    next_hour = current_time_utc.replace(hour=current_time_utc.hour+1, minute=0, second=0, microsecond=0)
    sleep_duration = (next_hour - current_time_utc).seconds
    sleep(sleep_duration) #wait until next hour, comment this to reflect data in db
