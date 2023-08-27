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
        #environ['POSTGRESQL_CS']='postgresql+psycopg2://postgres:password@localhost:5020/main'
        #environ['MYSQL_CS']='mysql+pymysql://nonroot:nonroot@localhost:3020/analytics?charset=utf8'
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
def calculate_max_temperature(last_exceution_time,current_start_hour): #alternative approach would be to use dictionaries
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
    results = psql_connection.execute(max_temp_stmt,dict(processed_time=last_exceution_time, hour_start=current_start_hour))
    return results

def compute_data_points(last_exceution_time):
    psql_metadata.reflect(bind=psql_engine, only=['devices'])
    find_datapoints_stmt = text(
        "SELECT  distinct device_id, count(*) as dataPoints,"
        " extract(hour from utc_time) as hourUtc from ("
        "SELECT devices.device_id, count(*) as count_data_point, "
        " TO_TIMESTAMP(devices.time::numeric)::timestamp as utc_time FROM devices"
        " WHERE CAST(devices.time AS INTEGER) > :processed_time"
        " group by devices.device_id,devices.time"
        " ORDER BY devices.time ASC)datapoint group by 1,hourUtc"
    )
    datapoints = psql_connection.execute(find_datapoints_stmt,dict(processed_time=last_exceution_time))
    return datapoints

#Total distance of device movement for every device per hours.
def compute_distance(distances,device_id,hourUtc,location,prior_location):
    if device_id not in distances:
        distances[device_id] = {}
    if hourUtc not in distances[device_id]:
        distances[device_id][hourUtc] = 0


    current_location_str = list(json.loads(location).values())
    current_location = (current_location_str[0], current_location_str[1])

    if prior_location:
        distances[device_id][hourUtc] += (geopy_distance(prior_location, current_location).km)
    prior_location = current_location
    return distances,prior_location

def data_aggregation(device_query_results, last_exceution_time,current_start_hour): #aggregate max_temp, data points, and distance
    distances = {}
    maximum_temp = []
    datapoints=[]
    prior_location=None

    maxTemp=calculate_max_temperature(last_exceution_time,current_start_hour) #Calculating max temp of each device per hour
    datapoint=compute_data_points(last_exceution_time) #calculating data points of each device per hour

    for max_temp in maxTemp.fetchall():
        maximum_temp.append(max_temp._asdict())

    for device_count in datapoint.fetchall():
        datapoints.append(device_count._asdict())

    for row in device_query_results.fetchall():
        device_id=row.device_id
        location=row.location
        hourUtc = prev_hour_from_unix_time(int(row.time))
        distances,prior_location=compute_distance(distances,device_id,hourUtc,location,prior_location)
        last_exceution_time = int(row.time)

    return maximum_temp, datapoints, distances, last_exceution_time


def insert_results(max_temperatures, data_points, distances):
    print("data_points",data_points)
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

    for data_point in range(0,len(data_points)):
        for identifier, record in data_points[data_point].items():
            if identifier == 'datapoints':
                list_data_points.append(record)


    for counter in range(0,len(max_temperatures)):
        for identifier, record in max_temperatures[counter].items():
            if identifier == key_device_id:
                list_device_id.append(record)
            if identifier == key_max_temperature:
                list_max_temperature.append(record)

        mysql_connection.execute(insert(aggregated_data).values(
            device_id=list_device_id[counter],
            date=list_hour_utc[counter],
            max_temperature=list_max_temperature[counter],
            data_points=list_data_points[counter],
            distance=locationDistances[counter]
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
                             )  #fetch devices data from postgres table device
    current_time = int(time())
    current_start_hour = current_time - (current_time % 3600) #1 hour has 3600 mins
    device_query_results = psql_connection.execute(selectDevicesStmt,dict(processed_time=last_exceution_time, hour_start=current_start_hour))
    max_temperatures, data_points, distances, last_exceution_time =data_aggregation(device_query_results, last_exceution_time,current_start_hour)
    insert_results(max_temperatures, data_points, distances)
    current_time_utc = datetime.now(timezone.utc)
    next_hour = current_time_utc.replace(hour=current_time_utc.hour+1, minute=0, second=0, microsecond=0)
    sleep_duration = (next_hour - current_time_utc).seconds
    sleep(sleep_duration) #wait until next hour
