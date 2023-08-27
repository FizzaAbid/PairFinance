import asyncio
import json
from os import environ
from time import time, sleep
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table, Column, Integer, String, MetaData

faker = Faker()
while True:
    try:
        #environ['POSTGRESQL_CS']='postgresql+psycopg2://postgres:password@localhost:5020/main'
        #psql_engine=create_engine('postgresql+psycopg2://postgres:password@localhost:5020/main', pool_pre_ping=True, pool_size=10,future=True)
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10,future=True)
        metadata_obj = MetaData()
        devices = Table(
            'devices', metadata_obj,
            Column('device_id', String),
            Column('temperature', Integer),
            Column('location', String),
            Column('time', String),
        )
        metadata_obj.create_all(psql_engine)
        break
    except OperationalError:
        sleep(0.01)



async def store_data_point(device_id):
    ins = devices.insert()
    with psql_engine.connect() as conn:
        while True:
            conn.begin()
            data = dict(
                device_id=device_id,
                temperature=faker.random_int(10, 50),
                location=json.dumps(dict(latitude=str(faker.latitude()), longitude=str(faker.longitude()))),
                time=str(int(time()))
            )
            conn.execute(ins, data)
            conn.commit()
            print("device_id",device_id, data['time'])
            await asyncio.sleep(0.1)


loop = asyncio.get_event_loop()
asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

loop.run_forever()
#%%
