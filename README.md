#Pair Finance Assessment

## Pair Finance Assessment Details
1. Find max temperature for each device per hour
2. Find data points for each device per hour
3. Find the total distance covered for each device per hour

Final results should be written to Mysql

## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

## How to Run the Project?
1. cd into the folder 'Data Engineer Task' where docker-compose.yml is present
2. Open docker desktop and run 'docker compose up'
3. Connect to Data Grip/Dbeaver or any database explorer tool
4. Connect to Postgres and SQL database with credentials (username: nonroot, password: nonroot, port: 3020 for Mysql) and for Postgres (username: postgres, password:password, port: 5420)
5. Query the tables devices, analytics table will be populated after an hour because of the sleep condition in the code.

## Code Explanation
analytics.py contains five functions, which do the following:
1. 
2.
3.
4.
5.
