## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

## How to Process Data?
1. cd into the folder 'Data Engineer Task'
2. Open docker desktop and run 'docker compose up'
3. Connect to Data Grip/Dbeaver or any database explorer tool
4. Connect to Postgres and SQL database with credentials mentioned in docker-compose.yml
5. Query the tables devices, analytics table will be populated after an hour because of the sleep condition in the code.

