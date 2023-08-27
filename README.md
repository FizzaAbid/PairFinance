## Pair Finance Assessment Details
1. Find max temperature for each device per hour
2. Find data points for each device per hour
3. Find the total distance covered for each device per hour


Final results should be written to Mysql

## How to Run the Project?
1. cd into the folder 'Data Engineer Task' where docker-compose.yml is present
2. Open docker desktop and run 'docker compose up'
3. Connect to Data Grip/Dbeaver or any database explorer tool
4. Connect to Postgres and SQL database with credentials (username: **nonroot**, password: **nonroot**, port: **3020** for Mysql) and for Postgres (username: **postgres**, password:**password**, port: **5020**)
5. Query the tables devices, analytics table will be populated after an hour because of the sleep condition in the code.

## Code Explanation
analytics.py contains five functions, which do the following:
1. calculate_max_temperature: calculates max temperature for each device per hour
2. compute_datapoints: calculates data points for each device per hour
3. compute_distance: Find the total distance covered for each device per hour
4. data_aggregation: Main logic, which calls all three above functions and aggregate results
5. insert_results: inserts aggregated results into Mysql database in table analytics

## Results
<img width="1100" alt="aggregated_output_screenshot" src="https://github.com/FizzaAbid/PairFinance/assets/31180223/bde3fe02-e05b-4ce6-b7f6-4354a6c17eb5">



## Alternative Approaches
1. Nested dictionaries can be used for calculating data points and max temperature instead of SQL
2. Pandas or any different library could have been used or either psycogy2 functions could have been utilized.

## Future Optimization Roadmap (Customizations)
1. Test cases can be added by using library pytest
2. SQL based built in functions can be utilized
3. Since the data is increasing rapidly, this can be done on pyspark to process data on multiple cores
4. Logging can be done within this code and add exceptions where indexing is being done especially.
