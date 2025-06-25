# Workload Generator for MongoDB

Workload Generator for MongoDB  was designed to help MongoDB users effortlessly generate data and simulate workloads for both sharded and non-sharded clusters. The generated workloads include standard CRUD operations, reflecting real-world usage patterns of a MongoDB environment.

Additionally, the tool supports the creation of realistic workloads by incorporating all CRUD operations through a set of queries that simulate common usage scenarios. Users can also define custom queries to run against collections created by the tool, further enhancing its flexibility and applicability.

While the tool provides extensive configuration capabilities, it requires minimal setup — only basic connection details are needed. It is user-friendly, does not require compilation, and offers a high level of flexibility.

The application was developed and tested using Python 3. As such, Python 3 is recommended for optimal compatibility. If upgrading is not feasible, modifications to the scripts may be necessary to support older Python versions.

The tool is optimized to leverage as many available CPU cores as you wish and supports configuration of an arbitrary number of threads among, enabling high parallelism and making it ideal for generating large-scale workloads and conducting effective stress tests on MongoDB clusters.


## Configuration

The tool consists of 9 files: 

* [mongodbCreds.py](mongodbCreds.py)
* [mongodbLoadQueries.py](mongodbLoadQueries.py) 
* [mongodbWorkload.py](mongodbWorkload.py)
* [app.py](app.py)
* [args.py](args.py)
* [collections/airline.json](collections/airline.json)
* [customProvider.py](customProvider.py)
* [logger.py](logger.py)
* [mongo_client.py](mongo_client.py)

The only required configuration is in [mongodbCreds.py](mongodbCreds.py), where you define the connection details for your  cluster. The file is self-explanatory and includes examples to help you set up your environment correctly. You may also extend this file to include additional parameters as needed.

All other files do not require any sort of configuration, however, you can provide your own collection definition JSON file and/or add additional query parameters to [mongodbLoadQueries.py](mongodbLoadQueries.py), but this is not a requirement. 

#### Pre-reqs

Workload Generator for MongoDB relies on a few additional Python libraries that are not installed by default. To ensure proper functionality, please install the following dependencies:

- [faker](https://pypi.org/project/Faker/) – Used to generate random fictional data, ideal for bootstrapping databases, stress testing, and creating randomized datasets.
- [joblib](https://joblib.readthedocs.io/en/stable/) – Enables parallel execution of tasks by leveraging multiple CPU cores.
- [pymongo](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/) – Library for interacting with MongoDB using Python.

To install these libraries, run the following command:

```
pip3 install faker joblib pymongo
```

## Functionality

By default, the workload runs for `60 seconds`, creating `4 threads` and using 1 CPU core. The workload creates a database called `airline` with a collection named `flights` and populates it with documents containing data similar to the sample shown below. (Note: Sharding is enabled by default if you use the provided [collections/airline.json](collections/airline.json). You can change this behavior by editing the JSON and removing the `shardConfig` section or providing a custom JSON -- more on this below).

```
[direct: mongos] airlines> db.flights.findOne()
{
    _id: ObjectId('685b235e686cbe204c4d531b'),
    flight_id: 8861,
    first_name: 'Ashley',
    last_name: 'Mitchell',
    email: 'pwilliams@example.org',
    flight_date: ISODate('1992-02-14T18:26:33.701Z'),
    origin: 'North Joshuafort',
    destination: 'West Cole',
    flight_code: 'FLT-639',
    duration_minutes: 2588,
    gate: 'U2',
    equipment: {
      plane_type: 'ERJ-145',
      total_seats: 50,
      amenities: [ 'WiFi', 'TV', 'Power outlets' ]
    },
    seats_available: 11,
    passengers: [
      {
        passenger_id: 1,
        name: 'Scott Phillips',
        seat_number: '2C',
        ticket_number: 'YHQ4UG8M4F'
      },
      {
        passenger_id: 2,
        name: 'Christina Lewis',
        seat_number: '11F',
        ticket_number: '4YV9MW3RQT'
      },
      {
        passenger_id: 3,
        name: 'Alexis Roberts',
        seat_number: '15E',
        ticket_number: 'BMSWKUCTM9'
      },
.... removed for brevity .....
```

The default query distribution ratio is as follows:

* 60% SELECT queries
* 20% UPDATE queries
* 10% INSERT queries
* 10% DELETE queries

This default distribution provides a balanced and meaningful baseline for performance testing and stress simulations. However, users are encouraged to adjust these ratios to better align with their specific use cases and workload characteristics.

During execution, the tool generates a real-time report every 5 seconds, showing the average number of queries executed across all utilized CPU cores, along with a detailed breakdown by operation type. At the end of the run, a final summary report is produced, providing statistics on overall workload performance and collection activity ([see sample output below](#Basic-Usage)).


## Usage

The workload is highly configurable at runtime, enabling you to fine-tune its behavior by specifying various parameters during execution. These parameters allow you to control key aspects of the workload, including workload duration, number of threads, query distribution, and more.

By default, the tool generates a predefined and preconfigured database and collection. However, you can optionally provide a custom collection definition file (see details below). You can also configure additional queries to the already existing ones (also detailed below). When used in combination, these options offer a high degree of flexibility, allowing you to adapt the application's behavior to meet your specific requirements and simulate operations that more accurately represent your target workload.

#### Getting help

You can obtain help and a list of all available parameters by running `./mongodbWorkload.py --help` 

```
./mongodbWorkload.py --help
usage: mongodbWorkload.py [-h] [--collections COLLECTIONS] [--collection_definition COLLECTION_DEFINITION] [--recreate] [--runtime RUNTIME] [--batch_size BATCH_SIZE] [--threads THREADS]  [--skip_update] [--skip_delete] [--skip_insert] [--skip_select] [--insert_ratio INSERT_RATIO] [--update_ratio UPDATE_RATIO] [--delete_ratio DELETE_RATIO][--select_ratio SELECT_RATIO] [--report_interval REPORT_INTERVAL] [--cpu_ops] [--optimized] [--cpu CPU] [--log [LOG]]

MongoDB Workload Generator

options:
  -h, --help                  show this help message and exit
  --collections COLLECTIONS   How many collections to create (default 1).
  --collection_definition     COLLECTION_DEFINITION Name (if placed in collections folder) or full path + name of JSON file with your collection definition (default collections/airline.json).
  --recreate            Recreate the collection before running the test.
  --runtime RUNTIME     Duration of the load test, specify in seconds (e.g., 60s) or minutes (e.g., 5m) (default 60s).
  --batch_size BATCH_SIZE Number of documents per batch insert (default 10).
  --threads THREADS     Number of threads for simultaneous operations (default 4).
  --skip_update         Skip update operations.
  --skip_delete         Skip delete operations.
  --skip_insert         Skip insert operations.
  --skip_select         Skip select operations.
  --insert_ratio INSERT_RATIO  Percentage of insert operations (default 10).
  --update_ratio UPDATE_RATIO  Percentage of update operations (default 20).
  --delete_ratio DELETE_RATIO  Percentage of delete operations (default 10).
  --select_ratio SELECT_RATIO  Percentage of select operations (default 60).
  --report_interval REPORT_INTERVAL  Interval (in seconds) between workload stats output (default 5s).
  --cpu_ops             Workload AVG OPS per CPU.
  --optimized           Run optimized workload only.
  --cpu CPU             Number of CPUs to launch multiple instances in parallel (default 1).
  --log [LOG]           Log filename and path (e.g., /tmp/report.log).
```                        

#### Basic Usage 

Once you have configured the settings to match your environment, you can run the workload without specifying any parameters. This will utilize the default settings, providing a great way to familiarize yourself with the tool and review its output. The default setting is not optimized, this means it will randomly choose between optimized and innefective queries, so your performance may vary.

```
./mongodbWorkload.py
2025-06-24 17:25:47 - INFO - Collection flights created in DB 'airline'
2025-06-24 17:25:48 - INFO - Sharding configured for airline.flights with key {'flight_id': 'hashed'}
2025-06-24 17:25:48 - INFO - Creating index on flights with keys=[('flight_id', 1), ('equipment.plane_type', 1)], options={}
2025-06-24 17:25:48 - INFO - Creating index on flights with keys=[('flight_id', 1), ('seats_available', 1)], options={}
2025-06-24 17:25:48 - INFO - Creating index on flights with keys=[('flight_id', 1), ('duration_minutes', 1), ('seats_available', 1)], options={}
2025-06-24 17:25:48 - INFO - Creating index on flights with keys=[('equipment.plane_type', 1)], options={}
2025-06-24 17:25:48 - INFO -

Duration: 60 seconds
CPUs: 1
Threads: (Per CPU: 4 | Total: 4)
Collections: 1
Configure Sharding: True
Insert batch size: 10
Optimized workload: False
Workload ratio: SELECTS: 60% | INSERTS: 10% | UPDATES: 20% | DELETES: 10%
Report frequency: 5 seconds
Report logfile: None

===================================================================================================================
                                                 Workload Started
===================================================================================================================

2025-06-24 17:25:55 - INFO - AVG Operations last 5s (1 CPUs): 37.20 (SELECTS: 23.20, INSERTS: 3.00, UPDATES: 7.40, DELETES: 3.60)
2025-06-24 17:26:00 - INFO - AVG Operations last 5s (1 CPUs): 38.40 (SELECTS: 23.20, INSERTS: 3.20, UPDATES: 7.40, DELETES: 4.60)
2025-06-24 17:26:05 - INFO - AVG Operations last 5s (1 CPUs): 36.60 (SELECTS: 23.20, INSERTS: 5.00, UPDATES: 5.40, DELETES: 3.00)
2025-06-24 17:26:10 - INFO - AVG Operations last 5s (1 CPUs): 36.40 (SELECTS: 22.00, INSERTS: 3.60, UPDATES: 8.00, DELETES: 2.80)
2025-06-24 17:26:15 - INFO - AVG Operations last 5s (1 CPUs): 36.40 (SELECTS: 22.80, INSERTS: 3.80, UPDATES: 6.60, DELETES: 3.20)
2025-06-24 17:26:20 - INFO - AVG Operations last 5s (1 CPUs): 34.40 (SELECTS: 18.20, INSERTS: 4.20, UPDATES: 7.80, DELETES: 4.20)
2025-06-24 17:26:25 - INFO - AVG Operations last 5s (1 CPUs): 31.40 (SELECTS: 18.40, INSERTS: 4.60, UPDATES: 6.20, DELETES: 2.20)
2025-06-24 17:26:30 - INFO - AVG Operations last 5s (1 CPUs): 33.60 (SELECTS: 20.80, INSERTS: 1.60, UPDATES: 7.20, DELETES: 4.00)
2025-06-24 17:26:35 - INFO - AVG Operations last 5s (1 CPUs): 27.80 (SELECTS: 16.00, INSERTS: 2.80, UPDATES: 6.40, DELETES: 2.60)
2025-06-24 17:26:40 - INFO - AVG Operations last 5s (1 CPUs): 32.60 (SELECTS: 20.40, INSERTS: 2.80, UPDATES: 5.20, DELETES: 4.20)
2025-06-24 17:26:45 - INFO - AVG Operations last 5s (1 CPUs): 30.20 (SELECTS: 18.60, INSERTS: 2.40, UPDATES: 6.60, DELETES: 2.60)
2025-06-24 17:26:50 - INFO - AVG Operations last 5s (1 CPUs): 29.40 (SELECTS: 18.40, INSERTS: 3.60, UPDATES: 6.00, DELETES: 1.40)
2025-06-24 17:26:55 - INFO - AVG Operations last 5s (1 CPUs): 0.40 (SELECTS: 0.00, INSERTS: 0.00, UPDATES: 0.40, DELETES: 0.00)
2025-06-24 17:27:00 - INFO - AVG Operations last 5s (1 CPUs): 0.40 (SELECTS: 0.00, INSERTS: 0.00, UPDATES: 0.40, DELETES: 0.00)
2025-06-24 17:27:00 - INFO -
===================================================================================================================
                                                Workload Finished
===================================================================================================================

2025-06-24 17:27:01 - INFO -
================================================================================
|                               Collection Stats                               |
================================================================================
|            Name         |     Sharded      |      Size      |    Documents   |
================================================================================
|          flights        |       True       |    0.95 MB     |      1989      |
================================================================================

2025-06-24 17:27:01 - INFO -
===================================================================================================================
                                        Workload Stats (All CPUs Combined)
===================================================================================================================
Workload Runtime: 1.21 minutes
CPUs Used: 1
Total Operations: 2024 (SELECT: 1226, INSERT: 203, UPDATE: 403, DELETE: 192)
AVG QPS: 27.89 (SELECTS: 16.89, INSERTS: 2.80, UPDATES: 5.55, DELETES: 2.65)
Documents Inserted: 2030, Matching Documents Selected: 1420, Documents Updated: 403, Documents Deleted: 41
===================================================================================================================
```

#### Advanced Usage

You have a wide range of options available, and the parameters are neither exclusive nor mutually dependent, allowing you to use as many or as few as needed.
The parameters available and their use cases are shown below:

1. collections -- Number of collections

  - The workload will create multiple collections when you specify `--collections` with the desired number of collections, for example: `--collections 5`. Each collection will have its index count appended to its name, such as `flights_1`, `flights_2`, `flights_3`, and so on. You can create additional collections even after the initial workload has been run. For example, if you run the first workload with `--collections 5` and then run a new workload with `--collections 6`, a new collection will be created and the workload will run against all 6 of them.


2. collection_definition -- Collection structure

  - You can provide your own custom collection definition file in JSON format following the example file provided, found in [collections/airline.json](collections/airline.json) (this is the collection definition used by default). 
  
  - You can create your own JSON file and place it in the `collections` folder or anywhere else in your file system. If you place the JSON file in the `collections` folder, all you need to do is pass the file name, e.g: `--collection_definition my_custom_definition.json`. However, if you store the JSON in a different folder, then you have to provide the entire path, e.g.: `--collection_definition /tmp/my_custom_definition.json`.

  - In addition to the standard and self explained parameters in the provided JSON file, you can also specify an option called "provider". This is the faker provider you use  to generate that specific random datatype. I have created custom [faker providers](https://faker.readthedocs.io/en/master/#providers) that are used by [collections/airline.json](collections/airline.json), their definition is available in [customProvider.py](./customProvider.py). Feel free to look at those and [create new ones if you'd like](https://faker.readthedocs.io/en/master/#how-to-create-a-provider) or use any of the many [faker providers](https://faker.readthedocs.io/en/master/#providers) available if you would like to further extend the tools functionality.

  - The collection definition file is where you configure the following:
    - database name
    - collection name
    - fields
    - sharding
    - indexes

3. recreate -- Recreating collections

  - If you want to start from scratch you can pass `--recreate` and this will drop and recreate everything for you based on the parameters you provide. 

4. optimized -- Optimized Workload
  
  - By default, this setting is disabled, allowing the workload to run all queries—including both optimized and inefficient ones. To execute a more performance-focused workload, use the `--optimized` flag when running the tool.

    When enabled, this mode ensures that queries utilize the primary key and appropriate indexes, instead of using random fields that may or may not have indexes or other less efficient patterns. Using this option significantly enhances the workload's performance and efficiency.

    Additionally, enabling this flag restricts find workloads to aggregate queries only, minimizing the overhead of transferring large volumes of data from the database to the client.

5. runtime -- How long to run the workload

  - You can specify how long to run the workload. This is done in secods or minutes, e.g: `--runtime 5m`

6. batch_size -- Batch size

  - The workload performs its inserts in batches of 10 documents. You can change this behavior with a new batch size, e.g: `--batch_size 20`

7. threads -- Number of threads

  - By default, the workload tool uses 4 threads. You can adjust this by specifying a different thread count using the --threads parameter (e.g., `--threads 12`).

    This setting controls how many threads the client—i.e., the workload tool—will start. Increasing the number of threads allows for more concurrent operations, thereby generating greater load on the database server.

    Note: This setting affects only the client (the workload tool) and does not modify any server-side configurations.

8. Skipping certain workloads

  - You can configure the tool to skip certain workloads by using any of the following (these can be used in combination if needed):
    - `--skip_update`         
    - `--skip_delete`         
    - `--skip_insert`        
    - `--skip_select`   

9. Query ratio

  - You can configure the default query ratio to suit your needs. For example, to run 90% SELECT statements instead of the default, use: `--select_ratio 90`. The workload will automatically distribute the remaining 10% across the other query types according to the original query ratio. The available options with example settings are:

    - `--insert_ratio 20`
    - `--update_ratio 30`
    - `--delete_ratio 15`
    - `--select_ratio 35`

10. CPUs

  - This setting determines how many CPU cores are used by the workload on the client machine, directly affecting the amount of load generated against the backend MongoDB cluster. 
    
    By default, the workload runs using a single CPU core (based on the local CPU count). Naturally, increasing the number of CPUs allows the tool to generate more concurrent operations, resulting in higher load on the database server.
    
    You can specify the number of CPUs to use via the --cpu parameter (e.g., `--cpu 10`). If the specified number exceeds the available CPU cores on the system, the workload will automatically adjust to use the maximum number of available cores.
    
    When used in combination with the thread configuration, this setting enables high levels of parallelism and can significantly increase the load on the MongoDB cluster.
  
    Note: This setting affects only the client (the workload tool) and does not modify server-side configurations.

11. Report interval (seconds)

  - You can configure the report interval from the default 5s, e.g: `--report_interval 1`

12. Record the workload report to a log

  - You can configure the workload to log its output to a file. While the workload will continue to stream its output to the terminal, a log file will also be created for additional analysis, should you choose to enable this option. e.g: `--log /tmp/report.log`

13. Report per CPU stats
  - By default the real-time workload report outputs the stats for all CPUs combined however, you can change this behavior by providing `--cpu_ops`. This will change the default behavior of the real-time reporting to display the AVG OPS per CPU, instead of AVG OPS for all CPUs combined.    

14. Configure custom queries

  - You can customize the workload by adding or removing queries in the [mongodbLoadQueries.py](mongodbLoadQueries.py) file to suit your specific requirements. The file includes a mix of optimized and intentionally inefficient queries to help simulate various workloads.

    During execution, the tool randomly selects between aggregation queries (e.g., `collection.count_documents(query)`) and select queries (e.g., `collection.find(query, projection)`), choosing from both optimized and inefficient options. If you prefer to use only optimized queries, you can enable the `--optimize` flag, which excludes inefficient queries from the workload.

    Note: The query definitions in this file are structured as templates, enabling the tool to dynamically execute queries against any custom collection—regardless of its definition. If you choose to modify this file, ensure that you follow the same structure to maintain compatibility with the workload tool.

15. Combine sharded and non-sharded collections in the same workload

  - You can create both sharded and non-sharded collections by following this workflow:
    1. Create a sharded workload by editing the JSON file accordingly and configuring the `shardConfig` parameter (see provided JSON for exmaple)
    2. Create another workload without configuring sharding in the JSON file (remove the `shardConfig` parameter), but increase the number of collections using `--collections` option (make sure the number specified is higher than the current number of collections). For example, if you have 1 collection, you can use `--collections 2` and this would create 1 new collection that's not sharded.

    The above workflow will utilize the existing sharded collections while creating new collections as non-sharded, since the sharding configuration was not provided, but the number of collections was increased with `--collections`.


