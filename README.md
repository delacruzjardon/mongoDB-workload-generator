# Workload Generator for MongoDB

Workload Generator for MongoDB  was designed to help MongoDB users effortlessly generate data and simulate workloads for both sharded and non-sharded clusters. The generated workloads include standard CRUD operations, reflecting real-world usage patterns of a MongoDB environment.

Additionally, the tool supports the creation of realistic workloads by incorporating all CRUD operations through a set of queries that simulate common usage scenarios. Users can also define custom queries to run against collections created by the tool, further enhancing its flexibility and applicability.

While the tool provides extensive configuration capabilities, it requires minimal setup — only basic connection details are needed. It is user-friendly, does not require compilation, and offers a high level of flexibility.

The application was developed and tested using Python 3. As such, Python 3 is recommended for optimal compatibility. If upgrading is not feasible, modifications to the scripts may be necessary to support older Python versions.

The tool is optimized to utilize as many CPU cores as desired on the host system and supports the configuration of an arbitrary number of threads. The number of CPUs and threads refers to the available cores and threads on the source host running the workload generator. The more resources available, the greater the potential load that can be generated against the destination server. This enables high parallelism, making the tool ideal for generating large-scale workloads and conducting effective stress tests on MongoDB clusters.

#### Pre-reqs

Workload Generator for MongoDB relies on a few additional Python libraries that are not installed by default. To ensure proper functionality, please install the following dependencies:

- [faker](https://pypi.org/project/Faker/) – Used to generate random fictional data, ideal for bootstrapping databases, stress testing, and creating randomized datasets.
- [joblib](https://joblib.readthedocs.io/en/stable/) – Enables parallel execution of tasks by leveraging multiple CPU cores.
- [pymongo](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/) – Library for interacting with MongoDB using Python.

To install these libraries, run the following command:

```
pip3 install faker joblib pymongo
```

## Configuration

The tool consists of 9 main files 

* [mongodbCreds.py](mongodbCreds.py) 
* [mongodbLoadQueries.py](mongodbLoadQueries.py)  
* [mongodbWorkload.py](mongodbWorkload.py) 
* [app.py](app.py)
* [args.py](args.py)
* [customProvider.py](customProvider.py)
* [custom_query_executor.py](custom_query_executor.py)
* [logger.py](logger.py)
* [mongo_client.py](mongo_client.py)


2 sample collections and 2 query templates have also been provided, they each have their own dedicated folders:

[collections](collections) -- Collection Definitions

* [collections/airline.json](collections/airline.json)
* [collections/rental.json](collections/rental.json)

[queries](queries) -- Query Definitions:

* [queries/airline.json](queries/airline.json)
* [queries/rental.json](queries/rental.json)

The only configuration required in order to be able to start using the workload generator is in [mongodbCreds.py](mongodbCreds.py), where you define the connection details for your  cluster. The file is self-explanatory and includes examples to help you set up your environment correctly. You may also extend this file to include additional parameters as needed.

All other files do not require any sort of configuration, the tool has a pre-definined set of queries that are randomly generated (these are found in [mongodbLoadQueries.py](mongodbLoadQueries.py)) which you can extend if you wish to do so. 

## How to run

./mongodbWorkload.py

## Functionality

By default, the workload runs for 60 seconds, utilizing 4 threads and a single CPU core. It scans the collections directory for JSON files (i.e., collection definitions) and creates the corresponding databases and collections accordingly.

The tool includes two sample collection definition files. When executed with the default settings, it creates two databases—airline and rental—each containing one collection: flights and cars, respectively. These collections are populated with sample documents similar to the example shown below.

Query generation is handled automatically using templates defined in [./mongodbLoadQueries.py](mongodbLoadQueries.py). During workload execution, the tool randomly selects from these templates to simulate a full range of CRUD operations.

Note: Sharding is enabled by default when using the sample collection definitions located in the collections directory. To disable this behavior, either remove the shardConfig section from the JSON files or supply custom collection definitions (see below for details).


`airline.flights`

```
[direct: mongos] airline> db.flights.findOne()
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

`rental.cars`

```
[direct: mongos] rental> db.cars.findOne()
{
  _id: ObjectId('685da56886e3e52b504a6e90'),
  rental_id: 5320,
  first_name: 'Hannah',
  last_name: 'Foster',
  email: 'paulamccann@example.org',
  rental_info: {
    rental_date: ISODate('2025-06-18T03:25:10.048Z'),
    return_date: ISODate('2025-06-27T03:25:10.048Z'),
    pickup_location: 'Lake Mary',
    drop_off_location: 'West Larry'
  },
  car_type: 'Pickup',
  license_plate: 'WQI-6371',
  price_usd: 4004.63,
  options: {
    gps: false,
    child_seat: true,
    extra_driver: false,
    insurance: 'basic'
  },
  drivers: [
    {
      driver_id: 1,
      name: 'Pamela Burgess',
      age: 67,
      license_number: 'MUJ5PLWCT8',
      license_state: 'ID'
    },
    {
      driver_id: 2,
      name: 'Francisco Chan',
      age: 29,
      license_number: 'GOEQOKRMVH',
      license_state: 'HI'
    }
  ]
}
```

The default query distribution ratio is as follows:

* 60% SELECT queries
* 20% UPDATE queries
* 10% INSERT queries
* 10% DELETE queries

This default distribution provides a balanced and meaningful baseline for performance testing and stress simulations. However, users are encouraged to adjust these ratios to better align with their specific use cases and workload characteristics.

During execution, the tool generates a real-time report every 5 seconds, showing the average number of queries executed across all utilized CPU cores, along with a detailed breakdown by operation type. At the end of the run, a final summary report is produced, providing statistics on overall workload performance and collection activity ([see sample output below](#Basic-Usage)).


## Customization

Workload generator comes pre-configured with 2 collections and 2 query definitions:

Collections:

* [collections/airline.json](collections/airline.json)
* [collections/rental.json](collections/rental.json)

Queries:

* [queries/airline.json](queries/airline.json)
* [queries/rental.json](queries/rental.json)


When the tool starts, it automatically searches for collection definition files in the collections folder and generates queries at random. You can create your own collection definition files (in JSON format) and place them in the collections folder. Similarly, you can add custom query definition files (also in JSON format) to the queries folder. In order to use the queries from the `queries` folder you must provide the `--custom_queries` option.

If you prefer to organize your custom collections and queries in different directories you can do so—just be sure to specify their locations when running the tool, as it defaults to searching in the collections and queries folders if you don't.

To use custom collection definitions and queries from the default folders, simply provide the following parameters:

```
--collection_definition --custom_queries
```

To load files from custom locations supply the full paths, as shown in the example below. This will load all files found in those folders:

```
--collection_definition /tmp/collections --custom_queries /tmp/queries
```

NOTE: When using your own custom queries the `--optimized` parameter is ignored, since you control how your queries are written. You can either write optimized, ineffective queries or both. You can also create separate query definition files with optimized queries and ineffective queries, the use case is your choice..

## Usage

The workload is highly configurable at runtime, enabling you to fine-tune its behavior by specifying various parameters during execution. These parameters allow you to control key aspects of the workload, including workload duration, number of threads, query distribution, and more.

By default, the tool generates a predefined and pre-configured database and collection as explained above. However, you can optionally provide custom collection definition and query files as explained above. When used in combination, these options offer a high degree of flexibility, allowing you to adapt the application's behavior to meet your specific requirements and simulate operations that more accurately represent your target workload.

#### Getting help

You can obtain help and a list of all available parameters by running `./mongodbWorkload.py --help` 

```
./mongodbWorkload.py --help
usage: mongodbWorkload.py [-h] [--collections COLLECTIONS] [--collection_definition [COLLECTION_DEFINITION]] [--recreate] [--runtime RUNTIME] [--batch_size BATCH_SIZE] [--threads THREADS]
                          [--skip_update] [--skip_delete] [--skip_insert] [--skip_select] [--insert_ratio INSERT_RATIO] [--update_ratio UPDATE_RATIO] [--delete_ratio DELETE_RATIO]
                          [--select_ratio SELECT_RATIO] [--report_interval REPORT_INTERVAL] [--optimized] [--cpu CPU] [--log [LOG]] [--custom_queries [CUSTOM_QUERIES]] [--debug]

MongoDB Workload Generator

options:
  -h, --help            show this help message and exit
  --collections COLLECTIONS
                        How many collections to create (default 1).
  --collection_definition [COLLECTION_DEFINITION]
                        (Optional) Name of a JSON file (from collections/), full path to a file, or a directory. If omitted, all JSON files from 'collections/' will be used.
  --recreate            Recreate the collection before running the test.
  --runtime RUNTIME     Duration of the load test, specify in seconds (e.g., 60s) or minutes (e.g., 5m) (default 60s).
  --batch_size BATCH_SIZE
                        Number of documents per batch insert (default 10).
  --threads THREADS     Number of threads for simultaneous operations (default 4).
  --skip_update         Skip update operations.
  --skip_delete         Skip delete operations.
  --skip_insert         Skip insert operations.
  --skip_select         Skip select operations.
  --insert_ratio INSERT_RATIO
                        Percentage of insert operations (default 10).
  --update_ratio UPDATE_RATIO
                        Percentage of update operations (default 20).
  --delete_ratio DELETE_RATIO
                        Percentage of delete operations (default 10).
  --select_ratio SELECT_RATIO
                        Percentage of select operations (default 60).
  --report_interval REPORT_INTERVAL
                        Interval (in seconds) between workload stats output (default 5s).
  --optimized           Run optimized workload only.
  --cpu CPU             Number of CPUs to launch multiple instances in parallel (default 1).
  --log [LOG]           Log filename and path (e.g., /tmp/report.log).
  --custom_queries [CUSTOM_QUERIES]
                        (Optional) Path to a single JSON query file or a directory containing multiple .json query files. If no path provided, all JSON files from 'queries/' will be used.
  --debug               Enable debug logging to show queries and results.
```                        

#### Basic Usage 

Once you have configured the settings to match your environment, you can run the workload without specifying any parameters. This will utilize the default settings, providing a great way to familiarize yourself with the tool and review its output. The default setting is not optimized, this means it will randomly choose between optimized and innefective queries, so your performance may vary.

```
./mongodbWorkload.py
2025-07-15 18:09:34 - INFO - Loaded 1 collection definition(s) from 'collections/rental.json'
2025-07-15 18:09:34 - INFO - Loaded 1 collection definition(s) from 'collections/airline.json'
2025-07-15 18:09:34 - INFO - Collection 'cars' created in DB 'rental'
2025-07-15 18:09:34 - INFO - Sharding configured for 'rental.cars' with key {'rental_id': 'hashed'}
2025-07-15 18:09:34 - INFO - Successfully created index: 'rental_id_1_car_type_1'
2025-07-15 18:09:34 - INFO - Successfully created index: 'rental_id_1_pickup_location_1_dropoff_location_1'
2025-07-15 18:09:34 - INFO - Successfully created index: 'rental_id_1_rental_date_1_return_date_1'
2025-07-15 18:09:34 - INFO - Successfully created index: 'license_plate_1'
2025-07-15 18:09:34 - INFO - Collection 'flights' created in DB 'airline'
2025-07-15 18:09:34 - INFO - Sharding configured for 'airline.flights' with key {'flight_id': 'hashed'}
2025-07-15 18:09:34 - INFO - Successfully created index: 'flight_id_1_equipment.plane_type_1'
2025-07-15 18:09:34 - INFO - Successfully created index: 'flight_id_1_seats_available_1'
2025-07-15 18:09:34 - INFO - Successfully created index: 'flight_id_1_duration_minutes_1_seats_available_1'
2025-07-15 18:09:34 - INFO - Successfully created index: 'equipment.plane_type_1'
2025-07-15 18:09:34 - INFO -

Duration: 60 seconds
CPUs: 1
Threads: (Per CPU: 4 | Total: 4)
Database and Collection: (rental.cars | airline.flights)
Instances of the same collection: 1
Configure Sharding: True
Insert batch size: 10
Optimized workload: False
Workload ratio: (SELECTS: 60% | INSERTS: 10% | UPDATES: 20% | DELETES: 10%)
Report frequency: 5 seconds
Report logfile: None

===================================================================================================================
                                                 Workload Started
===================================================================================================================

2025-07-15 18:09:40 - INFO - AVG Operations last 5s (1 CPUs): 147.00 (SELECTS: 88.60, INSERTS: 13.60, UPDATES: 28.40, DELETES: 16.40)
2025-07-15 18:09:45 - INFO - AVG Operations last 5s (1 CPUs): 133.80 (SELECTS: 77.80, INSERTS: 15.40, UPDATES: 29.00, DELETES: 11.60)
2025-07-15 18:09:50 - INFO - AVG Operations last 5s (1 CPUs): 157.20 (SELECTS: 91.40, INSERTS: 15.20, UPDATES: 32.60, DELETES: 18.00)
2025-07-15 18:09:55 - INFO - AVG Operations last 5s (1 CPUs): 148.60 (SELECTS: 88.80, INSERTS: 15.80, UPDATES: 29.40, DELETES: 14.60)
2025-07-15 18:10:00 - INFO - AVG Operations last 5s (1 CPUs): 133.60 (SELECTS: 76.00, INSERTS: 14.00, UPDATES: 29.80, DELETES: 13.80)
2025-07-15 18:10:05 - INFO - AVG Operations last 5s (1 CPUs): 161.80 (SELECTS: 97.40, INSERTS: 13.00, UPDATES: 34.20, DELETES: 17.20)
2025-07-15 18:10:10 - INFO - AVG Operations last 5s (1 CPUs): 155.00 (SELECTS: 96.40, INSERTS: 14.20, UPDATES: 32.00, DELETES: 12.40)
2025-07-15 18:10:15 - INFO - AVG Operations last 5s (1 CPUs): 147.20 (SELECTS: 87.40, INSERTS: 15.40, UPDATES: 30.80, DELETES: 13.60)
2025-07-15 18:10:20 - INFO - AVG Operations last 5s (1 CPUs): 145.20 (SELECTS: 90.00, INSERTS: 12.60, UPDATES: 30.20, DELETES: 12.40)
2025-07-15 18:10:25 - INFO - AVG Operations last 5s (1 CPUs): 152.20 (SELECTS: 89.00, INSERTS: 15.40, UPDATES: 31.80, DELETES: 16.00)
2025-07-15 18:10:30 - INFO - AVG Operations last 5s (1 CPUs): 135.20 (SELECTS: 78.80, INSERTS: 13.20, UPDATES: 30.00, DELETES: 13.20)
2025-07-15 18:10:35 - INFO - AVG Operations last 5s (1 CPUs): 161.60 (SELECTS: 99.00, INSERTS: 13.80, UPDATES: 32.60, DELETES: 16.20)
2025-07-15 18:10:40 - INFO - AVG Operations last 5s (1 CPUs): 161.60 (SELECTS: 99.00, INSERTS: 13.80, UPDATES: 32.60, DELETES: 16.20)
2025-07-15 18:10:40 - INFO -
===================================================================================================================
                                                Workload Finished
===================================================================================================================

2025-07-15 18:10:40 - INFO -
====================================================================================================
|                                         Collection Stats                                         |
====================================================================================================
|       Database       |      Collection      |     Sharded      |      Size      |    Documents   |
====================================================================================================
|        rental        |         cars         |       True       |    0.06 MB     |       155      |
|       airline        |       flights        |       True       |    0.28 MB     |       120      |
====================================================================================================

2025-07-15 18:10:40 - INFO -
===================================================================================================================
                                             Combined Workload Stats
===================================================================================================================
Workload Runtime: 1.10 minutes
Total Operations: 8892 (SELECT: 5303, INSERT: 858, UPDATE: 1854, DELETE: 877)
AVG Operations: 135.21 (SELECTS: 80.64, INSERTS: 13.05, UPDATES: 28.19, DELETES: 13.34)
Total: (Documents Inserted: 8580 | Documents Found: 4375 | Documents Updated: 129381 | Documents Deleted: 8305)
===================================================================================================================
```

#### Advanced Usage

You have a wide range of options available, and the parameters are neither exclusive nor mutually dependent, allowing you to use as many or as few as needed.
The parameters available and their use cases are shown below:

1. collections -- Number of collections

  - The workload will create multiple instances of the same collection when you specify `--collections` with the desired number of collections, for example: `--collections 5`. Each collection will have its index count appended to its name, such as `flights_1`, `flights_2`, `flights_3`, and so on. You can create additional collections even after the initial workload has been run. For example, if you run the first workload with `--collections 5` and then run a new workload with `--collections 6`, a new collection will be created and the workload will run against all 6 of them.


2. collection_definition -- Collection structure

  - You can provide your own custom collection definition file in JSON format, following the provided examples found in the [collections](collections) folder. The collections from this folder are the default collections used by the workload if you do not provide your own. 
  
  - You can create your own JSON file and place it in the `collections` folder or anywhere else in your file system. If you place the JSON file in the `collections` folder, all you need to do is pass the file name, e.g: `--collection_definition my_custom_definition.json`. However, if you store the JSON in a different folder, then you have to provide the entire path, e.g.: `--collection_definition /tmp/my_custom_definition.json`. You can create as many collections as you would like, you just need to create separate collection defintion files and place them in the same folder.

  - It is recommended for ease of use to place your custom collection definitions in the [collections](collections) folder.

  - In addition to the standard and self explained parameters in the provided JSON files, you can also specify an option called "provider". This is the faker provider used to generate each specific random datatype. I have created custom [faker providers](https://faker.readthedocs.io/en/master/#providers) that are used by the JSON files included with the tool, their definition is available in [customProvider.py](./customProvider.py). Feel free to look at those and [create new ones if you'd like](https://faker.readthedocs.io/en/master/#how-to-create-a-provider) or use any of the many [faker providers](https://faker.readthedocs.io/en/master/#providers) available if you would like to further extend the tools functionality.

  - The collection definition file is where you configure the following:
    - database name
    - collection name
    - fields
    - sharding
    - indexes

  - Sample behavior summary:
    * No input (default behavior)	When `--collection_definition` is omitted	-- Loads all .json from [collections](collections)
    * `--collection_definition airline.json` - Loads collections/airline.json
    * `--collection_definition /tmp/custom/airline.json` - 	Loads from full path
    * `--collection_definition ./alt_defs/airline.json` - Loads from relative path
    * `--collection_definition /nonexistent/path` -	Fails with error
    * `--collection_definition /tmp/mycollections`	- (Directory path) Loads all .json files in that directory

3. recreate -- Recreating collections

  - If you want to start from scratch you can pass `--recreate` and this will drop and recreate everything for you based on the parameters you provide. 

4. optimized -- Optimized Workload
  
  - By default, this setting is disabled, allowing the workload to run all queries—including both optimized and inefficient ones. To execute a more performance-focused workload, use the `--optimized` flag when running the tool.

    When enabled, this mode ensures that queries utilize the primary key and appropriate indexes, instead of using random fields that may or may not have indexes or other less efficient patterns. Using this option significantly enhances the workload's performance and efficiency.

    Additionally, enabling this flag restricts find workloads to aggregate queries only, minimizing the overhead of transferring large volumes of data from the database to the client.

    This option is automatically ignored if you provide your own queries via `--custom_queries`.

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

13. Configure custom query templates

  - You can customize the workload by adding or removing queries in the [mongodbLoadQueries.py](mongodbLoadQueries.py) file to suit your specific requirements. The file includes a mix of optimized and intentionally inefficient queries to help simulate various workloads. The queries created by this file are randomly generated based on the collection definitions.

    During execution, the tool randomly selects between aggregation queries (e.g., `collection.count_documents(query)`) and select queries (e.g., `collection.find(query, projection)`), choosing from both optimized and inefficient options. If you prefer to use only optimized queries, you can enable the `--optimize` flag, which excludes inefficient queries from the workload.

    Note: The query definitions in this file are structured as templates, enabling the tool to dynamically execute queries against any custom collection. If you choose to modify this file, ensure that you follow the same structure to maintain compatibility with the workload tool.

14. Create custom queries

  - You have the ability to create your own queries, enabling the workload to run exactly the same queries as your application. In order to do this you just need to create your own JSON query definition files and add them to the `queries` folder.

  - The tool comes with 2 query definition files that can be used when the configuration `--custom_queries` is provided. If you provide this option alone (without specifying a file name or path), the tool will automatically randomly generate queries based on all query definition files present in the `queries` folder.  

  - The query definition files are in JSON format and 2 examples have been provided, so you can build your own custom queries to match your specific use case (make sure you follow the same syntax as provided in the examples).

  - You can also store your custom queries in a separate folder, however you will need to pass the path, and this will randomly generate queries based on all the files in that folder: `--custom_queries /tmp/custom_query_folder` 

15. Combine sharded and non-sharded collections in the same workload

  - You can create both sharded and non-sharded collections by following this workflow:
    1. Create a sharded workload by editing the JSON file accordingly and configuring the `shardConfig` parameter (see provided JSON for exmaple)
    2. Create another workload without configuring sharding in the JSON file (remove the `shardConfig` parameter), but increase the number of collections using `--collections` option (make sure the number specified is higher than the current number of collections). For example, if you have 1 collection, you can use `--collections 2` and this would create 1 new collection that's not sharded.

    The above workflow will utilize the existing sharded collections while creating new collections as non-sharded, since the sharding configuration was not provided, but the number of collections was increased with `--collections`.

16. Debug

  - You can run the tool in debug mode by providing the `--debug` argument to see more details about your workload and if you wish to troubleshoot query issues.
