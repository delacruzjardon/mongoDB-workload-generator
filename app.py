#!/usr/bin/env python3
import pymongo # type: ignore
from datetime import datetime
import random
import string
from faker import Faker # type: ignore
import time
import threading
import logging
import textwrap
import signal
import sys
from urllib.parse import urlencode  # Properly format URL parameters
from multiprocessing import Lock

# Global stop event for clean shutdown
stop_event = threading.Event()

# Import db config from a separate file mongodbCreds.py
from mongodbCreds import dbconfig  

# Import the query module, this has all select queries this script will randomly execute
import mongodbLoadQueries  

# Initialize a lock for logging
log_lock = Lock()

# Construct the hosts string dynamically
hosts = ",".join([f"{host}:{dbconfig['port']}" for host in dbconfig["hosts"]])

# Start building the base connection URI
connection_uri = f"mongodb://{dbconfig['username']}:{dbconfig['password']}@{hosts}"

# Additional connection parameters
conn_params = {
    key: str(value)
    for key, value in dbconfig.items()
    if key not in {"hosts", "username", "password", "port"} and value is not None
}

# Ensure proper replica set handling
if "replicaSet" in dbconfig and dbconfig["replicaSet"]:
    conn_params["replicaSet"] = dbconfig["replicaSet"]

# Append additional connection parameters if any exist
if conn_params:
    connection_uri += "/?" + urlencode(conn_params)

try:
    # Create MongoDB client
    client = pymongo.MongoClient(connection_uri)  
    # Attempt to fetch server information to trigger an error if the connection fails
    client.admin.command('ping')
except pymongo.errors.ServerSelectionTimeoutError as e:
    logging.fatal(f"""Unable to connect to MongoDB, please make sure your configuration is correct.\nError: {e}""")
    sys.exit(1)  # Exit the script with an error code

db = client["airlines"]
letters = string.ascii_lowercase

# Update query types. Queries are imported from above file
update_type = random.choice(["seats", "delay", "gate", "equipment", "ac_seats"])

aircraft_seat_map = {
    "Airbus A320": 170,
    "Boeing 737": 160,    
    "CRJ 1000": 100,
    "Embraer E190": 90,    
    "Dash 8-400": 85,
    "ATR-72": 75,    
    "ERJ-145": 50
}

fake = Faker()

process_id = 0
insert_count = 0
update_count = 0
delete_count = 0
select_count = 0
flight_ids_batch = set()  
docs_deleted = 0
docs_inserted = 0
docs_updated = 0
docs_selected = 0
lock = threading.Lock()

def handle_exit(signum, frame):
    """Handle Ctrl+C gracefully by signaling threads to stop."""
    print("\n[!] Ctrl+C detected! Stopping workload...")
    stop_event.set()  # Signal all threads to stop
    sys.exit(0)

def random_string(max_length):
    return ''.join(random.choice(letters) for _ in range(max_length))

####################
# Create collections
####################
def create_collection(collection_name, collections=1, recreate=False, shard=False):
    logging.info("Configuring Workload")
    for i in range(1, collections + 1):
        collection_name_with_suffix = f"{collection_name}_{i}"
        collection = db[collection_name_with_suffix]
        try:
            if recreate and collection_name_with_suffix in db.list_collection_names():
                collection.drop()

            if collection_name_with_suffix not in db.list_collection_names():
                db.create_collection(collection_name_with_suffix)
                logging.info(f"Collection {collection_name_with_suffix} created")
                collection.create_index([("flight_id"), ("equipment.plane_type")])
                collection.create_index([("flight_id"), ("seats_available")])
                collection.create_index([("flight_id"), ("duration_minutes"), ("seats_available")])
                collection.create_index([("equipment.plane_type")])
                logging.info(f"Indexes created")
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error creating collection '{collection_name_with_suffix}': {e}")

        # We make sure the user isn't trying to shard a replicaset. If the replicaset option has been configured, the workload 
        # can not be sharded and we disabled it.
        # If replicaset is configured we skip sharding
        if "replicaSet" in dbconfig and dbconfig["replicaSet"]:
            continue 
        else:
            if shard:                
                shard_collection(collection_name_with_suffix)      

###################
# Shard collections
###################
def shard_collection(collection):
    try:
        db[collection].create_index([("flight_id", "hashed")])
        client.admin.command("enableSharding", "airlines")
        client.admin.command("shardCollection", f"airlines.{collection}", key={"flight_id": "hashed"})
        logging.info(f"Sharding configured: {collection}")
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error sharding collection '{collection}': {e}")

################
# CRUD Functions
################
def insert_flight_data(collection, batch_size=10):
    global insert_count, docs_inserted
    documents = []
    # flight_ids_batch = set()  # Temporary set to hold flight_ids
    for _ in range(batch_size):
        flight_id = random.randint(0, 9999999)
        flight_ids_batch.add(flight_id)  # Store each flight_id that will be inserted so they can later be used for select, updates and deletes
        plane_type = random.choice(list(aircraft_seat_map.keys()))  # Randomly select a plane type
        total_seats = aircraft_seat_map[plane_type]  # Get corresponding seat count
        num_passengers = random.randint(1, min(70, total_seats))  # Ensure passengers don't exceed total seats for the given ac type
        seats_available = total_seats - num_passengers  # Compute available seats
                
        document = {
            "flight_id": flight_id,
            "flight_name": "Flight_" + random_string(5),
            "departure": fake.city(),
            "arrival": fake.city(),
            "gate": random.choice(string.ascii_uppercase) + str(random.randint(1, 10)),
            "timestamp": datetime.now(),
            "duration_minutes": random.randint(60, 600),
            "seats_available": seats_available,  # Dynamically calculated
            "passengers": [{
                "passenger_id": idx,
                "name": fake.name(),
                "seat_number": str(random.randint(1, total_seats // 3)) + random.choice(["A","B","C","D","E","F"])
            } for idx in range(1, num_passengers + 1)],
            "equipment": {
                "plane_type": plane_type,
                "total_seats": total_seats,
                "amenities": ["WiFi", "TV", "Power outlets"]
            },
            "flight_code": "FLT-" + str(random.randint(100, 999)),
        }
        documents.append(document)
    try:
        result = collection.insert_many(documents)
        with lock:
            insert_count += 1  # The number of insert statements called
            docs_inserted += batch_size
        # print(f"Inserted batch of {batch_size} flights")
    except pymongo.errors.PyMongoError as e:
        print(f"Error inserting documents: {e}")

def select_random_flights(collection, optimized):
    global select_count, docs_selected
    # Efficiently select a random flight_id from the set or use a random one if empty
    flight_id = random.choice(tuple(flight_ids_batch)) if flight_ids_batch else random.randint(0, 99999)
    id_min = random.randint(0, 50000)
    id_max = random.randint(50001, 99999)
    ac = random.choice(list(aircraft_seat_map.keys()))  # Randomly select a plane type
    seats = random.randint(0, 90)
    hrs_min = random.randint(2, 5)
    hrs_max = random.randint(6, 10)
    optimized_queries, ineffective_queries, query_projections = mongodbLoadQueries.select_queries(flight_id, id_min, id_max, ac, seats, hrs_min, hrs_max)
    try:
        if optimized: # We only run aggregate queries (count) as these are faster since it doesn't require sending records back from DB to client
            query = random.choice(optimized_queries)
            flight_count = collection.count_documents(query)
            if flight_count:
                with lock:
                    docs_selected += flight_count
                # print(f"Flights Matching: {flight_count}")
        else: # This is not an optimized workload. Chose a random ineffective query and its associated projection
            query_index = random.randint(0, len(ineffective_queries) - 1)
            query = ineffective_queries[query_index]
            # Get the associated projection for the given query
            projection = query_projections[query_index] if query_index < len(query_projections) else None  # Handle cases where ineffective queries don't have projections
            # Now run the randomly selected query. We limit the number of records returned because we don't actually need to return all of them as this is not the intent of the workload
            cursor = collection.find(query, projection).limit(5) if projection else collection.find(query).limit(5)
            result_count = sum(1 for _ in cursor)  # Efficient way to count without loading all results into memory
            if result_count:
                with lock:
                    docs_selected += result_count
            # print(f"Sample Flights Data: {results[:3]}")  # Print sample results if needed
        select_count += 1  # Number of select statements    
    except pymongo.errors.PyMongoError as e:
        print(f"Error selecting flight_id {flight_id}: {e}")

def update_random_flights(collection):
    global update_count, docs_updated
    # Efficiently select a random flight_id from the set or use a random one if empty
    flight_id = random.choice(tuple(flight_ids_batch)) if flight_ids_batch else random.randint(0, 99999)
    minutes = random.randint(5, 120)
    gate = random.choice(string.ascii_uppercase) + str(random.randint(1, 50))
    plane_type = random.choice(list(aircraft_seat_map.keys()))  # Randomly select a plane type
    total_seats = aircraft_seat_map[plane_type]  # Get corresponding seat count
    num_passengers = random.randint(1, min(70, total_seats))  # Ensure passengers don't exceed total seats
    seats_available = total_seats - num_passengers  # Compute available seats
    update_list = mongodbLoadQueries.update_queries(minutes, seats_available, plane_type, gate, update_type, total_seats)
    update_query = random.choice(update_list)

    try:
        result = collection.update_one({"flight_id": flight_id}, update_query)
        with lock:
            update_count += 1  # The number of update queries
        if result.modified_count > 0:
            # print(f"Updated flight_id {flight_id}")
            with lock:
                docs_updated += 1  # the number of documents successfully updated
    except pymongo.errors.PyMongoError as e:
        print(f"Error updating flight_id {flight_id}: {e}")

def delete_random_flights(collection):
    global delete_count, docs_deleted
    # Efficiently select a random flight_id from the set or use a random one if empty
    flight_id = random.choice(tuple(flight_ids_batch)) if flight_ids_batch else random.randint(0, 99999)
    try:
        result = collection.delete_one({"flight_id": flight_id})
        with lock:
            delete_count += 1  # The number of delete queries
        if result.deleted_count > 0:
            # print(f"Deleted flight_id {flight_id}")
            with lock:
                docs_deleted += 1  # the number of documents successfully deleted
    except pymongo.errors.PyMongoError as e:
        print(f"Error deleting flight_id {flight_id}: {e}")

#######################
# End of CRUD functions
#######################



#######################################
# Validate and configure workload ratio
#######################################
def workload_ratio_config(args):
    # Default ratios for unspecified values
    default_ratios = {
        "insert_ratio": 10,
        "update_ratio": 20,
        "delete_ratio": 10,
        "select_ratio": 60
    }
    # Store custom workload ratios
    ratio_args = {
        "insert_ratio": args.insert_ratio,
        "update_ratio": args.update_ratio,
        "delete_ratio": args.delete_ratio,
        "select_ratio": args.select_ratio,
    }
    # Store workloads to skip if any
    skip_args = {
        "skip_update": args.skip_update,
        "skip_delete": args.skip_delete,
        "skip_insert": args.skip_insert,
        "skip_select": args.skip_select,
    }
    # Ensure all required keys exist in the ratio_args, defaulting to None if missing
    ratios = {
        "insert_ratio": ratio_args.get("insert_ratio", None),
        "update_ratio": ratio_args.get("update_ratio", None),
        "delete_ratio": ratio_args.get("delete_ratio", None),
        "select_ratio": ratio_args.get("select_ratio", None),
    }
    # Take into account if the user wants to skip specific workloads
    skip = {
        "skip_update": skip_args.get("skip_update", False),
        "skip_delete": skip_args.get("skip_delete", False),
        "skip_insert": skip_args.get("skip_insert", False),
        "skip_select": skip_args.get("skip_select", False),
    }
    # Set ratios to 0 for skipped workloads
    for key, skip_flag in skip.items():
        if skip_flag:
            ratio_key = key.replace("skip_", "") + "_ratio"
            ratios[ratio_key] = 0
    # Calculate sum of specified ratios
    specified_ratios = {k: v for k, v in ratios.items() if v is not None}
    specified_sum = sum(specified_ratios.values())
    # If the sum exceeds 100%, warn and reset to defaults
    if specified_sum > 100:
        with log_lock:  # This ensures only one process can log at a time
            logging.warning(f"The total workload ratio is {round(specified_sum, 2)}%, which exceeds 100%. "
                            "Each workload ratio will be adjusted to their default values.")
        return default_ratios  # Reset to default ratios if sum exceeds 100%

    # Calculate the remaining percentage for unspecified ratios
    remaining_percentage = round(100 - specified_sum, 10)

    # Count the number of unspecified ratios
    unspecified_keys = [key for key in ratios if ratios[key] is None]

    if unspecified_keys:
        # Distribute the remaining percentage proportionally based on default values
        total_default = sum(default_ratios[key] for key in unspecified_keys)
        for key in unspecified_keys:
            ratios[key] = round((default_ratios[key] / total_default) * remaining_percentage, 10)

    # Ensure the total sum is exactly 100%
    total_weight = sum(ratios.values())
    if total_weight != 100:
        with log_lock:  # This ensures only one process can log at a time
            logging.info(f"The adjusted workload ratio is {round(total_weight, 10)}%, which is not 100%. "
                         "Rebalancing the ratios...")
        scale_factor = 100 / total_weight
        for key in ratios:
            ratios[key] = round(ratios[key] * scale_factor, 10)  # Round to avoid floating point precision issues

    # Re-assign the adjusted ratios to args after validation and remove floating point precision
    args.insert_ratio = ratios["insert_ratio"] 
    args.update_ratio = ratios["update_ratio"]
    args.delete_ratio = ratios["delete_ratio"]
    args.select_ratio = ratios["select_ratio"]
   
    return ratios

###############################
# Output workload configuration
###############################
def log_workload_config(args, workload_length, workload_ratios, workload_logged):
    # Check if the function has already been executed
    if workload_logged:
        return

    table_width = 115
    workload_details = textwrap.dedent(f"""\n 
    Duration: {workload_length}
    CPUs: {args.cpu}
    Threads: (Per CPU: {args.threads} | Total: {args.cpu * args.threads})    
    Collections: {args.collections}
    Configure Sharding: {args.shard}
    Insert batch size: {args.batch_size}
    Optimized workload: {args.optimized}
    Workload ratio: SELECTS: {int(round(float(workload_ratios['select_ratio']), 0))}% | INSERTS: {int(round(float(workload_ratios['insert_ratio']), 0))}% | UPDATES: {int(round(float(workload_ratios['update_ratio']), 0))}% | DELETES: {int(round(float(workload_ratios['delete_ratio']), 0))}%
    Report frequency: {args.report_interval} seconds
    Report logfile: {args.log}\n
    {'=' * table_width}
    {' Workload Started':^{table_width - 2}}
    {'=' * table_width}\n""")    
    # with log_lock:  # This ensures only one process can log at a time
    logging.info(workload_details)
    # Set the flag to True to prevent further logging
    workload_logged = True  
 
#################################
# Output real-time workload stats
#################################
def log_ops_per_interval(args, report_interval, total_ops_per_sec, selects_per_sec, inserts_per_sec, updates_per_sec, deletes_per_sec, process_id):
    if args.cpu_ops: # We only run this if the user has chosen per cpu ops report
        ops_per_cpu = (
            f"AVG Operations last {report_interval}s per CPU (CPU #{process_id}) : "
            f"{total_ops_per_sec:.2f} "
            f"(SELECTS: {selects_per_sec:.2f}, "
            f"INSERTS: {inserts_per_sec:.2f}, "
            f"UPDATES: {updates_per_sec:.2f}, "
            f"DELETES: {deletes_per_sec:.2f})"
        )
        with log_lock:  # This ensures only one process can log at a time
            logging.info(ops_per_cpu)

##########################################
# Calculate operations per report_interval
##########################################
def calculate_ops_per_interval(args, allThreads, report_interval=5, process_id=0, total_ops_dict=None, lock=None):
    last_insert_count = 0
    last_update_count = 0
    last_delete_count = 0
    last_select_count = 0
    while not stop_event.is_set() and any(thread.is_alive() for thread in allThreads):
        time.sleep(report_interval)  # Wait for report_interval seconds
        with lock:
            inserts_per_sec = (insert_count - last_insert_count) / report_interval
            updates_per_sec = (update_count - last_update_count) / report_interval
            deletes_per_sec = (delete_count - last_delete_count) / report_interval
            selects_per_sec = (select_count - last_select_count) / report_interval

            total_ops_per_sec = selects_per_sec + inserts_per_sec + updates_per_sec + deletes_per_sec
            last_insert_count = insert_count
            last_update_count = update_count
            last_delete_count = delete_count
            last_select_count = select_count

            # Update shared dictionary for total operations tracking
            if total_ops_dict is not None:
                total_ops_dict['insert'][process_id] = inserts_per_sec
                total_ops_dict['update'][process_id] = updates_per_sec
                total_ops_dict['delete'][process_id] = deletes_per_sec
                total_ops_dict['select'][process_id] = selects_per_sec

        log_ops_per_interval(args, report_interval, total_ops_per_sec, selects_per_sec, inserts_per_sec, updates_per_sec, deletes_per_sec, process_id)


##############################################
# Output total operations across all CPUs
##############################################
def log_total_ops_per_interval(args, total_ops_dict, stop_event, lock):
    if not args.cpu_ops: # We only run this if the user has not chosen per cpu ops report
        while not stop_event.is_set():
            time.sleep(args.report_interval)
            with lock:
                total_selects = sum(total_ops_dict['select'])
                total_inserts = sum(total_ops_dict['insert'])
                total_updates = sum(total_ops_dict['update'])
                total_deletes = sum(total_ops_dict['delete'])
                total_ops = total_selects + total_inserts + total_updates + total_deletes

                if total_ops: # We only provide the output if the total ops isn't zero
                    logging.info(
                        f"AVG Operations last {args.report_interval}s ({args.cpu} CPUs): {total_ops:.2f} "
                        f"(SELECTS: {total_selects:.2f}, INSERTS: {total_inserts:.2f}, "
                        f"UPDATES: {total_updates:.2f}, DELETES: {total_deletes:.2f})"
                    )

#####################################################################
# Obtain real-time workload stats for each CPU. This is stored in the 
# output_queue which is later summarized by the main application file
##################################################################### 
def workload_stats(select_count, insert_count, update_count, delete_count, process_id, output_queue):
    # Create the dictionary with the required structure
    stats_dict = {
        "process_id": process_id,
        "stats": {
            "select": select_count,
            "insert": insert_count,
            "delete": delete_count,
            "update": update_count,
            "docs_inserted": docs_inserted,
            "docs_selected": docs_selected,
            "docs_updated": docs_updated,
            "docs_deleted": docs_deleted
        }
    }
    output_queue.put(stats_dict)

##########################################################################
# Obtain stats for all collections but only when the workload has finished
# We only need to collect this from only one of the running CPUs since the 
# collection information would be the same. This is stored in the 
# collection_queue which is later summarized by the main application file
##########################################################################
def collection_stats(collection_name, collections, collection_queue):
    collstats_dict = {}  # Dictionary to store unique collection stats
    for i in range(1, collections + 1):
        collection_name_with_suffix = f"{collection_name}_{i}"
        try:
            collstats = db.command("collstats", collection_name_with_suffix)
            collstats_dict[collection_name_with_suffix] = {
                "sharded": collstats.get("sharded", False),
                "size": collstats.get("size", 0),
                "documents": collstats.get("count", 0),
                }
        except pymongo.errors.PyMongoError as e:
            print(f"Error retrieving stats: {e}")
    collection_queue.put(collstats_dict)


#############################################################
# Randomly choose operations and collections for the workload
#############################################################
def worker(runtime, collections, batch_size, skip_update, skip_delete, skip_insert, skip_select, insert_ratio=10, update_ratio=20, delete_ratio=10, select_ratio=60, optimized=False): 
    work_start = time.time()
    operations = ["insert", "update", "delete", "select"]
    weights = [insert_ratio, update_ratio, delete_ratio, select_ratio]

    while time.time() - work_start < runtime and not stop_event.is_set():  # Ensure graceful exit
        operation = random.choices(operations, weights=weights, k=1)[0] # randomly choose what kind of operation based on the workload ratio
        collection = random.choice(collections) # choose collections randomly

        if operation == "insert" and not skip_insert:
            insert_flight_data(collection, batch_size)
        elif operation == "update" and not skip_update:
            update_random_flights(collection)
        elif operation == "delete" and not skip_delete:
            delete_random_flights(collection)
        elif operation == "select" and not skip_select:
            select_random_flights(collection, optimized)       

####################
# Start the workload
####################
def start_workload(args, process_id="", completed_processes="",output_queue="", collection_queue="", total_ops_dict=None):
    # Handler for Ctrl+C
    signal.signal(signal.SIGINT, handle_exit)

    # We have to configure the logging here since the args are passed on from via this function
    # Configure logging (args are passed from mongodbWorkload.py)
    log_handlers = [logging.StreamHandler()]  # Always stream to console
    if args.log:  
        log_handlers.append(logging.FileHandler(args.log, mode="a"))  # Log to file if specified

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=log_handlers
    )

    collections = [db[f"{args.collection_name}_{i}"] for i in range(1, args.collections + 1)]

    try:
        # Start multiple worker threads
        allThreads = []
        for _ in range(args.threads):
            thread = threading.Thread(target=worker, args=(args.runtime, collections, args.batch_size, args.skip_update, args.skip_delete, args.skip_insert, args.skip_select, args.insert_ratio, args.update_ratio, args.delete_ratio, args.select_ratio, args.optimized))
            thread.start()
            allThreads.append(thread)

        # Start the thread to calculate QPS AFTER initializing the worker threads
        logging_thread = threading.Thread(target=calculate_ops_per_interval, args=(args, allThreads,args.report_interval,process_id, total_ops_dict, lock), daemon=True)
        logging_thread.start()

        # Wait for all threads to finish
        for thread in allThreads:
            thread.join()

    except KeyboardInterrupt:
        stop_event.set()  # Ensure the thread stops
        logging_thread.join()  # Wait for the thread to finish gracefully

    finally:
        time.sleep(5) # We sleep a few seconds to make sure not to overlap with the real-time workload report
        # Mark this process as complete
        completed_processes[process_id] = True
        # Get collection stats after the workload has completed
        collection_stats(args.collection_name, args.collections, collection_queue)
        # Get workload stats
        workload_stats(select_count, insert_count, update_count, delete_count, process_id, output_queue)
        
        
####################
# Start the workload
####################
if __name__ == "__main__":
    start_workload()
    