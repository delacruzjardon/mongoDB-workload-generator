#!/usr/bin/env python3
from args import parser
import args as args_module  # so we can save the parsed args globally
from joblib import Parallel, delayed # type: ignore
import multiprocessing
import app  
# import custom as app
import logging
import time
import textwrap
import argparse
import sys
import os
import json 

args = parser.parse_args()
args_module.args = args

from logger import configure_logging
configure_logging(args.log if hasattr(args, "log") else None)

import mongo_client
mongo_client.init()

# Validate --log argument
if args.log is True:  # If --log is used but no file is provided
    print("Error: The --log option requires a filename and path (e.g., /tmp/report.log).", file=sys.stderr)
    sys.exit(1)

collection_def = None
shard_enabled = False
COLLECTION_DEF_DIR = 'collections/'

# Determine path to collection definition
if os.path.exists(args.collection_definition):
    collection_definition_path = args.collection_definition  # Use full or relative path as provided
else:
    collection_definition_path = os.path.join(COLLECTION_DEF_DIR, args.collection_definition)

# Validate and load the collection definition file
if not os.path.exists(collection_definition_path):
    logging.error(f"Error: Collection definition file '{collection_definition_path}' not found.", file=sys.stderr)
    sys.exit(1)

try:
    with open(collection_definition_path, 'r') as f:
        collection_def = json.load(f)
except json.JSONDecodeError as e:
    logging.error(f"Error: Failed to parse JSON file '{collection_definition_path}': {e}", file=sys.stderr)
    sys.exit(1)

# Validate database, collection, and sharding
for item in collection_def:
    database = item.get("databaseName")
    collection = item.get("collectionName")
    shard_config = item.get("shardConfig")
    shard_enabled = bool(shard_config)

    if not database or not collection:
        logging.error("Error: 'databaseName' and 'collectionName' must be provided in each collection definition.", file=sys.stderr)
        sys.exit(1)
    


################################################
# Obtain workload summary and provide the output
################################################
def workload_summary(workload_output,elapsed_time):
    # Initialize a dictionary to store the total sums
    total_stats = {"select": 0, "insert": 0, "delete": 0, "update": 0, "docs_inserted": 0, "docs_selected": 0, "docs_updated": 0, "docs_deleted": 0}
    # Iterate through each process entry in the workload list
    for entry in workload_output:
        stats = entry["stats"]
        # Sum up each stat
        total_stats["select"] += stats["select"]
        total_stats["insert"] += stats["insert"]
        total_stats["delete"] += stats["delete"]
        total_stats["update"] += stats["update"]
        total_stats["docs_inserted"] += stats["docs_inserted"]
        total_stats["docs_selected"] += stats["docs_selected"]
        total_stats["docs_updated"] += stats["docs_updated"]
        total_stats["docs_deleted"] += stats["docs_deleted"]
    
    table_width = 115

    if elapsed_time < 60:
        runtime = f"{elapsed_time:.2f} seconds"
        # logging.info(f"Workload Runtime: {elapsed_time:.2f} seconds")
    else:
        elapsed_time_minutes = elapsed_time / 60
        runtime = f"{elapsed_time_minutes:.2f} minutes"
        # logging.info(f"Workload Runtime: {elapsed_time_minutes:.2f} minutes")

    workload_stats = textwrap.dedent(f"""
{'=' * table_width}
{' Workload Stats (All CPUs Combined)':^{table_width - 2}}
{'=' * table_width}
Workload Runtime: {runtime}
CPUs Used: {args.cpu:<10}
Total Operations: {total_stats["select"] + total_stats["insert"] + total_stats["update"] + total_stats["delete"]} (SELECT: {total_stats["select"]}, INSERT: {total_stats["insert"]}, UPDATE: {total_stats["update"]}, DELETE: {total_stats["delete"]})
AVG QPS: {(total_stats["select"] + total_stats["insert"] + total_stats["update"] + total_stats["delete"]) / elapsed_time:.2f} (SELECTS: {total_stats["select"] / elapsed_time:.2f}, INSERTS: {total_stats["insert"] / elapsed_time:.2f}, UPDATES: {total_stats["update"] / elapsed_time:.2f}, DELETES: {total_stats["delete"] / elapsed_time:.2f})
Documents Inserted: {total_stats["docs_inserted"]}, Matching Documents Selected: {total_stats["docs_selected"]}, Documents Updated: {total_stats["docs_updated"]}, Documents Deleted: {total_stats["docs_deleted"]}
{'=' * table_width}\n""")
    logging.info(workload_stats)

##################################################
# Obtain collection summary and provide the output
##################################################   
def collection_summary(collection_output):
    unique_coll_stats = []
    seen = set()
    # Dedupe the collections (multiple CPU processes could run the same stats query)
    for item in collection_output:
        collection_name = (list(item.keys())[0])
        if collection_name not in seen:
            seen.add(collection_name)
            unique_coll_stats.append(item)

    # Build the entire table as a single string
    table = "\n"
    table += "="*80 + "\n"
    table += f"|{'Collection Stats':^78}| \n"
    table += "="*80 + "\n"
    table += f"|    {'Name':^20} | {'Sharded':^16} | {'Size':^14} | {'Documents':^15}|\n"
    table += "="*80 + "\n"

    # Print the unique stats for each collection
    for coll in unique_coll_stats:
        for coll_name, stats in coll.items():
            # Create collection stats for each collection in the dictionary
            # The strange ident below is so it lines up nicely with the rest of the other output messages
            size_in_mb = stats["size"] / 1024 / 1024  # Size in MB
            if size_in_mb >= 1024:
                # Convert to GB if the size is larger than 1024MB
                size_display = f"{size_in_mb / 1024:.2f} GB"
            else:
                # Otherwise, display in MB
                size_display = f"{size_in_mb:.2f} MB"

            # Add the collection's stats to the table
            table += f"|    {coll_name:^20} | {str(stats['sharded']):^16} | {size_display:^14} | {stats['documents']:^15}|\n"

    table += "="*80 + "\n"
    # Output the entire table in one call
    logging.info(table)    

########################################
# Monitor the workload for each CPU used
########################################
def monitor_completion(completed_processes):
    try:
        while not all(completed_processes):
            time.sleep(0.2)
        stop_event.set()
        total_ops_logger.join()  # Gracefully wait for logging process to finish
        total_ops_logger.close()  # Close process explicitly    

        table_width = 115
        workload_finished = textwrap.dedent(f"""
        {'=' * table_width}
        {' Workload Finished':^{table_width - 2}}
        {'=' * table_width}\n""")

        logging.info(workload_finished)
    except KeyboardInterrupt:
        logging.info("Monitoring interrupted. Cleaning up...")

#####################################
# Make the call to start the workload
# We use a slightly delayed start for each CPU to prevent some of the logging to get duplicated
#####################################
def delayed_start(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections):
    time.sleep(0.2)  # 200 milliseconds per process_id
    return app.start_workload(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections)

###############################
# Main section to start the app
###############################
if __name__ == "__main__":
    # Validate the number of CPUs selected aren't more than the total number of CPUs available, if so, assign the max available CPU
    available_cpus = os.cpu_count()
    if args.cpu > available_cpus:
        logging.info(f"Cannot set CPU to {args.cpu} as there are only {available_cpus} available. Workload will be configured to use {available_cpus} CPUs.")
        args.cpu = available_cpus
    # Validate workload duration
    if args.runtime.endswith("m"):
        duration = int(args.runtime[:-1])        
        args.runtime = duration * 60
        workload_length = str(duration) + " minutes"
    elif args.runtime.endswith("s"):
        duration = int(args.runtime[:-1])
        args.runtime = duration
        workload_length = str(duration) + " seconds"
    else:
        raise ValueError("Invalid time format. Use '60s' for seconds or '5m' for minutes.")    
    
    # We only need to run the create statements once (not for every CPU), so we run those here instead of within the multiprocessor below
    created_collections = app.create_collection(collection_def, args.collections, args.recreate)
    start_time = time.time()
    # Configure Workload Ratio
    workload_ratios = app.workload_ratio_config(args)
    app.log_workload_config(collection_definition_path,args,shard_enabled,workload_length,workload_ratios,workload_logged=False)
    workload_output = []
    collection_output = []
    # Create a shared manager for tracking each CPU process completion
    with multiprocessing.Manager() as manager:
        # Create list where each index corresponds to a CPU core (False initially)
        completed_processes = manager.list([False] * args.cpu)
        # Queue for workload output
        output_queue = manager.Queue()
        # Queue for collection summary
        collection_queue = manager.Queue()
        # Shared flag to ensure certain workload output messages are only displayed once. This is done to prevent unnecessasry duplication of worklog output
        # when the workload is being run using multiple CPUs
        # Boolean flag to track if the workload summary has already been displayed
        workload_logged = manager.Value('b', False)
        # Boolean flag to track if the collection summary has already been displayed
        collection_logged = manager.Value('b', False)  

        # Shared dictionary for storing per-CPU operations
        total_ops_dict = manager.dict({
            'insert': manager.list([0] * args.cpu),
            'update': manager.list([0] * args.cpu),
            'delete': manager.list([0] * args.cpu),
            'select': manager.list([0] * args.cpu),
        })

        lock = multiprocessing.Lock()
        stop_event = multiprocessing.Event()

        # Start a separate process for logging total operations across CPUs
        total_ops_logger = multiprocessing.Process(
            target=app.log_total_ops_per_interval, 
            args=(args, total_ops_dict, stop_event, lock)
        )
        total_ops_logger.start()

        # Run workload in parallel using all available CPU cores as per --cpu argument (default is 1)
        parallel_executor = Parallel(n_jobs=args.cpu)
        parallel_executor(
            delayed(delayed_start)(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections) for process_id in range(args.cpu)
        )
        # We add the workload and collection output from all CPUs to each appropriate queue so we can provide a summarized report after the workload has finished
        # Workload queue
        while not output_queue.empty():
            workload_output.append(output_queue.get())
        # Collection queue
        while not collection_queue.empty():
            collection_output.append(collection_queue.get())
        # Monitor the workload completion status
        monitor_completion(completed_processes)

    # Generate the final workload summary
    elapsed_time = time.time() - start_time
    collection_summary(collection_output)
    workload_summary(workload_output,elapsed_time)

    
