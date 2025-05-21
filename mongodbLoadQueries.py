# This file contains all queries for the workload. You can add new queries to the appropriate function and they'll be randomly chosen
# while the workload is running. All queries have a default set of parameters however, the workload does provide them async for a more
# realistic workload
# The queries below have a mix of "good" and "bad" queries. The good queries always use the shard key in order to avoid shard merges
# The bad queries do not use the shard key (on purpose) in order to create workload that's not optimal
# You can comment out the queries you do not want to include or add more queries

# SELECT queries
def select_queries(flight_id, id_min=1, id_max=1, ac="Dash 8-400", seats=1, hrs_min=1, hrs_max=2):
    optimized_queries = [
        {"flight_id": flight_id},  # Find a specific flight by ID
        {"flight_id": flight_id, "equipment.plane_type": ac},  # Find flights for a given flight with a specific aircraft type
        {"flight_id": flight_id, "seats_available": {"$gt": seats}},  # Find flights for a given flight with more than a given number of seats available
        {"flight_id": flight_id, "duration_minutes": {"$lt": hrs_max * 60}},  # Find flights for a given flight with a duration under x hours (converts to minutes since this is how it's stored)
        {"flight_id": flight_id, "duration_minutes": {"$gt": hrs_min * 60}},  # Find flights for a gien flight with a duration more than x hours (converts to minutes since this is how it's stored)
    ]

    ineffective_queries = [
        {"flight_id": {"$gte": id_min, "$lte": id_max}},  # Find flights for flight_id ranges
        {"flight_id": True, "equipment.plane_type": ac},  # Find all flights for a given aircraft type
        {"flight_id": True, "seats_available": {"$gt": seats}},  # Find all flights with more than a given number of seats available
        {"flight_id": True, "duration_minutes": {"$lt": hrs_max * 60}},  # Find all flights with a duration under x hours (converts to minutes since this is how it's stored)
        {"flight_id": True, "duration_minutes": {"$gt": hrs_min * 60}},  # Find all flights with a duration more than x hours (converts to minutes since this is how it's stored)
    ]

    # We have a combination of aggregate and non-aggreagate queries, so we use the projection below to limit the fields returned for the non aggregate queries
    # The projection below can be used for both of the above lists
    query_projections = [
        {"flight_id": 1, "_id": 0},  # Only return flight_id
        {"flight_id": 1, "equipment.plane_type": 1, "_id": 0},  # Return flight_id and aircraft type
        {"flight_id": 1, "seats_available": 1, "_id": 0},  # Return flight_id and available seats
        {"flight_id": 1, "duration_minutes": 1, "_id": 0},  # Return flight_id and flight duration (less than X hours)
        {"flight_id": 1, "duration_minutes": 1, "_id": 0},  # Return flight_id and flight duration (more than X hours)
    ]

    return optimized_queries, ineffective_queries, query_projections

# UPDATE queries
def update_queries(minutes=1, seats=1, ac="Dash 8-400", gate="B2", update_type="delay", total_seats="85"):
    if update_type == "seats":
        return [
            {"$set": {"seats_available": seats}},
        ]   
    elif update_type == "delay":
        return [
            {"$inc": {"duration_minutes": minutes}},
        ]
    elif update_type == "gate":
        return [
            {"$set": {"gate": gate}},
        ]
    elif update_type == "equipment":
        return [
            {"$set": {"equipment.plane_type": ac}},
        ]


    


