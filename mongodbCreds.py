# Configuration for MongoDB connection
dbconfig = {
    "username": "your_db_user",
    "password": "your_db_pass",
    "port": 27017,
    "hosts": [
        "da-cl01-mongodb-mongos00",
        "da-cl01-mongodb-mongos01"
    ],
    # "port": 20000,
    # "hosts": [
    #     "localhost"
    # ],
    "serverSelectionTimeoutMS": 5000, # We need this to fail faster, otherwise the default is 30 seconds
    "connectTimeoutMS": 3000,  # Example timeout setting
    "maxPoolSize": 500, # Example pool setting
    # Leave replicaSet: None if connecting to mongos. Enter the appropriate replicaSet name if connecting to replicaSet instead of Mongos
    "replicaSet": None,  
    # "replicaSet": "rslab", 
    "authSource": "admin",  # Adjust for authentication
    "tls": "false",  # Example tls setting
}