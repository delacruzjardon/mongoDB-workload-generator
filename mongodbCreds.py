# Configuration for MongoDB connection
dbconfig = {
    "username": "login_here",
    "password": "password_here",
    "port": 27017,
    "hosts": [
        "mongos00",
        "mongos01"
    ],
    "serverSelectionTimeoutMS": 5000, # We need this to fail faster, otherwise the default is 30 seconds
    "connectTimeoutMS": 3000,  # Example timeout setting
    "maxPoolSize": 500, # Example pool setting
    # Leave replicaSet: None if connecting to mongos. Enter the appropriate replicaSet name if connecting to replicaSet instead of Mongos
    "replicaSet": None,  
    "authSource": "admin",  # Adjust for authentication
    "tls": "false",  # Example tls setting
}