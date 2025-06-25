#!/usr/bin/env python3
import pymongo
import logging
import sys
from urllib.parse import urlencode

_db = None
_client = None 

def init():
    global _db, _client
    from args import args
    from mongodbCreds import dbconfig

    hosts = ",".join([f"{host}:{dbconfig['port']}" for host in dbconfig["hosts"]])
    connection_uri = f"mongodb://{dbconfig['username']}:{dbconfig['password']}@{hosts}"

    conn_params = {
        key: str(value)
        for key, value in dbconfig.items()
        if key not in {"hosts", "username", "password", "port"} and value is not None
    }
    if "replicaSet" in dbconfig and dbconfig["replicaSet"]:
        conn_params["replicaSet"] = dbconfig["replicaSet"]
    if conn_params:
        connection_uri += "/?" + urlencode(conn_params)

    try:
        _client = pymongo.MongoClient(connection_uri)
        _client.admin.command('ping')
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logging.fatal(f"Unable to connect to MongoDB, please check your config.\nError: {e}")
        sys.exit(1)

    _db = _client["config"]
    # logging.info(f"Connected to MongoDB")

def get_db():
    if _db is None:
        raise RuntimeError("MongoDB not initialized. Call mongo_client.init() first.")
    return _db

def get_client():  
    if _client is None:
        raise RuntimeError("MongoClient not initialized. Call mongo_client.init() first.")
    return _client
