import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["pymongo"]
    )
    
    # Your MongoDB connection code here
    from pymongo import MongoClient
    client = MongoClient("mongodb://mongodb:27017/")
    
    # Return data as a DataFrame
    db = client[dbt.config.get_var('mongo_database')]
    data = list(db[dbt.config.get_var('mongo_collection')].find())
    
    return pd.DataFrame(data)