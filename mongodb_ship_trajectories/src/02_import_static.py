# 02_import_static.py
# Import static vessel info + shiptype codes into MongoDB 

import pandas as pd
from pymongo import MongoClient, ASCENDING

from config import (
    MONGO_URI, DB_NAME,
    AIS_STATIC, AIS_CODES,
    COLL_VESSELS, COLL_SHIPTYPE_CODES
)


# Εισάγει unipi_ais_static.csv στη συλλογή vessels
# Εισάγει ais_codes_descriptions.csv στη συλλογή shiptype_codes
# Βάζει indexes για lookup

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    vessels = db[COLL_VESSELS]
    codes = db[COLL_SHIPTYPE_CODES]

    print("Dropping collections:", COLL_VESSELS, COLL_SHIPTYPE_CODES)
    vessels.drop()
    codes.drop()

    # -------------------------
    # 1) Shiptype codes
    # -------------------------
    print("Reading codes:", AIS_CODES)
    codes_df = pd.read_csv(AIS_CODES)

    # normalize column names 
    codes_df.columns = [c.strip() for c in codes_df.columns]

    codes_docs = codes_df.to_dict("records")
    if codes_docs:
        codes.insert_many(codes_docs, ordered=False)

    # create index
    if "code" in codes_df.columns:
        codes.create_index([("code", ASCENDING)])

    # -------------------------
    # 2) Vessels static
    # -------------------------
    print("Reading static:", AIS_STATIC)
    static_df = pd.read_csv(AIS_STATIC)

    static_df.columns = [c.strip() for c in static_df.columns]

    if "vessel_id" not in static_df.columns:
        raise ValueError("Expected column 'vessel_id' not found in unipi_ais_static.csv")

    # Dedup by vessel_id 
    before = len(static_df)
    static_df["vessel_id"] = static_df["vessel_id"].astype(str)
    static_df = static_df.drop_duplicates(subset=["vessel_id"], keep="first")
    after = len(static_df)
    print(f"Dedup vessel_id: {before} -> {after}")

    # Convert NaN to None 
    static_df = static_df.where(pd.notna(static_df), None)

    docs = static_df.to_dict("records")

    # Set _id = vessel_id 
    for d in docs:
        d["_id"] = d["vessel_id"]

    if docs:
        vessels.insert_many(docs, ordered=False)

    if "country" in static_df.columns:
        vessels.create_index([("country", ASCENDING)])
    if "shiptype" in static_df.columns:
        vessels.create_index([("shiptype", ASCENDING)])

    print("Done.")
    print("vessels:", vessels.count_documents({}))
    print("shiptype_codes:", codes.count_documents({}))

if __name__ == "__main__":
    main()



