import pandas as pd
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timezone

from config import MONGO_URI, DB_NAME, AIS_DYNAMIC_JAN2019, COLL_AIS_POINTS


# Διαβάζει το unipi_ais_dynamic_jan2019.csv σε chunks
# Μετατρέπει t (ms) → timestamp (UTC datetime)
# Μετατρέπει (lon,lat) → GeoJSON loc
# Βάζει indexes (2dsphere, time, vessel+time)

def ms_to_dt_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COLL_AIS_POINTS]

    print("Dropping collection:", COLL_AIS_POINTS)
    coll.drop()

    csv_path = AIS_DYNAMIC_JAN2019
    print("Reading file:", csv_path)

    chunk_size = 200_000
    inserted = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        docs = []

        # columns: t, vessel_id, lon, lat, heading, speed, course
        for row in chunk.itertuples(index=False):
            docs.append({
                "vessel_id": str(row.vessel_id),
                "timestamp": ms_to_dt_utc(int(row.t)),
                "loc": {"type": "Point", "coordinates": [float(row.lon), float(row.lat)]},
                "speed": None if pd.isna(row.speed) else float(row.speed),
                "heading": None if pd.isna(row.heading) else float(row.heading),
                "course": None if pd.isna(row.course) else float(row.course),
            })

        if docs:
            coll.insert_many(docs, ordered=False)
            inserted += len(docs)
            print("Inserted:", inserted)

    print("Creating indexes...")
    coll.create_index([("loc", "2dsphere")])
    coll.create_index([("timestamp", ASCENDING)])
    coll.create_index([("vessel_id", ASCENDING), ("timestamp", ASCENDING)])

    print("Done. Total docs:", coll.count_documents({}))

if __name__ == "__main__":
    main()
