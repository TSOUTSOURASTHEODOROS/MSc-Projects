#####
import geopandas as gpd
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timezone

from config import MONGO_URI, DB_NAME, WEATHER_JAN2019_SHP, COLL_WEATHER


def epoch_sec_to_dt_utc(sec: int) -> datetime:
    # Epoch seconds -> datetime me timezone UTC
    # (dhlwnei kathara oti ola ta weather timestamps einai UTC)
    return datetime.fromtimestamp(sec, tz=timezone.utc)

# -----------------------------
# Helpers gia asfaleis metatropes + rounding
# -----------------------------

def _to_float(x):
    # - epistrefei None an den mporei na ginei float
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def _round(x, ndigits=2):
    # Geniko rounding helper:
    x = _to_float(x)
    if x is None:
        return None
    return round(x, ndigits)

# -----------------------------
# Metatropes monadwn
# -----------------------------

def k_to_c(k):
    # Kelvin -> Celsius
    # C = K - 273.15
    k = _to_float(k)
    if k is None:
        return None
    return k - 273.15

def pa_to_hpa(pa):
    # Pascal -> hPa
    # 1 hPa = 100 Pa  => hPa = Pa / 100
    pa = _to_float(pa)
    if pa is None:
        return None
    return pa / 100.0

def ms_to_kmh(ms):
    # meters/sec -> km/h
    # km/h = m/s * 3.6
    ms = _to_float(ms)
    if ms is None:
        return None
    return ms * 3.6

def m_to_km(m):
    # meters -> km
    m = _to_float(m)
    if m is None:
        return None
    return m / 1000.0

# -----------------------------
# Rounding rules (raw + derived)
# -----------------------------
# Idea:
# - Theloume na strogkilopoioume ta floats wste:
#   1) na min exoume "floating point noise"
#   2) na einai pio "stable" gia comparisons / storage
#


ROUNDING_RULES = {
    # -------- RAW fields (opws erxontai apo shapefile) --------
    # thermokrasies se Kelvin (2 dekadika arkoun)
    "TMP": 2,
    "TMIN": 2,
    "TMAX": 2,
    "DPT": 2,

    # piesi se Pascal
    "PRMSL": 2,

    # humidity % (2 dekadika)
    "RH": 2,

    # wind speeds se m/s (2 dekadika)
    "WSPD": 2,
    "GUST": 2,

    # wind directions se degrees (1 dekadiko arketo)
    "WDIRMAT": 1,
    "WDIRMET": 1,

    # precipitation (2 dekadika)
    "APCP": 2,

    # wind components se m/s (2 dekadika)
    "UGRD": 2,
    "VGRD": 2,

    # visibility (meters) -> tha to kanoume int (0 dekadika)
    "VIS": 0,

    # -------- DERIVED fields (nea) --------
    # Celsius  se 2 dekadika
    "tmp_c": 2,
    "tmin_c": 2,
    "tmax_c": 2,
    "dpt_c": 2,

    # hPa (2 dekadika)
    "prmsl_hpa": 2,

    # km/h (2 dekadika)
    "wspd_kmh": 2,
    "gust_kmh": 2,

    # km (2 dekadika)
    "vis_km": 2,

    # km/h (2 dekadika)
    "ugrd_kmh": 2,
    "vgrd_kmh": 2,
}

def _apply_rounding(doc: dict) -> None:

    for field, ndigits in ROUNDING_RULES.items():
        if field not in doc:
            continue

        # VIS: to theloume integer (meters), oxi float
        if field == "VIS":
            v = _to_float(doc.get(field))
            if v is None:
                doc[field] = None
            else:
                # round -> int, gia na min exoume 11300.0
                doc[field] = int(round(v))
            continue

        # gia ola ta alla: safe rounding
        doc[field] = _round(doc.get(field), ndigits)

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COLL_WEATHER]

    print("Dropping collection:", COLL_WEATHER)
    coll.drop()

    print("Reading shapefile:", WEATHER_JAN2019_SHP)
    gdf = gpd.read_file(WEATHER_JAN2019_SHP)

    # ELEGXOS: to shapefile prepei na exei geometry
    if "geometry" not in gdf.columns:
        raise ValueError("Shapefile has no geometry column")

    print("Rows:", len(gdf))
    print("Columns:", list(gdf.columns))

    inserted = 0
    batch = []

    # -----------------------------
    # Loop se kathe grammi tou shapefile
    # -----------------------------
    # Performance note:
    # - iterrows den einai to pio grigoro, alla gia ena import script
    #   mas noiazei perissotero na einai katharo + na kanei swsta mapping.
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            # An leipei geometry, den exoume ti na kanoume
            continue

        # An den einai Point (p.x. Polygon), pairnoume centroid gia na exoume ena Point
        if geom.geom_type != "Point":
            geom = geom.centroid

        # To shapefile mas exei timestamp_ se epoch seconds
        ts_epoch = row.get("timestamp_")
        if pd.isna(ts_epoch):
            continue
        ts_epoch = int(ts_epoch)

        # Vasiko doc:
        # - timestamp_ (epoch) + timestamp (datetime UTC)
        # - loc (GeoJSON Point) + lon/lat
        doc = {
            "timestamp_": ts_epoch,
            "timestamp": epoch_sec_to_dt_utc(ts_epoch),
            "timestamp_str": None,  # an yparxei string timestamp sto shapefile, to vazoume edw
            "loc": {"type": "Point", "coordinates": [float(geom.x), float(geom.y)]},
            "lon": float(geom.x),
            "lat": float(geom.y),
        }

        # -----------------------------
        # Copy OLA ta loipa pedia apo shapefile
        # (ektos apo geometry / timestamp_ / timestamp)
        # -----------------------------
        for c in gdf.columns:
            if c in ["geometry", "timestamp_", "timestamp"]:
                continue

            v = row.get(c)
            if pd.isna(v):
                v = None
            doc[c] = v

        # An yparxei "timestamp" string sto shapefile, kratame to string se timestamp_str
        if "timestamp" in gdf.columns:
            v = row.get("timestamp")
            if pd.isna(v):
                v = None
            doc["timestamp_str"] = v

        # -----------------------------
        # Metatropes monadwn (NEA pedia)
        # Kratame ta original fields OSO EINAI.
        #
        # Rounding:
        # -----------------------------

        # Thermokrasies (Kelvin -> Celsius)
        doc["tmp_c"] = k_to_c(doc.get("TMP"))
        doc["tmin_c"] = k_to_c(doc.get("TMIN"))
        doc["tmax_c"] = k_to_c(doc.get("TMAX"))
        doc["dpt_c"] = k_to_c(doc.get("DPT"))

        # Piesi (Pa -> hPa)
        # PRMSL ~ 101336 Pa => 1013.36 hPa
        doc["prmsl_hpa"] = pa_to_hpa(doc.get("PRMSL"))

        # Anemos (m/s -> km/h)
        doc["wspd_kmh"] = ms_to_kmh(doc.get("WSPD"))
        doc["gust_kmh"] = ms_to_kmh(doc.get("GUST"))

        # Oratotita (m -> km) [an yparxei]
        doc["vis_km"] = m_to_km(doc.get("VIS"))

        # U/V components (m/s -> km/h) [an yparxei]
        doc["ugrd_kmh"] = ms_to_kmh(doc.get("UGRD"))
        doc["vgrd_kmh"] = ms_to_kmh(doc.get("VGRD"))


        _apply_rounding(doc)

        # -----------------------------
        # Batch insert gia taxytita
        # -----------------------------
        batch.append(doc)

        if len(batch) >= 5000:
            coll.insert_many(batch, ordered=False)
            inserted += len(batch)
            print("Inserted:", inserted)
            batch = []

    # Insert ta ypoloipa
    if batch:
        coll.insert_many(batch, ordered=False)
        inserted += len(batch)

    # -----------------------------
    # Indexes (poli simantika gia queries meta)

    print("Creating indexes...")
    coll.create_index([("loc", "2dsphere")])
    coll.create_index([("timestamp", 1)])

    print("Done. Total weather docs:", coll.count_documents({}))

if __name__ == "__main__":
    main()
