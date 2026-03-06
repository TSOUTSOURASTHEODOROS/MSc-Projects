
from pymongo import MongoClient
from pymongo.errors import DocumentTooLarge
from datetime import timedelta, datetime, timezone

from config import (
    MONGO_URI, DB_NAME,
    COLL_AIS_POINTS, COLL_WEATHER,
    COLL_TRAJECTORIES, COLL_FAILED
)


GAP_HOURS = 6

WEATHER_WINDOW_HOURS = 3

WEATHER_MAX_DISTANCE_METERS = 50_000

# =========================================
# UPDATE #1 (Enrichment vessels -> trajectories)
# =========================================

try:
    from config import COLL_VESSELS  # type: ignore
except Exception:
    COLL_VESSELS = "vessels"


# =========================================
# UPDATE #2 (Annotations)
# =========================================
TURN_THRESHOLD_DEG = 45.0

STOP_MAX_SPEED = 0.5
SLOW_MAX_SPEED = 2.0
NORMAL_MIN_SPEED = 2.0
FAST_MIN_SPEED = 10.0



# =========================================
# WEATHER FIELDS (για έλεγχο μεγέθους)
# =========================================

ALL_WEATHER_FIELDS = [
    # --- canonical / basic metadata ---
    "timestamp", "timestamp_", "timestamp_str",
    "loc", "lon", "lat",
    # --- original NOAA fields (όπως έρχονται από shapefile) ---
    "TMP", "TMIN", "TMAX",
    "PRMSL",
    "RH",
    "GUST",
    "VIS",
    "WSPD",
    "WDIRMAT", "WDIRMET",
    "DPT",
    "APCP",
    "UGRD", "VGRD",
    # --- derived / converted fields (που προσθέσαμε εμείς) ---
    "tmp_c", "tmin_c", "tmax_c", "dpt_c",
    "prmsl_hpa",
    "wspd_kmh", "gust_kmh",
    "vis_km",
    "ugrd_kmh", "vgrd_kmh",
]

# Εδώ κρατάμε μόνο τα πεδία που θα κάνουμε embed μέσα στα trajectory points.
# Γιατί: αν βάλουμε όλα τα weather fields σε κάθε point, το document μεγαλώνει πολύ
# και μπορεί να χτυπήσουμε το MongoDB 16MB document limit.
ACCEPTED_WEATHER_FIELDS = [
    # --- time ---
    "timestamp",
    # --- temperature (Celsius, πιο χρήσιμο από Kelvin) ---
    "tmp_c",
    # --- pressure ---
    "prmsl_hpa",
    # --- humidity ---
    "RH",
    # --- wind ---
    "wspd_kmh",
    "WDIRMET",
    "gust_kmh",
    # --- rain ---
    "APCP",
]


WEATHER_PROJECTION = {k: 1 for k in ACCEPTED_WEATHER_FIELDS}
WEATHER_PROJECTION["_id"] = 0


# =========================================
# ΒΟΗΘΗΤΙΚΑ: bbox / centroid
# =========================================

def bbox_from_coords(coords):
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return {
        "min_lon": min(lons), "max_lon": max(lons),
        "min_lat": min(lats), "max_lat": max(lats),
    }

def centroid_from_bbox(b):
    return {
        "type": "Point",
        "coordinates": [
            (b["min_lon"] + b["max_lon"]) / 2,
            (b["min_lat"] + b["max_lat"]) / 2
        ]
    }


# =========================================
# BUCKETING + CACHING για Weather
# =========================================

def floor_to_bucket_hours(ts: datetime, bucket_hours: int) -> datetime:

    if ts.tzinfo is None:
        # για να είμαστε safe, το θεωρούμε UTC
        ts = ts.replace(tzinfo=timezone.utc)

    h = ts.hour
    bucket_start_hour = (h // bucket_hours) * bucket_hours
    return ts.replace(hour=bucket_start_hour, minute=0, second=0, microsecond=0)

def make_weather_cache_key(ts: datetime, lon: float, lat: float):

    time_bucket = floor_to_bucket_hours(ts, WEATHER_WINDOW_HOURS)
    lon_bucket = round(lon, 2)
    lat_bucket = round(lat, 2)
    return (time_bucket, lon_bucket, lat_bucket)

def find_weather_with_cache(db, weather_cache: dict, point_loc: dict, ts: datetime):

    if ts is None or point_loc is None:
        return None

    coords = point_loc.get("coordinates")
    if not coords or len(coords) != 2:
        return None

    lon, lat = coords[0], coords[1]

    # 1) Φτιάξε cache key με bucketing (3 ώρες + ~1km)
    key = make_weather_cache_key(ts, lon, lat)

    # 2) Αν υπάρχει ήδη στο cache, επιστρέφουμε αμέσως (χωρίς query)
    if key in weather_cache:
        return weather_cache[key]

    # 3) Αλλιώς, κάνουμε query στη Mongo μόνο 1 φορά για αυτό το bucket
    wcoll = db[COLL_WEATHER]

    tmin = ts - timedelta(hours=WEATHER_WINDOW_HOURS)
    tmax = ts + timedelta(hours=WEATHER_WINDOW_HOURS)

    q = {
        "loc": {
            "$near": {
                "$geometry": point_loc,
                "$maxDistance": WEATHER_MAX_DISTANCE_METERS
            }
        },
        "timestamp": {"$gte": tmin, "$lte": tmax}
    }

    # Παίρνουμε μόνο accepted fields (projection)
    doc = wcoll.find_one(q, WEATHER_PROJECTION)

    # Αν δεν βρούμε τίποτα, αποθηκεύουμε None για να μην ξανακάνουμε το ίδιο query
    weather_cache[key] = doc if doc else None
    return weather_cache[key]


# =========================================
# UPDATE #2 (Annotations helpers)
# =========================================

def angular_diff_deg(a: float, b: float) -> float:
    """
    Ypologizei thn mikroteri gwniaki diafora metaxy 2 angles (0..360)
    Paradeigma:
      350 -> 10 => 20 (oxi 340)
    """
    diff = abs(a - b)
    return min(diff, 360 - diff)

def build_annotations(speed, heading, course, prev_course):
    """
    Ftiaxnei mia LISTA me annotations gia ena point.
    """
    tags = []

    # --- Speed-based tags ---
    # An speed den yparxei (None), den vazoume tipota
    if speed is not None:
        if speed <= STOP_MAX_SPEED:
            tags.append("STOP")
        elif speed < SLOW_MAX_SPEED:
            tags.append("SLOW")
        elif speed >= FAST_MIN_SPEED:
            tags.append("FAST")
        else:
            #  NORMAL: 2.0 <= speed < 10.0
            tags.append("NORMAL")

    # --- Heading missing ---
    if heading is None:
        tags.append("MISSING_HEADING")
    
    # --- Course missing ---
    if course is None:
        tags.append("MISSING_COURSE")

    # --- Turn tag ---
    # Turn mporei na vgei mono an exoume kai course kai prev_course (kai ta 2 not None)
    if course is not None and prev_course is not None:
        angle = angular_diff_deg(float(course), float(prev_course))
        if angle >= TURN_THRESHOLD_DEG:
            tags.append("TURN")

    return tags


# =========================================
# ΑΠΟΘΗΚΕΥΣΗ TRAJECTORY (με χειρισμό 16MB)
# =========================================

def flush_trajectory(db, traj_doc, failed_coll):
    """
    DocumentTooLarge λόγω 16MB limit, το στέλνει στο failed_trajectories.
    """
    if traj_doc is None:
        return

    tcoll = db[COLL_TRAJECTORIES]

    try:
        tcoll.insert_one(traj_doc)

    except DocumentTooLarge:
        # MongoDB limitation: max 16MB ανά document
        failed_coll.insert_one({
            "reason": "DocumentTooLarge",
            "vessel_id": traj_doc.get("vessel_id"),
            "start_time": traj_doc.get("start_time"),
            "end_time": traj_doc.get("end_time"),
            "num_points": traj_doc.get("num_points"),
        })

    except Exception as e:
        failed_coll.insert_one({
            "reason": "OtherInsertError",
            "error": str(e),
            "vessel_id": traj_doc.get("vessel_id"),
            "start_time": traj_doc.get("start_time"),
            "end_time": traj_doc.get("end_time"),
            "num_points": traj_doc.get("num_points"),
        })


# =========================================
# MAIN
# =========================================

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    ais = db[COLL_AIS_POINTS]
    traj_coll = db[COLL_TRAJECTORIES]
    failed_coll = db[COLL_FAILED]

    # =========================================
    # STEP 0: Drop previous collections
    # =========================================
    print("Dropping:", COLL_TRAJECTORIES, COLL_FAILED)
    traj_coll.drop()
    failed_coll.drop()

    # =========================================
    # STEP 1: Load AIS points sorted
    # =========================================
    cursor = ais.find({}, {"_id": 0}).sort([("vessel_id", 1), ("timestamp", 1)])

    # Cache gia weather 
    weather_cache = {}

    # =========================================
    # Variables pou kratane state gia to streaming build
    # =========================================
    current = None
    prev_vessel = None
    prev_time = None

    # Gia bbox calculation
    coords_for_bbox = []

    # Metrisi trajectories
    traj_count = 0

    # UPDATE #2:
    # Kratame prev_course mono mesa sto idio trajectory,
    # gia na vgaloume "TURN".
    prev_course_in_traj = None

    # =========================================
    # STEP 2: Stream through points and build trajectories
    # =========================================
    for doc in cursor:
        vessel = doc["vessel_id"]
        ts = doc.get("timestamp")
        loc = doc.get("loc")

        # -----------------------------------------
        # STEP 2a: Decide if we start new trajectory
        # -----------------------------------------
        new_traj = False
        if prev_vessel is None:
            new_traj = True
        elif vessel != prev_vessel:
            new_traj = True
        elif prev_time is not None and ts is not None:
            if ts - prev_time > timedelta(hours=GAP_HOURS):
                new_traj = True

        # -----------------------------------------
        # STEP 2b: If new trajectory -> close previous
        # -----------------------------------------
        if new_traj:
            if current and coords_for_bbox:
                b = bbox_from_coords(coords_for_bbox)
                current["bbox"] = b
                current["centroid"] = centroid_from_bbox(b)
                current["num_points"] = len(current["points"])

                flush_trajectory(db, current, failed_coll)
                traj_count += 1

                if traj_count % 50 == 0:
                    print("Saved trajectories:", traj_count, "| weather_cache size:", len(weather_cache))

            # -----------------------------------------
            # STEP 2c: Open new trajectory doc
            # -----------------------------------------
            vdoc = db[COLL_VESSELS].find_one(
                {"vessel_id": vessel},
                {"_id": 0, "country": 1, "shiptype": 1}
            )

            current = {
                "vessel_id": vessel,
                "start_time": ts,
                "end_time": ts,
                "points": [],
                # enrichment info
                "vessel": {
                    "country": vdoc.get("country") if vdoc else None,
                    "shiptype": vdoc.get("shiptype") if vdoc else None,
                }
            }

            coords_for_bbox = []

            # UPDATE #2:
            # Reset prev_course gia neo trajectory
            prev_course_in_traj = None

        # -----------------------------------------
        # STEP 2d: Weather enrichment (with caching + projection)
        # -----------------------------------------
        weather = find_weather_with_cache(db, weather_cache, loc, ts)

        # -----------------------------------------
        # STEP 2e: Build point annotations
        # -----------------------------------------
        # Edw ftiaxnoume annotations lista gia to point (STOP/SLOW/FAST/TURN/MISSING_HEADING)
        speed = doc.get("speed")
        heading = doc.get("heading")
        course = doc.get("course")

        annotations = build_annotations(speed, heading, course, prev_course_in_traj)

        # Meta pou ypologisame TURN, an exoume valid course,
        # krata to san prev_course gia to epomeno point.
        if course is not None:
            prev_course_in_traj = course

        # -----------------------------------------
        # STEP 2f: Store point inside current trajectory
        # -----------------------------------------
        point = {
            "timestamp": ts,
            "loc": loc,
            "speed": speed,
            "heading": heading,
            "course": course,
            "annotations": annotations,  # <-- LISTA tags
            "weather": weather
        }

        current["points"].append(point)
        current["end_time"] = ts

        # κρατάμε coordinates για bbox
        if loc and "coordinates" in loc:
            coords_for_bbox.append(loc["coordinates"])

        prev_vessel = vessel
        prev_time = ts

    # =========================================
    # STEP 3: Flush last trajectory
    # =========================================
    if current and coords_for_bbox:
        b = bbox_from_coords(coords_for_bbox)
        current["bbox"] = b
        current["centroid"] = centroid_from_bbox(b)
        current["num_points"] = len(current["points"])
        flush_trajectory(db, current, failed_coll)
        traj_count += 1

    # =========================================
    # STEP 4: Create indexes
    # =========================================
    print("Creating indexes...")
    traj_coll.create_index([("vessel_id", 1)])
    traj_coll.create_index([("start_time", 1)])
    traj_coll.create_index([("centroid", "2dsphere")])

    print("Done.")
    print("trajectories:", traj_coll.count_documents({}))
    print("failed:", failed_coll.count_documents({}))
    print("weather_cache final size:", len(weather_cache))


if __name__ == "__main__":
    main()
