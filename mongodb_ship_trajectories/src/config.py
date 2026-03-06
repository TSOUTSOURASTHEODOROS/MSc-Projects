from pathlib import Path

# ====== MongoDB ======
# το Compass στο mongodb://localhost:29018
MONGO_URI = "mongodb://localhost:29018"
DB_NAME = "maritime_routes_clean"

# ====== Project paths ======
PROJECT_ROOT = Path(r"C:\Users\georg\Desktop\MongoDB_exe - MaritimeRoutes")
DATASETS = PROJECT_ROOT / "datasets"

# ====== Dataset files  ======
AIS_DYNAMIC_JAN2019 = DATASETS / "unipi_ais_dynamic_2019" / "unipi_ais_dynamic_jan2019.csv"

AIS_STATIC = DATASETS / "ais_static" / "unipi_ais_static.csv"
AIS_CODES = DATASETS / "ais_static" / "ais_codes_descriptions.csv"

WEATHER_JAN2019_SHP = DATASETS / "noaa_weather" / "2019" / "jan" / "noaa_weather_jan2019_v2.shp"
PIRAEUS_PORT_SHP = DATASETS / "geodata" / "piraeus_port" / "piraeus_port.shp"

# ====== Collections ======
COLL_AIS_POINTS = "ais_points"
COLL_VESSELS = "vessels"
COLL_SHIPTYPE_CODES = "shiptype_codes"
COLL_WEATHER = "weather_points"

COLL_TRAJECTORIES = "trajectories"
COLL_FAILED = "failed_trajectories"
