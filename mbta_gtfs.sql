DROP TABLE IF EXISTS stops;

CREATE TABLE stops (
  stop_id TEXT NOT NULL PRIMARY KEY,
  stop_code TEXT,
  stop_name TEXT NOT NULL,
  stop_desc TEXT,
  platform_code TEXT,
  platform_name TEXT,
  stop_lat REAL NOT NULL,
  stop_lon REAL NOT NULL,
  zone_id TEXT,
  stop_address TEXT,
  stop_url TEXT,
  level_id TEXT,
  location_type INTEGER NOT NULL,
  parent_station TEXT,
  wheelchair_boarding INTEGER NOT NULL,
  municipality TEXT NOT NULL,
  on_street TEXT,
  at_street TEXT,
  vehicle_type INTEGER NOT NULL
) WITHOUT ROWID;
