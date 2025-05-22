CREATE TABLE IF NOT EXISTS routes (
  route_id TEXT NOT NULL PRIMARY KEY,
  agency_id INTEGER NOT NULL,
  route_short_name TEXT,
  route_long_name TEXT,
  route_desc TEXT NOT NULL,
  route_type INTEGER NOT NULL,
  route_url TEXT,
  route_color TEXT NOT NULL,
  route_text_color TEXT NOT NULL,
  route_sort_order INTEGER NOT NULL,
  route_fare_class TEXT NOT NULL,
  line_id TEXT,
  listed_route INTEGER,
  network_id TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS stop_times (
  trip_id TEXT NOT NULL,
  arrival_time TEXT NOT NULL,
  departure_time TEXT NOT NULL,
  stop_id TEXT NOT NULL,
  stop_sequence INTEGER NOT NULL,
  stop_headsign TEXT,
  pickup_type INTEGER NOT NULL,
  drop_off_type INTEGER NOT NULL,
  timepoint INTEGER NOT NULL,
  checkpoint_id TEXT,
  continuous_pickup INTEGER,
  continuous_drop_off INTEGER,
  PRIMARY KEY (trip_id, stop_sequence)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS stops (
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

CREATE TABLE IF NOT EXISTS trips (
  route_id TEXT NOT NULL,
  service_id TEXT NOT NULL,
  trip_id TEXT NOT NULL PRIMARY KEY,
  trip_headsign TEXT NOT NULL,
  trip_short_name TEXT,
  direction_id INTEGER NOT NULL,
  block_id TEXT,
  shape_id TEXT NOT NULL,
  wheelchair_accessible INTEGER NOT NULL,
  trip_route_type INTEGER,
  route_pattern_id TEXT NOT NULL,
  bikes_allowed INTEGER NOT NULL
) WITHOUT ROWID;
