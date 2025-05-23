-- relate a stop id to its connecting route ids
--
-- `stop_routes` relates a stop id to all ("listed") routes that pass directly through it
--
-- `child_stops` relates each stop to all of its children, or if it doesn't have any children, itself
--
-- we can then find all "connecting routes" by relating each stop to its connecting stops (via `connecting_stops`),
-- joining to `child_stops` to relate to the connecting stops' *children* (if any), since the parent stops aren't
-- listed in stop_times.txt, and finally joinint to `stop_routes` to get all routes that all children of all connecting
-- stops pass through. and finally, to be considered a "connecting" route, there has to be a reason to leave the stop
-- itself, so we remove the routes that go through the stop itself

WITH stop_routes AS (
  SELECT DISTINCT stop_times.stop_id, trips.route_id
  FROM stop_times
    JOIN trips ON stop_times.trip_id = trips.trip_id
    JOIN routes ON trips.route_id = routes.route_id
  WHERE routes.listed_route IS NULL
    OR routes.listed_route != 1
),
child_stops AS (
  SELECT stops1.stop_id, COALESCE(stops2.stop_id, stops1.stop_id) child_stop_id
  FROM stops stops1
    LEFT JOIN stops stops2 ON stops1.stop_id = stops2.parent_station
),
stop_connecting_routes AS (
  SELECT DISTINCT connecting_stops.stop_id, stop_routes.route_id
  FROM connecting_stops
    JOIN child_stops ON connecting_stops.connecting_stop_id = child_stops.stop_id
    JOIN stop_routes ON child_stops.child_stop_id = stop_routes.stop_id
  EXCEPT
  SELECT stop_id, route_id
  FROM stop_routes
)
SELECT stop_connecting_routes.stop_id, stop_connecting_routes.route_id
FROM stop_connecting_routes
  JOIN routes ON stop_connecting_routes.route_id = routes.route_id
ORDER BY stop_connecting_routes.stop_id, routes.route_sort_order
;

-- stop routes

WITH child_stops AS (
  SELECT stops1.stop_id, COALESCE(stops2.stop_id, stops1.stop_id) child_stop_id
  FROM stops stops1
    LEFT JOIN stops stops2 ON stops1.stop_id = stops2.parent_station
),
stop_routes AS (
  SELECT DISTINCT child_stops.stop_id, trips.route_id
  FROM child_stops
    JOIN stop_times ON child_stops.child_stop_id = stop_times.stop_id
    JOIN trips ON stop_times.trip_id = trips.trip_id
    JOIN routes ON trips.route_id = routes.route_id
  WHERE routes.listed_route IS NULL
    OR routes.listed_route != 1
)
SELECT stop_routes.stop_id, stop_routes.route_id, routes.route_short_name, routes.route_type
FROM stop_routes
  JOIN routes ON stop_routes.route_id = routes.route_id
ORDER BY stop_routes.stop_id, routes.route_sort_order
;
