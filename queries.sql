-- (stop, route) pairs for which any trip on that route passes through the stop.
-- Only keep "listed" route ids (throwing away weird unlisted stuff like shuttles, which are part of another route).

SELECT DISTINCT stop_times.stop_id, trips.route_id
FROM stop_times
  JOIN trips ON stop_times.trip_id = trips.trip_id
  JOIN routes ON trips.route_id = routes.route_id
WHERE routes.listed_route IS NULL
  OR routes.listed_route != 1
;

-- Same as above, except the route ids are those that belong to a connecting stop

WITH stop_routes AS (
  SELECT DISTINCT stop_times.stop_id, trips.route_id
  FROM stop_times
    JOIN trips ON stop_times.trip_id = trips.trip_id
    JOIN routes ON trips.route_id = routes.route_id
  WHERE routes.listed_route IS NULL
    OR routes.listed_route != 1
)
SELECT DISTINCT connecting_stops.stop_id, stop_routes.route_id connecting_stop_route_id
FROM connecting_stops
  JOIN stop_routes ON connecting_stops.connecting_stop_id = stop_routes.stop_id
;

-- Same as above, except with a stop's own route ids removed
-- I believe this therefore relates a stop to "connecting routes", i.e. routes that don't go through the stop but do
-- go through one of its connecting stops
-- Oh wait what about "parent station"...
-- To pick up next time: stop 'place-pktrm' has no entries in stop_times.txt but it is a parent station to which
-- many other stops point

WITH stop_routes AS (
  SELECT DISTINCT stop_times.stop_id, trips.route_id
  FROM stop_times
    JOIN trips ON stop_times.trip_id = trips.trip_id
    JOIN routes ON trips.route_id = routes.route_id
  WHERE routes.listed_route IS NULL
    OR routes.listed_route != 1
)
SELECT DISTINCT connecting_stops.stop_id, stop_routes.route_id
FROM connecting_stops
  JOIN stop_routes ON connecting_stops.connecting_stop_id = stop_routes.stop_id
EXCEPT
SELECT stop_id, route_id
FROM stop_routes
;
