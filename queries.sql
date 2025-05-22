-- (stop, route) pairs for which any trip on that route passes through the stop.
-- Only keep "listed" route ids (throwing away weird unlisted stuff like shuttles, which are part of another route).

SELECT DISTINCT stop_times.stop_id, trips.route_id
FROM stop_times
  JOIN trips ON stop_times.trip_id = trips.trip_id
  JOIN routes ON trips.route_id = routes.route_id
WHERE routes.listed_route IS NULL
  OR routes.listed_route != 1
;
