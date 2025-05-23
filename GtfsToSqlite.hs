import Control.Monad (when)
import Cretheus.Decode qualified
import Cretheus.Decode qualified as Cretheus (Decoder)
import Cretheus.Encode qualified
import Cretheus.Encode qualified as Cretheus (Encoding, PropertyEncoding)
import Crypto.Hash.MD5 qualified as Md5
import Data.Aeson qualified as Aeson
import Data.Aeson.Encode.Pretty qualified as Aeson.Pretty
import Data.Aeson.Key qualified as Aeson.Key
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Csv qualified as Cassava (FromNamedRecord (..), NamedRecord, Parser, lookup)
import Data.Csv.Streaming qualified as Cassava
import Data.Foldable (for_)
import Data.Int (Int64)
import Data.Map.Merge.Strict qualified as Map
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Database.SQLite3 qualified as Sqlite
import NeatInterpolation qualified

main :: IO ()
main = do
  Sqlite.withDatabase "mbta_gtfs.sqlite" \database -> do
    processConnectingStops database
    processRoutes database
    processStops database
    processStopTimes database
    processTrips database
    processStopConnectingRoutes database

------------------------------------------------------------------------------------------------------------------------
-- Connecting stops

processConnectingStops :: Sqlite.Database -> IO ()
processConnectingStops database = do
  bytes <- LazyByteString.readFile "data/connecting-stops.json"
  let md5 = Md5.hashlazy bytes
  storedMd5 <-
    Sqlite.withStatement database "SELECT md5 FROM connecting_stops_md5" \statement -> do
      Sqlite.stepNoCB statement >>= \case
        Sqlite.Row -> Just <$> Sqlite.columnBlob statement 0
        Sqlite.Done -> pure Nothing

  if Just md5 == storedMd5
    then Text.putStrLn "connecting_stops table in mbta_gtfs.sqlite is up-to-date with data/connecting-stops.json"
    else do
      Text.putStrLn "Inserting data/connecting-stops.json into mbta_gtfs.sqlite"
      let decoder :: Cretheus.Decoder (Map Text [Text])
          decoder =
            Cretheus.Decode.map Aeson.Key.toText (Cretheus.Decode.list Cretheus.Decode.text)
      case Cretheus.Decode.fromLazyBytes decoder bytes of
        Left err -> error (Text.unpack err)
        Right connectingStops -> do
          Sqlite.withStatement database "DELETE FROM connecting_stops" \statement -> do
            _ <- Sqlite.stepNoCB statement
            pure ()
          Sqlite.withStatement database "INSERT INTO connecting_stops VALUES (?, ?)" \statement ->
            for_ (Map.toList connectingStops) \(stopId, connectingStopIds) ->
              for_ connectingStopIds \connectingStopId -> do
                bindTextOrNull statement 1 stopId
                bindTextOrNull statement 2 connectingStopId
                _ <- Sqlite.stepNoCB statement
                Sqlite.reset statement
                Sqlite.clearBindings statement
          case storedMd5 of
            Just _ ->
              Sqlite.withStatement database "UPDATE connecting_stops_md5 SET md5 = ?" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()
            Nothing -> do
              Sqlite.withStatement database "INSERT INTO connecting_stops_md5 VALUES (?)" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()

------------------------------------------------------------------------------------------------------------------------
-- Routes

data RoutesRow
  = RoutesRow
  { routeId :: !Text,
    agencyId :: !Int,
    routeShortName :: !Text,
    routeLongName :: !Text,
    routeDesc :: !Text,
    routeType :: !Int,
    routeUrl :: !Text,
    routeColor :: !Text,
    routeTextColor :: !Text,
    routeSortOrder :: !Int,
    routeFareClass :: !Text,
    lineId :: !Text,
    listedRoute :: !(Maybe Int),
    networkId :: !Text
  }

instance Cassava.FromNamedRecord RoutesRow where
  parseNamedRecord :: Cassava.NamedRecord -> Cassava.Parser RoutesRow
  parseNamedRecord record =
    RoutesRow
      <$> Cassava.lookup record "route_id"
      <*> Cassava.lookup record "agency_id"
      <*> Cassava.lookup record "route_short_name"
      <*> Cassava.lookup record "route_long_name"
      <*> Cassava.lookup record "route_desc"
      <*> Cassava.lookup record "route_type"
      <*> Cassava.lookup record "route_url"
      <*> Cassava.lookup record "route_color"
      <*> Cassava.lookup record "route_text_color"
      <*> Cassava.lookup record "route_sort_order"
      <*> Cassava.lookup record "route_fare_class"
      <*> Cassava.lookup record "line_id"
      <*> Cassava.lookup record "listed_route"
      <*> Cassava.lookup record "network_id"

processRoutes :: Sqlite.Database -> IO ()
processRoutes database = do
  bytes <- LazyByteString.readFile "MBTA_GTFS/routes.txt"
  let md5 = Md5.hashlazy bytes
  storedMd5 <-
    Sqlite.withStatement database "SELECT md5 FROM routes_md5" \statement -> do
      Sqlite.stepNoCB statement >>= \case
        Sqlite.Row -> Just <$> Sqlite.columnBlob statement 0
        Sqlite.Done -> pure Nothing

  if Just md5 == storedMd5
    then Text.putStrLn "routes table in mbta_gtfs.sqlite is up-to-date with MBTA_GTFS/routes.txt"
    else do
      Text.putStrLn "Insertint MBTA_GTFS/routes.txt into mbta_gtfs.sqlite"
      case Cassava.decodeByName @RoutesRow bytes of
        Left err -> error ("bad header: " ++ err)
        Right (_header, records) -> do
          Sqlite.withStatement database "DELETE FROM routes" \statement -> do
            _ <- Sqlite.stepNoCB statement
            pure ()
          Sqlite.withStatement database "INSERT INTO routes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" \statement ->
            forRecords records \route -> do
              bindTextOrNull statement 1 route.routeId
              Sqlite.bindInt statement 2 route.agencyId
              bindTextOrNull statement 3 route.routeShortName
              bindTextOrNull statement 4 route.routeLongName
              bindTextOrNull statement 5 route.routeDesc
              Sqlite.bindInt statement 6 route.routeType
              bindTextOrNull statement 7 route.routeUrl
              bindTextOrNull statement 8 route.routeColor
              bindTextOrNull statement 9 route.routeTextColor
              Sqlite.bindInt statement 10 route.routeSortOrder
              bindTextOrNull statement 11 route.routeFareClass
              bindTextOrNull statement 12 route.lineId
              bindMaybeInt statement 13 route.listedRoute
              bindTextOrNull statement 14 route.networkId
              _ <- Sqlite.stepNoCB statement
              Sqlite.reset statement
              Sqlite.clearBindings statement
          case storedMd5 of
            Just _ ->
              Sqlite.withStatement database "UPDATE routes_md5 SET md5 = ?" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()
            Nothing -> do
              Sqlite.withStatement database "INSERT INTO routes_md5 VALUES (?)" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()

------------------------------------------------------------------------------------------------------------------------
-- Stops

data StopsRow
  = StopsRow
  { stopId :: !Text,
    stopCode :: !Text,
    stopName :: !Text,
    stopDesc :: !Text,
    platformCode :: !Text,
    platformName :: !Text,
    stopLat :: !(Maybe Double),
    stopLon :: !(Maybe Double),
    zoneId :: !Text,
    stopAddress :: !Text,
    stopUrl :: !Text,
    levelId :: !Text,
    locationType :: !Int,
    parentStation :: !Text,
    wheelchairBoarding :: !Int,
    municipality :: !Text,
    onStreet :: !Text,
    atStreet :: !Text,
    vehicleType :: !(Maybe Int)
  }

instance Cassava.FromNamedRecord StopsRow where
  parseNamedRecord :: Cassava.NamedRecord -> Cassava.Parser StopsRow
  parseNamedRecord record =
    StopsRow
      <$> Cassava.lookup record "stop_id"
      <*> Cassava.lookup record "stop_code"
      <*> Cassava.lookup record "stop_name"
      <*> Cassava.lookup record "stop_desc"
      <*> Cassava.lookup record "platform_code"
      <*> Cassava.lookup record "platform_name"
      <*> Cassava.lookup record "stop_lat"
      <*> Cassava.lookup record "stop_lon"
      <*> Cassava.lookup record "zone_id"
      <*> Cassava.lookup record "stop_address"
      <*> Cassava.lookup record "stop_url"
      <*> Cassava.lookup record "level_id"
      <*> Cassava.lookup record "location_type"
      <*> Cassava.lookup record "parent_station"
      <*> Cassava.lookup record "wheelchair_boarding"
      <*> Cassava.lookup record "municipality"
      <*> Cassava.lookup record "on_street"
      <*> Cassava.lookup record "at_street"
      <*> Cassava.lookup record "vehicle_type"

processStops :: Sqlite.Database -> IO ()
processStops database = do
  bytes <- LazyByteString.readFile "MBTA_GTFS/stops.txt"
  let md5 = Md5.hashlazy bytes
  storedMd5 <-
    Sqlite.withStatement database "SELECT md5 FROM stops_md5" \statement -> do
      Sqlite.stepNoCB statement >>= \case
        Sqlite.Row -> Just <$> Sqlite.columnBlob statement 0
        Sqlite.Done -> pure Nothing

  if Just md5 == storedMd5
    then Text.putStrLn "stops table in mbta_gtfs.sqlite is up-to-date with MBTA_GTFS/stops.txt"
    else do
      Text.putStrLn "Inserting MBTA_GTFS/stops.txt into mbta_gtfs.sqlite"
      case Cassava.decodeByName @StopsRow bytes of
        Left err -> error ("bad header: " ++ err)
        Right (_header, records) -> do
          Sqlite.withStatement database "DELETE FROM stops" \statement -> do
            _ <- Sqlite.stepNoCB statement
            pure ()
          Sqlite.withStatement database "INSERT INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" \statement ->
            forRecords records \stop -> do
              bindTextOrNull statement 1 stop.stopId
              bindTextOrNull statement 2 stop.stopCode
              bindTextOrNull statement 3 stop.stopName
              bindTextOrNull statement 4 stop.stopDesc
              bindTextOrNull statement 5 stop.platformCode
              bindTextOrNull statement 6 stop.platformName
              bindMaybeDouble statement 7 stop.stopLat
              bindMaybeDouble statement 8 stop.stopLon
              bindTextOrNull statement 9 stop.zoneId
              bindTextOrNull statement 10 stop.stopAddress
              bindTextOrNull statement 11 stop.stopUrl
              bindTextOrNull statement 12 stop.levelId
              Sqlite.bindInt statement 13 stop.locationType
              bindTextOrNull statement 14 stop.parentStation
              Sqlite.bindInt statement 15 stop.wheelchairBoarding
              bindTextOrNull statement 16 stop.municipality
              bindTextOrNull statement 17 stop.onStreet
              bindTextOrNull statement 18 stop.atStreet
              bindMaybeInt statement 19 stop.vehicleType
              _ <- Sqlite.stepNoCB statement
              Sqlite.reset statement
              Sqlite.clearBindings statement
          case storedMd5 of
            Just _ ->
              Sqlite.withStatement database "UPDATE stops_md5 SET md5 = ?" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()
            Nothing -> do
              Sqlite.withStatement database "INSERT INTO stops_md5 VALUES (?)" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()

------------------------------------------------------------------------------------------------------------------------
-- Stop times

data StopTimesRow = StopTimesRow
  { tripId :: !Text,
    arrivalTime :: !Text,
    departureTime :: !Text,
    stopId :: !Text,
    stopSequence :: !Int,
    stopHeadsign :: !Text,
    pickupType :: !Int,
    dropOffType :: !Int,
    timepoint :: !Int,
    checkpointId :: !Text,
    continuousPickup :: !(Maybe Int),
    continuousDropoff :: !(Maybe Int)
  }

instance Cassava.FromNamedRecord StopTimesRow where
  parseNamedRecord :: Cassava.NamedRecord -> Cassava.Parser StopTimesRow
  parseNamedRecord record =
    StopTimesRow
      <$> Cassava.lookup record "trip_id"
      <*> Cassava.lookup record "arrival_time"
      <*> Cassava.lookup record "departure_time"
      <*> Cassava.lookup record "stop_id"
      <*> Cassava.lookup record "stop_sequence"
      <*> Cassava.lookup record "stop_headsign"
      <*> Cassava.lookup record "pickup_type"
      <*> Cassava.lookup record "drop_off_type"
      <*> Cassava.lookup record "timepoint"
      <*> Cassava.lookup record "checkpoint_id"
      <*> Cassava.lookup record "continuous_pickup"
      <*> Cassava.lookup record "continuous_drop_off"

processStopTimes :: Sqlite.Database -> IO ()
processStopTimes database = do
  bytes <- LazyByteString.readFile "MBTA_GTFS/stop_times.txt"
  let md5 = Md5.hashlazy bytes
  storedMd5 <-
    Sqlite.withStatement database "SELECT md5 FROM stop_times_md5" \statement -> do
      Sqlite.stepNoCB statement >>= \case
        Sqlite.Row -> Just <$> Sqlite.columnBlob statement 0
        Sqlite.Done -> pure Nothing

  if Just md5 == storedMd5
    then Text.putStrLn "stop_times table in mbta_gtfs.sqlite is up-to-date with MBTA_GTFS/stop_times.txt"
    else do
      Text.putStrLn "Inserting MBTA_GTFS/stop_times.txt into mbta_gtfs.sqlite"
      case Cassava.decodeByName @StopTimesRow bytes of
        Left err -> error ("bad header: " ++ err)
        Right (_header, records) -> do
          Sqlite.withStatement database "DELETE FROM stop_times" \statement -> do
            _ <- Sqlite.stepNoCB statement
            pure ()
          Sqlite.withStatement database "INSERT INTO stop_times VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" \statement ->
            forRecords records \stopTime -> do
              bindTextOrNull statement 1 stopTime.tripId
              bindTextOrNull statement 2 stopTime.arrivalTime
              bindTextOrNull statement 3 stopTime.departureTime
              bindTextOrNull statement 4 stopTime.stopId
              Sqlite.bindInt statement 5 stopTime.stopSequence
              bindTextOrNull statement 6 stopTime.stopHeadsign
              Sqlite.bindInt statement 7 stopTime.pickupType
              Sqlite.bindInt statement 8 stopTime.dropOffType
              Sqlite.bindInt statement 9 stopTime.timepoint
              bindTextOrNull statement 10 stopTime.checkpointId
              bindMaybeInt statement 11 stopTime.continuousPickup
              bindMaybeInt statement 12 stopTime.continuousDropoff
              _ <- Sqlite.stepNoCB statement
              Sqlite.reset statement
              Sqlite.clearBindings statement
          case storedMd5 of
            Just _ ->
              Sqlite.withStatement database "UPDATE stop_times_md5 SET md5 = ?" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()
            Nothing -> do
              Sqlite.withStatement database "INSERT INTO stop_times_md5 VALUES (?)" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()

------------------------------------------------------------------------------------------------------------------------
-- Trips

data TripsRow
  = TripsRow
  { routeId :: !Text,
    serviceId :: !Text,
    tripId :: !Text,
    tripHeadsign :: !Text,
    tripShortName :: !Text,
    directionId :: !Int,
    blockId :: !Text,
    shapeId :: !Text,
    wheelchairAccessible :: !Int,
    tripRouteType :: !(Maybe Int),
    routePatternId :: !Text,
    bikesAllowed :: !Int
  }

instance Cassava.FromNamedRecord TripsRow where
  parseNamedRecord :: Cassava.NamedRecord -> Cassava.Parser TripsRow
  parseNamedRecord record =
    TripsRow
      <$> Cassava.lookup record "route_id"
      <*> Cassava.lookup record "service_id"
      <*> Cassava.lookup record "trip_id"
      <*> Cassava.lookup record "trip_headsign"
      <*> Cassava.lookup record "trip_short_name"
      <*> Cassava.lookup record "direction_id"
      <*> Cassava.lookup record "block_id"
      <*> Cassava.lookup record "shape_id"
      <*> Cassava.lookup record "wheelchair_accessible"
      <*> Cassava.lookup record "trip_route_type"
      <*> Cassava.lookup record "route_pattern_id"
      <*> Cassava.lookup record "bikes_allowed"

processTrips :: Sqlite.Database -> IO ()
processTrips database = do
  bytes <- LazyByteString.readFile "MBTA_GTFS/trips.txt"
  let md5 = Md5.hashlazy bytes
  storedMd5 <-
    Sqlite.withStatement database "SELECT md5 FROM trips_md5" \statement -> do
      Sqlite.stepNoCB statement >>= \case
        Sqlite.Row -> Just <$> Sqlite.columnBlob statement 0
        Sqlite.Done -> pure Nothing

  if Just md5 == storedMd5
    then Text.putStrLn "trips table in mbta_gtfs.sqlite is up-to-date with MBTA_GTFS/trips.txt"
    else do
      Text.putStrLn "Inserting MBTA_GTFS/trips.txt into mbta_gtfs.sqlite"
      case Cassava.decodeByName @TripsRow bytes of
        Left err -> error ("bad header: " ++ err)
        Right (_header, records) -> do
          Sqlite.withStatement database "DELETE FROM trips" \statement -> do
            _ <- Sqlite.stepNoCB statement
            pure ()
          Sqlite.withStatement database "INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" \statement ->
            forRecords records \trip -> do
              bindTextOrNull statement 1 trip.routeId
              bindTextOrNull statement 2 trip.serviceId
              bindTextOrNull statement 3 trip.tripId
              bindTextOrNull statement 4 trip.tripHeadsign
              bindTextOrNull statement 5 trip.tripShortName
              Sqlite.bindInt statement 6 trip.directionId
              bindTextOrNull statement 7 trip.blockId
              bindTextOrNull statement 8 trip.shapeId
              Sqlite.bindInt statement 9 trip.wheelchairAccessible
              bindMaybeInt statement 10 trip.tripRouteType
              bindTextOrNull statement 11 trip.routePatternId
              Sqlite.bindInt statement 12 trip.bikesAllowed
              _ <- Sqlite.stepNoCB statement
              Sqlite.reset statement
              Sqlite.clearBindings statement
          case storedMd5 of
            Just _ ->
              Sqlite.withStatement database "UPDATE trips_md5 SET md5 = ?" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()
            Nothing -> do
              Sqlite.withStatement database "INSERT INTO trips_md5 VALUES (?)" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()

------------------------------------------------------------------------------------------------------------------------
-- Stop routes and connecting routes

processStopConnectingRoutes :: Sqlite.Database -> IO ()
processStopConnectingRoutes database = do
  let drawRows :: Sqlite.Statement -> IO (Map Text [(Text, Text, Int)])
      drawRows statement =
        let loop acc =
              Sqlite.stepNoCB statement >>= \case
                Sqlite.Row -> do
                  stopId <- Sqlite.columnText statement 0
                  routeId <- Sqlite.columnText statement 1
                  routeShortName <-
                    Sqlite.columnType statement 2 >>= \case
                      Sqlite.NullColumn -> pure ""
                      _ -> Sqlite.columnText statement 2
                  routeType <- Sqlite.columnInt64 statement 3
                  loop $!
                    Map.alter
                      ( let value = (routeId, routeShortName, fromIntegral @Int64 @Int routeType)
                         in \case
                              Nothing -> Just [value]
                              Just values -> Just (value : values)
                      )
                      stopId
                      acc
                Sqlite.Done -> pure acc
         in loop Map.empty

  stopRoutes0 <- Sqlite.withStatement database stopRoutesQuery drawRows
  stopConnectingRoutes0 <- Sqlite.withStatement database stopConnectingRoutesQuery drawRows

  let stopRoutesAndConnectingRoutes :: Map Text ([(Text, Text, Int)], [(Text, Text, Int)])
      stopRoutesAndConnectingRoutes =
        Map.merge
          (Map.mapMissing \_ -> (,[]))
          (Map.mapMissing \_ -> ([],))
          (Map.zipWithMatched \_ -> (,))
          stopRoutes0
          stopConnectingRoutes0

  let value =
        Cretheus.Encode.asValue $
          Cretheus.Encode.map
            Aeson.Key.fromText
            ( \(routes, connectingRoutes) ->
                Cretheus.Encode.object
                  [ optionalListPropertyEncoder "routes" routeInfoEncoder routes,
                    optionalListPropertyEncoder "connecting_routes" routeInfoEncoder connectingRoutes
                  ]
            )
            stopRoutesAndConnectingRoutes

  let filename = "data/stop-connecting-routes.json"
  LazyByteString.writeFile filename (Aeson.Pretty.encodePretty value)
  Text.putStrLn ("Wrote " <> Text.pack filename)
  pure ()
  where
    optionalListPropertyEncoder :: Aeson.Key -> (a -> Cretheus.Encoding) -> [a] -> Cretheus.PropertyEncoding
    optionalListPropertyEncoder name f xs =
      Cretheus.Encode.optionalProperty
        name
        case xs of
          [] -> Nothing
          _ -> Just (Cretheus.Encode.list f xs)

    routeInfoEncoder :: (Text, Text, Int) -> Cretheus.Encoding
    routeInfoEncoder (routeId, shortName, type_) =
      Cretheus.Encode.object
        [ Cretheus.Encode.property "id" (Cretheus.Encode.text routeId),
          Cretheus.Encode.property "short_name" (Cretheus.Encode.text shortName),
          Cretheus.Encode.property "type" (Cretheus.Encode.int type_)
        ]

stopRoutesQuery :: Text
stopRoutesQuery =
  [NeatInterpolation.text|
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
  |]

stopConnectingRoutesQuery :: Text
stopConnectingRoutesQuery =
  [NeatInterpolation.text|
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
  |]

------------------------------------------------------------------------------------------------------------------------
-- Misc

forRecords :: Cassava.Records a -> (a -> IO ()) -> IO ()
forRecords records0 f =
  let loop n = \case
        Cassava.Cons (Right record) records1 -> do
          when (mod n 1000 == 0) do
            Text.putStrLn ("Processing record " <> Text.pack (show n))
          f record
          loop (n + 1) records1
        Cassava.Nil Nothing _ -> pure ()
        Cassava.Cons (Left err) _ -> error err
        Cassava.Nil (Just err) _ -> error err
   in loop (1 :: Int) records0

bindMaybeInt :: Sqlite.Statement -> Sqlite.ParamIndex -> Maybe Int -> IO ()
bindMaybeInt statement index = \case
  Just n -> Sqlite.bindInt statement index n
  Nothing -> Sqlite.bindNull statement index

bindMaybeDouble :: Sqlite.Statement -> Sqlite.ParamIndex -> Maybe Double -> IO ()
bindMaybeDouble statement index = \case
  Just n -> Sqlite.bindDouble statement index n
  Nothing -> Sqlite.bindNull statement index

bindTextOrNull :: Sqlite.Statement -> Sqlite.ParamIndex -> Text -> IO ()
bindTextOrNull statement index text
  | Text.null text = Sqlite.bindNull statement index
  | otherwise = Sqlite.bindText statement index text
