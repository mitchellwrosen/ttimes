import Control.Monad (when)
import Cretheus.Decode qualified
import Cretheus.Decode qualified as Cretheus (Decoder)
import Crypto.Hash.MD5 qualified as Md5
import Data.Aeson.Key qualified as Aeson.Key
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Csv qualified as Cassava (FromNamedRecord (..), lookup)
import Data.Csv.Streaming qualified as Cassava
import Data.Foldable (for_)
import Data.List qualified as List
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Database.SQLite3 qualified as Sqlite

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

main :: IO ()
main = do
  let step description action = do
        Text.putStrLn description
        answer <- getLine
        when (List.take 1 answer == "y" || List.take 1 answer == "Y") action

  Sqlite.withDatabase "mbta_gtfs.sqlite" \database -> do
    do
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
          Text.putStrLn "connecting_stops table in mbta_gtfs.sqlite is not up-to-date with data/connecting-stops.json"
          step "Insert data/connecting-stops.json into mbta_gtfs.sqlite?" do
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

    do
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
          Text.putStrLn "routes table in mbta_gtfs.sqlite is not up-to-date with MBTA_GTFS/routes.txt"
          step "Insert MBTA_GTFS/routes.txt into mbta_gtfs.sqlite?" do
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

    do
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
          Text.putStrLn "stop_times table in mbta_gtfs.sqlite is not up-to-date with MBTA_GTFS/stop_times.txt"
          step "Insert MBTA_GTFS/stop_times.txt into mbta_gtfs.sqlite?" do
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
    do
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
          Text.putStrLn "stops table in mbta_gtfs.sqlite is not up-to-date with MBTA_GTFS/stops.txt"
          step "Insert MBTA_GTFS/stops.txt into mbta_gtfs.sqlite?" do
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

    do
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
          Text.putStrLn "trips table in mbta_gtfs.sqlite is not up-to-date with MBTA_GTFS/trips.txt"
          step "Insert MBTA_GTFS/trips.txt into mbta_gtfs.sqlite?" do
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
