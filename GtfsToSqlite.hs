import Control.Monad (when)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Csv qualified as Cassava (FromNamedRecord (..), lookup)
import Data.Csv.Streaming qualified as Cassava
import Data.List qualified as List
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

data StopsRow
  = StopsRow
  { stopId :: !Text,
    stopCode :: !Text,
    stopName :: !Text,
    stopDesc :: !Text,
    platformCode :: !Text,
    platformName :: !Text,
    stopLat :: !Double,
    stopLon :: !Double,
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
    vehicleType :: !Int
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

main :: IO ()
main = do
  let step description action = do
        Text.putStrLn description
        answer <- getLine
        when (List.take 1 answer == "y" || List.take 1 answer == "Y") action

  Sqlite.withDatabase "mbta_gtfs.sqlite" \database -> do
    step "Insert MBTA_GTFS/routes.txt into mbta_gtfs.sqlite?" do
      bytes <- LazyByteString.readFile "MBTA_GTFS/routes.txt"
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

    step "Insert MBTA_GTFS/stops.txt into mbta_gtfs.sqlite?" do
      bytes <- LazyByteString.readFile "MBTA_GTFS/stops.txt"
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
              Sqlite.bindDouble statement 7 stop.stopLat
              Sqlite.bindDouble statement 8 stop.stopLon
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
              Sqlite.bindInt statement 19 stop.vehicleType
              _ <- Sqlite.stepNoCB statement
              Sqlite.reset statement
              Sqlite.clearBindings statement

forRecords :: Cassava.Records a -> (a -> IO ()) -> IO ()
forRecords records0 f =
  let loop = \case
        Cassava.Cons (Right record) records1 -> do
          f record
          loop records1
        Cassava.Nil Nothing _ -> pure ()
        Cassava.Cons (Left err) _ -> error err
        Cassava.Nil (Just err) _ -> error err
   in loop records0

bindMaybeInt :: Sqlite.Statement -> Sqlite.ParamIndex -> Maybe Int -> IO ()
bindMaybeInt statement index = \case
  Just n -> Sqlite.bindInt statement index n
  Nothing -> Sqlite.bindNull statement index

bindTextOrNull :: Sqlite.Statement -> Sqlite.ParamIndex -> Text -> IO ()
bindTextOrNull statement index text
  | Text.null text = Sqlite.bindNull statement index
  | otherwise = Sqlite.bindText statement index text
