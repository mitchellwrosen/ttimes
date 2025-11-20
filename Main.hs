import Control.Arrow ((>>>))
import Control.Exception (evaluate)
import Control.Monad (when)
import Cretheus.Decode qualified
import Cretheus.Decode qualified as Cretheus (Decoder)
import Cretheus.Encode qualified
import Cretheus.Encode qualified as Cretheus (Encoding, PropertyEncoding)
import Crypto.Hash.MD5 qualified as Md5
import Data.Aeson qualified as Aeson
import Data.Aeson.Encode.Pretty qualified as Aeson.Pretty
import Data.Aeson.Key qualified as Aeson.Key
import Data.Bits ((.&.), (.<<.), (.>>.), (.|.))
import Data.ByteString qualified as ByteString
import Data.ByteString.Builder qualified as ByteString.Builder
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Csv qualified as Cassava (FromNamedRecord (..), NamedRecord, Parser, lookup)
import Data.Csv.Streaming qualified as Cassava
import Data.Foldable (fold, for_)
import Data.Function ((&))
import Data.Int (Int64)
import Data.List qualified as List
import Data.Map.Merge.Strict qualified as Map
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Data.Text.Lazy.Builder qualified as LazyTextBuilder
import Data.Text.Lazy.Builder.RealFloat qualified as LazyTextBuilder
import Data.Text.Lazy.IO qualified as LazyText
import Database.SQLite3 qualified as Sqlite
import GHC.Clock (getMonotonicTime)
import NeatInterpolation qualified
import Network.HTTP.Client qualified as Http
import Network.HTTP.Client.TLS qualified as Http.Tls
import Network.HTTP.Types qualified as Http
import Network.HTTP.Types.Header qualified as Http
import System.Directory qualified as Directory
import System.Exit (exitFailure)
import System.Process qualified as Process

type RouteId = Text

type StopId = Text

main :: IO ()
main = do
  Sqlite.withDatabase "mbta_gtfs.sqlite" \database -> do
    Sqlite.withStatement database "BEGIN" \statement -> do
      _ <- Sqlite.stepNoCB statement
      pure ()
    downloaded <- downloadMbtaGtfsZip database
    when downloaded do
      processConnectingStops database
      processRoutes database
      processShapes database
      processStops database
      processStopTimes database
      processTrips database
      processStopConnectingRoutes database
    Sqlite.withStatement database "COMMIT" \statement -> do
      _ <- Sqlite.stepNoCB statement
      pure ()

-----------------------------------------------------------------------------------------------------------------------
-- MBTA_GTFS.zip

downloadMbtaGtfsZip :: Sqlite.Database -> IO Bool
downloadMbtaGtfsZip database = do
  storedEtag <-
    Sqlite.withStatement database "SELECT etag FROM gtfs_zip_etag" \statement -> do
      Sqlite.stepNoCB statement >>= \case
        Sqlite.Row -> Just <$> Sqlite.columnText statement 0
        Sqlite.Done -> pure Nothing
  let request :: Http.Request
      request =
        Http.defaultRequest
          { Http.host = Text.encodeUtf8 "cdn.mbta.com",
            Http.method = Http.methodGet,
            Http.path = Text.encodeUtf8 "/MBTA_GTFS.zip",
            Http.port = 443,
            Http.requestHeaders = [(Http.hIfNoneMatch, maybe ByteString.empty Text.encodeUtf8 storedEtag)],
            Http.secure = True
          }
  httpManager <- Http.newManager Http.Tls.tlsManagerSettings
  response <-
    time "Downloaded MBTA_GTFS.zip" do
      Http.httpLbs request httpManager
  case Http.statusCode (Http.responseStatus response) of
    200 -> do
      LazyByteString.writeFile "MBTA_GTFS.zip" (Http.responseBody response)
      let etag =
            case List.lookup Http.hETag (Http.responseHeaders response) of
              Just bytes -> Text.decodeUtf8 bytes
              Nothing -> Text.empty
      case storedEtag of
        Just _ ->
          Sqlite.withStatement database "UPDATE gtfs_zip_etag SET etag = ?" \statement -> do
            Sqlite.bindText statement 1 etag
            _ <- Sqlite.stepNoCB statement
            pure ()
        Nothing -> do
          Sqlite.withStatement database "INSERT INTO gtfs_zip_etag VALUES (?)" \statement -> do
            Sqlite.bindText statement 1 etag
            _ <- Sqlite.stepNoCB statement
            pure ()
      time "Unzipped MBTA_GTFS.zip to MBTA_GTFS/" do
        Process.callCommand "unzip -o MBTA_GTFS.zip -d MBTA_GTFS >/dev/null"
      pure True
    304 -> pure False
    code -> do
      Text.putStrLn ("Unexpected response code: " <> Text.pack (show code))
      exitFailure

------------------------------------------------------------------------------------------------------------------------
-- Connecting stops

processConnectingStops :: Sqlite.Database -> IO ()
processConnectingStops database = do
  httpManager <- Http.newManager Http.Tls.tlsManagerSettings

  let request :: Http.Request
      request =
        Http.defaultRequest
          { Http.host = Text.encodeUtf8 "api-v3.mbta.com",
            Http.method = Http.methodGet,
            Http.path = Text.encodeUtf8 "/stops",
            Http.port = 443,
            Http.queryString =
              Http.renderQueryText
                True
                [ ("fields[stop]", Nothing),
                  ("include", Just "connecting_stops")
                ]
                & ByteString.Builder.toLazyByteString
                & LazyByteString.toStrict,
            Http.requestHeaders = [(Http.hAcceptEncoding, "gzip")],
            Http.secure = True
          }

  response <-
    time "GET /stops" do
      Http.httpLbs request httpManager

  case Http.statusCode (Http.responseStatus response) of
    200 -> do
      let decoder :: Cretheus.Decoder [(Text, [Text])]
          decoder =
            Cretheus.Decode.object $
              Cretheus.Decode.property "data" $
                Cretheus.Decode.list $
                  Cretheus.Decode.object $
                    (,)
                      <$> Cretheus.Decode.property "id" Cretheus.Decode.text
                      <*> ( Cretheus.Decode.property "relationships" $
                              Cretheus.Decode.object do
                                Cretheus.Decode.property "connecting_stops" $
                                  Cretheus.Decode.object $
                                    Cretheus.Decode.property "data" $
                                      Cretheus.Decode.list $
                                        Cretheus.Decode.object $
                                          Cretheus.Decode.property "id" Cretheus.Decode.text
                          )
      case Cretheus.Decode.fromLazyBytes decoder (Http.responseBody response) of
        Left err -> do
          Text.putStrLn ("JSON parse failure: " <> err)
          exitFailure
        Right connectingStops0 -> do
          let connectingStops :: Map Text [Text]
              connectingStops =
                List.foldl'
                  ( \acc -> \case
                      (_, []) -> acc
                      (stop, stops) -> Map.insert stop (List.sort stops) acc
                  )
                  Map.empty
                  connectingStops0
          time "Inserted connecting stops into mbta_gtfs.sqlite" do
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
    code -> do
      Text.putStrLn ("Unexpected response code: " <> Text.pack (show code))
      exitFailure

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

  when (Just md5 /= storedMd5) do
    time "Inserted MBTA_GTFS/routes.txt into mbta_gtfs.sqlite" do
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
-- Shapes

data ShapesRow
  = ShapesRow
  { shapeId :: !Text,
    shapePtLat :: !Double,
    shapePtLon :: !Double,
    shapePtSequence :: !Int,
    shapeDistTraveled :: !(Maybe Double)
  }

instance Cassava.FromNamedRecord ShapesRow where
  parseNamedRecord :: Cassava.NamedRecord -> Cassava.Parser ShapesRow
  parseNamedRecord record =
    ShapesRow
      <$> Cassava.lookup record "shape_id"
      <*> Cassava.lookup record "shape_pt_lat"
      <*> Cassava.lookup record "shape_pt_lon"
      <*> Cassava.lookup record "shape_pt_sequence"
      <*> Cassava.lookup record "shape_dist_traveled"

processShapes :: Sqlite.Database -> IO ()
processShapes database = do
  bytes <- LazyByteString.readFile "MBTA_GTFS/shapes.txt"
  let md5 = Md5.hashlazy bytes
  storedMd5 <-
    Sqlite.withStatement database "SELECT md5 FROM shapes_md5" \statement -> do
      Sqlite.stepNoCB statement >>= \case
        Sqlite.Row -> Just <$> Sqlite.columnBlob statement 0
        Sqlite.Done -> pure Nothing

  when (Just md5 /= storedMd5) do
    time "Inserted MBTA_GTFS/shapes.txt into mbta_gtfs.sqlite" do
      case Cassava.decodeByName @ShapesRow bytes of
        Left err -> error ("bad header: " ++ err)
        Right (_header, records) -> do
          Sqlite.withStatement database "DELETE FROM shapes" \statement -> do
            _ <- Sqlite.stepNoCB statement
            pure ()
          Sqlite.withStatement database "INSERT INTO shapes VALUES (?, ?, ?, ?, ?)" \statement ->
            forRecords records \shape -> do
              Sqlite.bindText statement 1 shape.shapeId
              Sqlite.bindDouble statement 2 shape.shapePtLat
              Sqlite.bindDouble statement 3 shape.shapePtLon
              Sqlite.bindInt statement 4 shape.shapePtSequence
              bindMaybeDouble statement 5 shape.shapeDistTraveled
              _ <- Sqlite.stepNoCB statement
              Sqlite.reset statement
              Sqlite.clearBindings statement
          case storedMd5 of
            Just _ ->
              Sqlite.withStatement database "UPDATE shapes_md5 SET md5 = ?" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()
            Nothing -> do
              Sqlite.withStatement database "INSERT INTO shapes_md5 VALUES (?)" \statement -> do
                Sqlite.bindBlob statement 1 md5
                _ <- Sqlite.stepNoCB statement
                pure ()

    let shapesFilename = "data/shapes.json"

    time ("Wrote " <> shapesFilename) do
      let query =
            [NeatInterpolation.text|
              SELECT shape_id, shape_pt_lat, shape_pt_lon
              FROM shapes
              ORDER BY shape_id, shape_pt_sequence
            |]
      Sqlite.withStatement database query \statement -> do
        let loop acc =
              Sqlite.stepNoCB statement >>= \case
                Sqlite.Row -> do
                  shapeId <- Sqlite.columnText statement 0
                  shapePtLat <- Sqlite.columnDouble statement 1
                  shapePtLon <- Sqlite.columnDouble statement 2
                  loop ((shapeId, shapePtLat, shapePtLon) : acc)
                Sqlite.Done ->
                  pure do
                    List.foldl'
                      ( \shapes (shapeId, shapePtLat, shapePtLon) ->
                          Map.alter (Just . ((shapePtLat, shapePtLon) :) . fromMaybe []) shapeId shapes
                      )
                      Map.empty
                      acc
        shapes <- loop []
        shapes
          & Cretheus.Encode.map
            Aeson.Key.fromText
            ( Cretheus.Encode.list \(shapePtLat, shapePtLon) ->
                ( Cretheus.Encode.object
                    [ Cretheus.Encode.property "latitude" (Cretheus.Encode.double shapePtLat),
                      Cretheus.Encode.property "longitude" (Cretheus.Encode.double shapePtLon)
                    ]
                )
            )
          & Cretheus.Encode.asValue
          & Aeson.Pretty.encodePretty
          & LazyByteString.writeFile (Text.unpack shapesFilename)

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

  when (Just md5 /= storedMd5) do
    time "Inserted MBTA_GTFS/stops.txt into mbta_gtfs.sqlite" do
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

    let stopCoordsFilename = "data/stop-coords.json"
    let stopCoordsSnappedFilename = "data/stop-coords-snapped.json"

    time ("Wrote " <> stopCoordsFilename <> " and " <> stopCoordsSnappedFilename) do
      let query =
            [NeatInterpolation.text|
              SELECT stop_id, stop_lat, stop_lon
              FROM stops
              WHERE stop_lat IS NOT NULL
                AND stop_lon IS NOT NULL
            |]
      Sqlite.withStatement database query \statement -> do
        let loop acc =
              Sqlite.stepNoCB statement >>= \case
                Sqlite.Row -> do
                  stopId <- Sqlite.columnText statement 0
                  stopLat <- Sqlite.columnDouble statement 1
                  stopLon <- Sqlite.columnDouble statement 2
                  loop ((stopId, stopLat, stopLon) : acc)
                Sqlite.Done -> pure (reverse acc)
        let f filename =
              Cretheus.Encode.list
                ( \(stopId, stopLat, stopLon) ->
                    Cretheus.Encode.object
                      [ Cretheus.Encode.property "id" (Cretheus.Encode.text stopId),
                        Cretheus.Encode.property "latitude" (Cretheus.Encode.double stopLat),
                        Cretheus.Encode.property "longitude" (Cretheus.Encode.double stopLon)
                      ]
                )
                >>> Cretheus.Encode.asValue
                >>> Aeson.Pretty.encodePretty
                >>> LazyByteString.writeFile (Text.unpack filename)
        stops <- loop []
        stops
          & f stopCoordsFilename
        stops
          & map
            ( \(stopId, stopLat, stopLon) ->
                let (stopLat1, stopLon1) = ungeohash (geohash stopLat stopLon)
                 in (stopId, stopLat1, stopLon1)
            )
          & f stopCoordsSnappedFilename

geohash :: Double -> Double -> Int
geohash latitude longitude =
  ((round ((latitude + 90) * 15876.0)) .<<. 23) .|. round ((longitude + 180) * 11798.167249279111)

ungeohash :: Int -> (Double, Double)
ungeohash n =
  ( fromIntegral @Int @Double (n .>>. 23) * 6.298815822625347e-5 - 89.99996850592089,
    fromIntegral @Int @Double (n .&. 8388607) * 8.475892728687176e-5 - 179.99995762053635
  )

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

  when (Just md5 /= storedMd5) do
    time "Inserted MBTA_GTFS/stop_times.txt into mbta_gtfs.sqlite" do
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

    let allUniqueTripsFilename = "data/all-unique-trips.json"

    time ("Wrote " <> allUniqueTripsFilename) do
      let query =
            [NeatInterpolation.text|
              SELECT DISTINCT trip_id, stop_id
              FROM stop_times
            |]
      Sqlite.withStatement database query \statement -> do
        let loop !acc =
              Sqlite.stepNoCB statement >>= \case
                Sqlite.Row -> do
                  tripId <- Sqlite.columnText statement 0
                  stopId <- Sqlite.columnText statement 1
                  loop (Map.alter (Just . maybe (Set.singleton stopId) (Set.insert stopId)) tripId acc)
                Sqlite.Done -> pure acc
        trips <- Set.fromList . Map.elems <$> loop Map.empty
        trips
          & Cretheus.Encode.set (Cretheus.Encode.set Cretheus.Encode.text)
          & Cretheus.Encode.asValue
          & Aeson.Pretty.encodePretty
          & LazyByteString.writeFile (Text.unpack allUniqueTripsFilename)

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

  when (Just md5 /= storedMd5) do
    time "Inserted MBTA_GTFS/trips.txt into mbta_gtfs.sqlite" do
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
  stopRoutesAndConnectingRoutes <-
    time "Computed new stop connecting routes" do
      computeStopConnectingRoutes database

  let encodeStopRoutesAndConnectingRoutes =
        Aeson.Pretty.encodePretty . Cretheus.Encode.asValue . Cretheus.Encode.map Aeson.Key.fromText f
        where
          f (routes, connectingRoutes) =
            Cretheus.Encode.object
              [ optionalListPropertyEncoder "routes" routeInfoEncoder routes,
                optionalListPropertyEncoder "connecting_routes" routeInfoEncoder connectingRoutes
              ]

  let stopConnectingRoutesFilename = "data/stop-connecting-routes.json"
  let changedStopConnectingRoutesFilename = "changed-stop-connecting-routes.json"
  let deletedStopConnectingRoutesFilename = "deleted-stop-connecting-routes.json"

  maybeOldStopRoutesAndConnectingRoutes <-
    time "Parsed old stop connecting routes" do
      Directory.doesFileExist stopConnectingRoutesFilename >>= \case
        False -> pure Nothing
        True -> fmap Just do
          bytes <- ByteString.readFile stopConnectingRoutesFilename
          let decoder :: Cretheus.Decoder (Map Text ([(Text, Text, Int)], [(Text, Text, Int)]))
              decoder =
                Cretheus.Decode.map
                  Aeson.Key.toText
                  ( Cretheus.Decode.object $
                      (,)
                        <$> ( fromMaybe []
                                <$> Cretheus.Decode.optionalProperty "routes" (Cretheus.Decode.list routeInfoDecoder)
                            )
                        <*> ( fromMaybe []
                                <$> Cretheus.Decode.optionalProperty "connecting_routes" (Cretheus.Decode.list routeInfoDecoder)
                            )
                  )
          Cretheus.Decode.fromBytes decoder bytes & onLeft \err -> do
            Text.putStrLn err
            exitFailure

  when (Just stopRoutesAndConnectingRoutes /= maybeOldStopRoutesAndConnectingRoutes) do
    LazyByteString.writeFile
      stopConnectingRoutesFilename
      (encodeStopRoutesAndConnectingRoutes stopRoutesAndConnectingRoutes)
    Text.putStrLn ("Wrote " <> Text.pack stopConnectingRoutesFilename)

    case maybeOldStopRoutesAndConnectingRoutes of
      Nothing -> do
        Directory.copyFile stopConnectingRoutesFilename changedStopConnectingRoutesFilename
        Text.putStrLn ("Wrote " <> Text.pack changedStopConnectingRoutesFilename)
      Just oldStopRoutesAndConnectingRoutes -> do
        !changedStopRoutesAndConnectingRoutes <-
          time "Computed changed stop connecting routes" do
            evaluate $
              Map.merge
                Map.dropMissing
                (Map.mapMissing (const id))
                (Map.zipWithMaybeMatched \_ old new -> if old == new then Nothing else Just new)
                oldStopRoutesAndConnectingRoutes
                stopRoutesAndConnectingRoutes
        when (not (Map.null changedStopRoutesAndConnectingRoutes)) do
          LazyByteString.writeFile
            changedStopConnectingRoutesFilename
            (encodeStopRoutesAndConnectingRoutes changedStopRoutesAndConnectingRoutes)
          Text.putStrLn ("Wrote " <> Text.pack changedStopConnectingRoutesFilename)

        !deletedStopRoutesAndConnectingRoutes <-
          time "Computed deleted stop connecting routes" do
            evaluate $
              Map.difference oldStopRoutesAndConnectingRoutes stopRoutesAndConnectingRoutes
        when (not (Map.null deletedStopRoutesAndConnectingRoutes)) do
          LazyByteString.writeFile
            deletedStopConnectingRoutesFilename
            (encodeStopRoutesAndConnectingRoutes deletedStopRoutesAndConnectingRoutes)
          Text.putStrLn ("Wrote " <> Text.pack deletedStopConnectingRoutesFilename)
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

    routeInfoDecoder :: Cretheus.Decoder (Text, Text, Int)
    routeInfoDecoder =
      Cretheus.Decode.object $
        (,,)
          <$> Cretheus.Decode.property "id" Cretheus.Decode.text
          <*> Cretheus.Decode.property "short_name" Cretheus.Decode.text
          <*> Cretheus.Decode.property "type" Cretheus.Decode.int

computeStopConnectingRoutes :: Sqlite.Database -> IO (Map StopId ([(RouteId, Text, Int)], [(RouteId, Text, Int)]))
computeStopConnectingRoutes database = do
  !allStopIds :: Set StopId <-
    Sqlite.withStatement database "SELECT stop_id FROM stops" \statement ->
      let loop !stopIds =
            Sqlite.stepNoCB statement >>= \case
              Sqlite.Done -> pure stopIds
              Sqlite.Row -> do
                stopId <- Sqlite.columnText statement 0
                loop $! Set.insert stopId stopIds
       in loop Set.empty

  !stopGraph :: Map StopId (Set StopId) <-
    Sqlite.withStatement database stopGraphQuery \statement ->
      let loop !acc =
            Sqlite.stepNoCB statement >>= \case
              Sqlite.Done -> pure acc
              Sqlite.Row -> do
                stopId <- Sqlite.columnText statement 0
                adjacentStopId <- Sqlite.columnText statement 1
                loop $!
                  Map.alter
                    (Just . maybe (Set.singleton adjacentStopId) (Set.insert adjacentStopId))
                    stopId
                    acc
       in loop Map.empty

  let adjacent stopId = Map.findWithDefault Set.empty stopId stopGraph

  let reachable stopId =
        let loop acc frontier =
              case Set.minView frontier of
                Just (otherStopId, frontier1) ->
                  if Set.member otherStopId acc
                    then loop acc frontier1
                    else loop (Set.insert otherStopId acc) (Set.union (adjacent otherStopId) frontier1)
                Nothing -> acc
         in loop Set.empty (adjacent stopId)

  !theStopRoutes :: Map StopId (Map RouteId (Text, Int, Int)) <-
    Sqlite.withStatement database stopRoutesQuery \statement -> do
      let loop !acc =
            Sqlite.stepNoCB statement >>= \case
              Sqlite.Done -> pure acc
              Sqlite.Row -> do
                stopId <- Sqlite.columnText statement 0
                routeId <- Sqlite.columnText statement 1
                routeShortName <-
                  Sqlite.columnType statement 2 >>= \case
                    Sqlite.NullColumn -> pure ""
                    _ -> Sqlite.columnText statement 2
                routeType <- Sqlite.columnInt64 statement 3
                routeSortOrder <- Sqlite.columnInt64 statement 4
                loop $!
                  Map.alter
                    ( let value =
                            ( routeShortName,
                              fromIntegral @Int64 @Int routeType,
                              fromIntegral @Int64 @Int routeSortOrder
                            )
                       in Just . \case
                            Nothing -> Map.singleton routeId value
                            Just values -> Map.insert routeId value values
                    )
                    stopId
                    acc
       in loop Map.empty

  let theStopConnectingRoutes :: Map StopId (Map RouteId (Text, Int, Int))
      !theStopConnectingRoutes =
        Map.fromSet
          ( \stopId ->
              fold (Map.restrictKeys theStopRoutes (reachable stopId))
                `Map.difference` Map.findWithDefault Map.empty stopId theStopRoutes
          )
          allStopIds

  let sortRoutes :: Map RouteId (Text, Int, Int) -> [(RouteId, Text, Int)]
      sortRoutes =
        Map.toList
          -- I think `routeSortOrder` is probably a unique sorting key, but add backup sort keys just in case
          >>> List.sortOn (\(routeId, (routeShortName, _, routeSortOrder)) -> (routeSortOrder, routeShortName, routeId))
          >>> map (\(routeId, (routeShortName, routeType, _)) -> (routeId, routeShortName, routeType))

  pure $!
    Map.merge
      (Map.mapMissing \_ routes -> (sortRoutes routes, []))
      (Map.mapMissing \_ connectingRoutes -> ([], sortRoutes connectingRoutes))
      (Map.zipWithMatched \_ routes connectingRoutes -> (sortRoutes routes, sortRoutes connectingRoutes))
      theStopRoutes
      theStopConnectingRoutes

-- First relate a stop to all of its "effective stop ids" – itself and all of its children. We therefore say a stop's
-- routes are the routes that directly pass through it or any of its children.
stopRoutesQuery :: Text
stopRoutesQuery =
  [NeatInterpolation.text|
    WITH effective_stops (stop_id, effective_stop_id) AS (
      SELECT parent_stop_id, child_stop_id
      FROM stop_children_view
      UNION ALL
      SELECT stop_id, stop_id
      FROM stops
    )
    SELECT effective_stops.stop_id, routes.route_id, routes.route_short_name, routes.route_type, routes.route_sort_order
    FROM effective_stops
      JOIN stop_routes_view ON effective_stops.effective_stop_id = stop_routes_view.stop_id
      JOIN routes ON stop_routes_view.route_id = routes.route_id
  |]

stopGraphQuery :: Text
stopGraphQuery =
  [NeatInterpolation.text|
    SELECT stop_id, parent_station
    FROM stops
    WHERE parent_station IS NOT NULL
    UNION
    SELECT parent_stop_id, child_stop_id
    FROM stop_children_view
    UNION
    SELECT stop_id, connecting_stop_id
    FROM connecting_stops
  |]

------------------------------------------------------------------------------------------------------------------------
-- Misc

time :: Text -> IO a -> IO a
time label action = do
  t0 <- getMonotonicTime
  result <- action
  t1 <- getMonotonicTime
  LazyText.putStrLn . LazyTextBuilder.toLazyText $
    "["
      <> LazyTextBuilder.formatRealFloat LazyTextBuilder.Fixed (Just 1) (t1 - t0)
      <> "s] "
      <> LazyTextBuilder.fromText label
  pure result

onLeft :: (Applicative m) => (a -> m b) -> Either a b -> m b
onLeft f = \case
  Left x -> f x
  Right y -> pure y

forRecords :: Cassava.Records a -> (a -> IO ()) -> IO ()
forRecords records0 f =
  let loop n = \case
        Cassava.Cons (Right record) records1 -> do
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
