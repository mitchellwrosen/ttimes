import Data.ByteString.Lazy qualified as LazyByteString
import Data.Csv qualified as Cassava (FromNamedRecord (..), lookup)
import Data.Csv.Streaming qualified as Cassava
import Data.Foldable (for_)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Database.SQLite3 qualified as Sqlite

data StopsRow
  = StopsRow
      !Text
      !Text
      !Text
      !Text
      !Text
      !Text
      !Double
      !Double
      !Text
      !Text
      !Text
      !Text
      !Int
      !Text
      !Int
      !Text
      !Text
      !Text
      !Int

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
  Sqlite.withDatabase "mbta_gtfs.sqlite" \database -> do
    Text.putStrLn "Inserting MBTA_GTFS/stops.txt into mbta_gtfs.sqlite"
    bytes <- LazyByteString.readFile "MBTA_GTFS/stops.txt"
    case Cassava.decodeByName @StopsRow bytes of
      Left err -> error ("bad header: " ++ err)
      Right (_header, records) -> do
        Sqlite.withStatement database "DELETE FROM stops" \statement -> do
          _ <- Sqlite.stepNoCB statement
          pure ()
        Sqlite.withStatement database "INSERT INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" \statement ->
          for_ records \(StopsRow stopId stopCode stopName stopDesc platformCode platformName stopLat stopLon zoneId stopAddress stopUrl levelId locationType parentStation wheelchairBoarding municipality onStreet atStreet vehicleType) -> do
            bindTextOrNull statement 1 stopId
            bindTextOrNull statement 2 stopCode
            bindTextOrNull statement 3 stopName
            bindTextOrNull statement 4 stopDesc
            bindTextOrNull statement 5 platformCode
            bindTextOrNull statement 6 platformName
            Sqlite.bindDouble statement 7 stopLat
            Sqlite.bindDouble statement 8 stopLon
            bindTextOrNull statement 9 zoneId
            bindTextOrNull statement 10 stopAddress
            bindTextOrNull statement 11 stopUrl
            bindTextOrNull statement 12 levelId
            Sqlite.bindInt statement 13 locationType
            bindTextOrNull statement 14 parentStation
            Sqlite.bindInt statement 15 wheelchairBoarding
            bindTextOrNull statement 16 municipality
            bindTextOrNull statement 17 onStreet
            bindTextOrNull statement 18 atStreet
            Sqlite.bindInt statement 19 vehicleType
            _ <- Sqlite.stepNoCB statement
            Sqlite.reset statement
            Sqlite.clearBindings statement

bindTextOrNull :: Sqlite.Statement -> Sqlite.ParamIndex -> Text -> IO ()
bindTextOrNull statement index text
  | Text.null text = Sqlite.bindNull statement index
  | otherwise = Sqlite.bindText statement index text
