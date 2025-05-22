{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Monad (when)
import Cretheus.Decode qualified
import Cretheus.Decode qualified as Cretheus (Decoder)
import Cretheus.Encode qualified
import Cretheus.Encode qualified as Cretheus (Encoding)
import Data.Aeson.Encode.Pretty qualified as Aeson.Pretty
import Data.Aeson.Key qualified as Aeson.Key
import Data.ByteString qualified as ByteString
import Data.ByteString.Builder qualified as ByteString.Builder
import Data.ByteString.Lazy qualified as ByteString.Lazy
import Data.Foldable (fold)
import Data.Foldable qualified as Foldable
import Data.Function ((&))
import Data.List qualified as List
import Data.Map (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Network.HTTP.Client qualified as Http
import Network.HTTP.Client.TLS qualified as Http.Tls
import Network.HTTP.Types qualified as Http
import System.Environment qualified as Environment
import System.Exit (exitFailure)

connectingStopsFilename :: FilePath
connectingStopsFilename = "data/connecting-stops.json"

connectingStopsLastModifiedFilename :: FilePath
connectingStopsLastModifiedFilename = "data/connecting-stops-last-modified.txt"

stopsFilename :: FilePath
stopsFilename = "data/stops.json"

stopsLastModifiedFilename :: FilePath
stopsLastModifiedFilename = "data/stops-last-modified.txt"

stopRoutesFilename :: FilePath
stopRoutesFilename = "data/stop-routes.json"

stopConnectingRoutesFilename :: FilePath
stopConnectingRoutesFilename = "data/stop-connecting-routes.json"

routesFilename :: FilePath
routesFilename = "data/routes.json"

routesLastModifiedFilename :: FilePath
routesLastModifiedFilename = "data/routes-last-modified.txt"

main :: IO ()
main = do
  let step description action = do
        Text.putStrLn description
        answer <- getLine
        when (List.take 1 answer == "y" || List.take 1 answer == "Y") action
  step "Get all stop info, including their connecting stops?" getStops
  step "Get connecting stop info, a subset of all stop info?" getConnectingStops
  step "Get all route info?" getRoutes
  step "For each stop, get the routes that go through it?" getRoutesForAllStops
  step "Write out stop connecting routes file with stops and stop route info?" writeConnectingRoutesForAllStops

getStops :: IO ()
getStops = do
  mbtaApiKey <- Text.pack <$> Environment.getEnv "MBTA_API_KEY"
  httpManager <- Http.newManager Http.Tls.tlsManagerSettings

  maybeLastModified <-
    (Just <$> ByteString.readFile stopsLastModifiedFilename) <|> pure Nothing

  let request :: Http.Request
      request =
        Http.defaultRequest
          { Http.host = Text.encodeUtf8 "api-v3.mbta.com",
            Http.method = Http.methodGet,
            Http.path =
              Http.encodePathSegments ["stops"]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.port = 443,
            Http.queryString =
              Http.renderQueryText
                True
                [ ("include", Just "connecting_stops")
                ]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.requestHeaders =
              ("X-API-Key", Text.encodeUtf8 mbtaApiKey)
                : case maybeLastModified of
                  Nothing -> []
                  Just lastModified -> [(Http.hIfModifiedSince, lastModified)],
            Http.secure = True
          }

  response <- Http.httpLbs request httpManager

  case Http.statusCode (Http.responseStatus response) of
    200 -> do
      case Cretheus.Decode.fromLazyBytes Cretheus.Decode.value (Http.responseBody response) of
        Left err -> do
          Text.putStrLn ("JSON parse failure: " <> err)
          exitFailure
        Right value -> do
          ByteString.Lazy.writeFile stopsFilename (Aeson.Pretty.encodePretty value)
          Text.putStrLn ("Wrote " <> Text.pack stopsFilename)
          case lookup Http.hLastModified (Http.responseHeaders response) of
            Nothing -> pure ()
            Just lastModified -> do
              ByteString.writeFile stopsLastModifiedFilename lastModified
              Text.putStrLn ("Wrote " <> Text.pack stopsLastModifiedFilename)
    304 -> Text.putStrLn (Text.pack stopsFilename <> " is up-to-date.")
    code -> do
      Text.putStrLn ("Unexpected response code: " <> Text.pack (show code))
      exitFailure

getConnectingStops :: IO ()
getConnectingStops = do
  mbtaApiKey <- Text.pack <$> Environment.getEnv "MBTA_API_KEY"
  httpManager <- Http.newManager Http.Tls.tlsManagerSettings

  maybeLastModified <-
    (Just <$> ByteString.readFile connectingStopsLastModifiedFilename) <|> pure Nothing

  let request :: Http.Request
      request =
        Http.defaultRequest
          { Http.host = Text.encodeUtf8 "api-v3.mbta.com",
            Http.method = Http.methodGet,
            Http.path =
              Http.encodePathSegments ["stops"]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.port = 443,
            Http.queryString =
              Http.renderQueryText
                True
                [ ("fields[stop]", Nothing),
                  ("include", Just "connecting_stops")
                ]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.requestHeaders =
              ("X-API-Key", Text.encodeUtf8 mbtaApiKey)
                : case maybeLastModified of
                  Nothing -> []
                  Just lastModified -> [(Http.hIfModifiedSince, lastModified)],
            Http.secure = True
          }

  response <- Http.httpLbs request httpManager

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
          ByteString.Lazy.writeFile connectingStopsFilename (Aeson.Pretty.encodePretty connectingStops)
          Text.putStrLn ("Wrote " <> Text.pack connectingStopsFilename)
          case lookup Http.hLastModified (Http.responseHeaders response) of
            Nothing -> pure ()
            Just lastModified -> do
              ByteString.writeFile connectingStopsLastModifiedFilename lastModified
              Text.putStrLn ("Wrote " <> Text.pack connectingStopsLastModifiedFilename)
    304 -> Text.putStrLn (Text.pack connectingStopsFilename <> " is up-to-date.")
    code -> do
      Text.putStrLn ("Unexpected response code: " <> Text.pack (show code))
      exitFailure

getRoutes :: IO ()
getRoutes = do
  mbtaApiKey <- Text.pack <$> Environment.getEnv "MBTA_API_KEY"
  httpManager <- Http.newManager Http.Tls.tlsManagerSettings

  maybeLastModified <-
    (Just <$> ByteString.readFile routesLastModifiedFilename) <|> pure Nothing

  let request :: Http.Request
      request =
        Http.defaultRequest
          { Http.host = Text.encodeUtf8 "api-v3.mbta.com",
            Http.method = Http.methodGet,
            Http.path =
              Http.encodePathSegments ["routes"]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.port = 443,
            Http.requestHeaders =
              ("X-API-Key", Text.encodeUtf8 mbtaApiKey)
                : case maybeLastModified of
                  Nothing -> []
                  Just lastModified -> [(Http.hIfModifiedSince, lastModified)],
            Http.secure = True
          }

  response <- Http.httpLbs request httpManager

  case Http.statusCode (Http.responseStatus response) of
    200 -> do
      case Cretheus.Decode.fromLazyBytes Cretheus.Decode.value (Http.responseBody response) of
        Left err -> do
          Text.putStrLn ("JSON parse failure: " <> err)
          exitFailure
        Right value -> do
          ByteString.Lazy.writeFile routesFilename (Aeson.Pretty.encodePretty value)
          Text.putStrLn ("Wrote " <> Text.pack routesFilename)
          case lookup Http.hLastModified (Http.responseHeaders response) of
            Nothing -> pure ()
            Just lastModified -> do
              ByteString.writeFile routesLastModifiedFilename lastModified
              Text.putStrLn ("Wrote " <> Text.pack routesLastModifiedFilename)
    304 -> Text.putStrLn (Text.pack routesFilename <> " is up-to-date.")
    code -> do
      Text.putStrLn ("Unexpected response code: " <> Text.pack (show code))
      exitFailure

getRoutesForAllStops :: IO ()
getRoutesForAllStops = do
  mbtaApiKey <- Text.pack <$> Environment.getEnv "MBTA_API_KEY"

  stopsJson <- ByteString.readFile stopsFilename
  let decoder :: Cretheus.Decoder [Text]
      decoder =
        Cretheus.Decode.object $
          Cretheus.Decode.property "data" $
            Cretheus.Decode.list $
              Cretheus.Decode.object $
                Cretheus.Decode.property "id" Cretheus.Decode.text
  case Cretheus.Decode.fromBytes decoder stopsJson of
    Left err -> do
      Text.putStrLn ("Error parsing " <> Text.pack stopsFilename <> ": " <> err)
      exitFailure
    Right stops -> do
      httpManager <- Http.newManager Http.Tls.tlsManagerSettings

      routes <- do
        let numStops = length stops
        Foldable.foldlM
          ( \acc (i, stopId) -> do
              Text.putStrLn (Text.pack (show i) <> "/" <> Text.pack (show numStops) <> " Fetching routes for stop " <> stopId)
              routes <- getRoutesForStop mbtaApiKey httpManager stopId
              threadDelay 200_000
              pure $! Map.insert stopId routes acc
          )
          Map.empty
          (zip [(1 :: Int) ..] stops)

      let value =
            Cretheus.Encode.asValue $
              Cretheus.Encode.map
                Aeson.Key.fromText
                (Cretheus.Encode.list routeInfoEncoder)
                routes

      ByteString.Lazy.writeFile stopRoutesFilename (Aeson.Pretty.encodePretty value)
      Text.putStrLn ("Wrote " <> Text.pack stopRoutesFilename)

getRoutesForStop :: Text -> Http.Manager -> Text -> IO [(Text, Text, Int)]
getRoutesForStop mbtaApiKey httpManager stopId = do
  let request :: Http.Request
      request =
        baseRequest
          { Http.queryString =
              Http.renderQueryText
                True
                [ ("fields[route]", Just "short_name,type"),
                  ("filter[stop]", Just stopId)
                ]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict
          }

  let loop :: Int -> IO [(Text, Text, Int)]
      loop n
        | n >= 10 = do
            Text.putStrLn "Giving up after 10 tries."
            exitFailure
        | otherwise = do
            response <- Http.httpLbs request httpManager

            case Http.statusCode (Http.responseStatus response) of
              200 -> do
                let routeDecoder :: Cretheus.Decoder (Text, Text, Int)
                    routeDecoder =
                      Cretheus.Decode.object do
                        routeId <- Cretheus.Decode.property "id" Cretheus.Decode.text
                        (shortName, type_) <-
                          Cretheus.Decode.property
                            "attributes"
                            ( Cretheus.Decode.object do
                                shortName <- Cretheus.Decode.property "short_name" Cretheus.Decode.text
                                type_ <- Cretheus.Decode.property "type" Cretheus.Decode.int
                                pure (shortName, type_)
                            )
                        pure (routeId, shortName, type_)
                let decoder :: Cretheus.Decoder [(Text, Text, Int)]
                    decoder =
                      Cretheus.Decode.object (Cretheus.Decode.property "data" (Cretheus.Decode.list routeDecoder))
                case Cretheus.Decode.fromLazyBytes decoder (Http.responseBody response) of
                  Left err -> do
                    Text.putStrLn ("JSON parse failure: " <> err)
                    exitFailure
                  Right routes -> pure routes
              code -> do
                Text.putStrLn ("Unexpected response code: " <> Text.pack (show code))
                threadDelay 1_000_000
                loop (n + 1)

  loop 0
  where
    baseRequest =
      Http.defaultRequest
        { Http.host = Text.encodeUtf8 "api-v3.mbta.com",
          Http.method = Http.methodGet,
          Http.path =
            Http.encodePathSegments ["routes"]
              & ByteString.Builder.toLazyByteString
              & ByteString.Lazy.toStrict,
          Http.port = 443,
          Http.requestHeaders = [("X-API-Key", Text.encodeUtf8 mbtaApiKey)],
          Http.secure = True
        }

writeConnectingRoutesForAllStops :: IO ()
writeConnectingRoutesForAllStops = do
  stops0 <- do
    json <- ByteString.readFile stopsFilename
    let decoder :: Cretheus.Decoder [(Text, [Text], Maybe Text)]
        decoder =
          Cretheus.Decode.object $
            Cretheus.Decode.property "data" $
              Cretheus.Decode.list $
                Cretheus.Decode.object do
                  stopId <- Cretheus.Decode.property "id" Cretheus.Decode.text
                  (connectingStops, parentStation) <-
                    Cretheus.Decode.property "relationships" $
                      Cretheus.Decode.object do
                        connectingStops <-
                          Cretheus.Decode.property "connecting_stops" $
                            Cretheus.Decode.object $
                              Cretheus.Decode.property "data" $
                                Cretheus.Decode.list $
                                  Cretheus.Decode.object $
                                    Cretheus.Decode.property "id" Cretheus.Decode.text
                        parentStation <-
                          Cretheus.Decode.property "parent_station" $
                            Cretheus.Decode.object $
                              Cretheus.Decode.property "data" $
                                Cretheus.Decode.nullable $
                                  Cretheus.Decode.object $
                                    Cretheus.Decode.property "id" Cretheus.Decode.text
                        pure (connectingStops, parentStation)
                  pure (stopId, connectingStops, parentStation)
    case Cretheus.Decode.fromBytes decoder json of
      Left err -> do
        Text.putStrLn ("Error parsing " <> Text.pack stopsFilename <> ": " <> err)
        exitFailure
      Right stops -> pure (Map.fromList (map (\(x, y, z) -> (x, (y, z))) stops))

  routes0 <- do
    json <- ByteString.readFile stopRoutesFilename
    let decoder :: Cretheus.Decoder (Map Text (Set (Text, Text, Int)))
        decoder =
          Cretheus.Decode.map
            Aeson.Key.toText
            ( Set.fromList
                <$> Cretheus.Decode.list
                  ( Cretheus.Decode.object do
                      routeId <- Cretheus.Decode.property "id" Cretheus.Decode.text
                      shortName <- Cretheus.Decode.property "short_name" Cretheus.Decode.text
                      type_ <- Cretheus.Decode.property "type" Cretheus.Decode.int
                      pure (routeId, shortName, type_)
                  )
            )
    case Cretheus.Decode.fromBytes decoder json of
      Left err -> do
        Text.putStrLn ("Error parsing " <> Text.pack stopRoutesFilename <> ": " <> err)
        exitFailure
      Right routes -> pure routes

  let stopToConnectingRoutes :: Map Text (Set (Text, Text, Int), Set (Text, Text, Int))
      stopToConnectingRoutes =
        Map.mapWithKey
          ( \myStopId (connectingStops, maybeParentStation) ->
              let myStopIds =
                    Set.singleton myStopId
                      & maybe id Set.insert maybeParentStation
                  connectingStopIds =
                    Set.union
                      (Set.fromList connectingStops)
                      ( case maybeParentStation of
                          Nothing -> Set.empty
                          Just parentStation -> maybe Set.empty (Set.fromList . fst) (Map.lookup parentStation stops0)
                      )
                  myRouteIds = fold (Map.restrictKeys routes0 myStopIds)
                  myConnectingRouteIds = Set.difference (fold (Map.restrictKeys routes0 connectingStopIds)) myRouteIds
               in (myRouteIds, myConnectingRouteIds)
          )
          stops0

  let value =
        Cretheus.Encode.asValue $
          Cretheus.Encode.map
            Aeson.Key.fromText
            ( \(myRoutes, connectingRoutes) ->
                Cretheus.Encode.object
                  [ Cretheus.Encode.optionalProperty
                      "routes"
                      if Set.null myRoutes
                        then Nothing
                        else Just (Cretheus.Encode.set routeInfoEncoder myRoutes),
                    Cretheus.Encode.optionalProperty
                      "connecting_routes"
                      if Set.null connectingRoutes
                        then Nothing
                        else Just (Cretheus.Encode.set routeInfoEncoder connectingRoutes)
                  ]
            )
            stopToConnectingRoutes

  ByteString.Lazy.writeFile stopConnectingRoutesFilename (Aeson.Pretty.encodePretty value)
  Text.putStrLn ("Wrote " <> Text.pack stopConnectingRoutesFilename)

routeInfoEncoder :: (Text, Text, Int) -> Cretheus.Encoding
routeInfoEncoder (routeId, shortName, type_) =
  Cretheus.Encode.object
    [ Cretheus.Encode.property "id" (Cretheus.Encode.text routeId),
      Cretheus.Encode.property "short_name" (Cretheus.Encode.text shortName),
      Cretheus.Encode.property "type" (Cretheus.Encode.int type_)
    ]
