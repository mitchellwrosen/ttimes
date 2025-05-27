{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Control.Applicative ((<|>))
import Control.Monad (when)
import Cretheus.Decode qualified
import Cretheus.Decode qualified as Cretheus (Decoder)
import Data.Aeson.Encode.Pretty qualified as Aeson.Pretty
import Data.ByteString qualified as ByteString
import Data.ByteString.Builder qualified as ByteString.Builder
import Data.ByteString.Lazy qualified as ByteString.Lazy
import Data.Function ((&))
import Data.List qualified as List
import Data.Map (Map)
import Data.Map.Strict qualified as Map
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

main :: IO ()
main = do
  let step description action = do
        Text.putStrLn description
        answer <- getLine
        when (List.take 1 answer == "y" || List.take 1 answer == "Y") action
  step "Get connecting stop info, a subset of all stop info?" getConnectingStops

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
