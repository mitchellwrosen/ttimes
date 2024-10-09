{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Applicative ((<|>))
import Data.Aeson qualified as Aeson
import Data.Aeson.Encode.Pretty qualified as Aeson.Pretty
import Data.ByteString qualified as ByteString
import Data.ByteString.Builder qualified as ByteString.Builder
import Data.ByteString.Lazy qualified as ByteString.Lazy
import Data.Function ((&))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Network.HTTP.Client qualified as Http
import Network.HTTP.Client.TLS qualified as Http.Tls
import Network.HTTP.Types qualified as Http
import System.Environment qualified as Environment
import System.Exit (exitFailure)

main :: IO ()
main = do
  getStops

getStops :: IO ()
getStops = do
  let stopsFilename = "data/stops.json" :: FilePath
  let stopsLastModifiedFilename = "data/stops-last-modified.txt" :: FilePath

  mbtaApiKey <- Text.pack <$> Environment.getEnv "MBTA_API_KEY"
  httpManager <- Http.newManager Http.Tls.tlsManagerSettings

  maybeLastModified <-
    (Just <$> ByteString.readFile stopsLastModifiedFilename) <|> pure Nothing

  let request :: Http.Request
      request =
        Http.defaultRequest
          { Http.method = Http.methodGet,
            Http.secure = True,
            Http.host = Text.encodeUtf8 "api-v3.mbta.com",
            Http.port = 443,
            Http.path =
              Http.encodePathSegments ["stops"]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.requestHeaders =
              ("X-API-Key", Text.encodeUtf8 mbtaApiKey)
                : case maybeLastModified of
                  Nothing -> []
                  Just lastModified -> [(Http.hIfModifiedSince, lastModified)]
          }

  response <- Http.httpLbs request httpManager

  case Http.statusCode (Http.responseStatus response) of
    200 -> do
      let body = Http.responseBody response
      case Aeson.eitherDecode @Aeson.Value body of
        Left err -> do
          Text.putStrLn ("JSON parse failure: " <> Text.pack err)
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

getRoutesForStop :: Text -> IO ()
getRoutesForStop stopId = do
  mbtaApiKey <- Text.pack <$> Environment.getEnv "MBTA_API_KEY"

  let request :: Http.Request
      request =
        Http.defaultRequest
          { Http.method = Http.methodGet,
            Http.secure = True,
            Http.host = Text.encodeUtf8 "api-v3.mbta.com",
            Http.port = 443,
            Http.path =
              Http.encodePathSegments ["routes"]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.queryString =
              Http.renderQueryText
                True
                [ ("fields[route]", Just "short_name,type"),
                  ("filter[stop]", Just stopId)
                ]
                & ByteString.Builder.toLazyByteString
                & ByteString.Lazy.toStrict,
            Http.requestHeaders = [("X-API-Key", Text.encodeUtf8 mbtaApiKey)]
          }

  pure ()
