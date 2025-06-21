module Main exposing (..)

import Browser exposing (UrlRequest)
import Browser.Navigation exposing (Key)
import Html exposing (Html)
import Html.Attributes
import Html.Lazy
import Http
import Json.Decode
import Time
import Ttimes.Ports
import Url exposing (Url)
import Url.Builder


type alias Flags =
    { deviceId : String
    , window :
        { height : Int
        , width : Int
        }
    }


type Message
    = CurrentPosition
        { latitude : Float
        , longitude : Float
        , timestamp : Int
        }
    | CurrentPositionError
        { code : Int
        , message : String
        }
    | GetRoutes
        (Result
            Http.Error
            { location :
                Maybe
                    { city : String
                    , neighborhood : Maybe String
                    }
            , predictions :
                Result
                    MbtaError
                    { orsError : Maybe OrsError
                    , predictions :
                        List
                            { direction : Int
                            , route : String
                            , routeClass : Int
                            , routeName : String
                            , routePattern : Maybe String
                            , stop : String
                            , stopLocation :
                                { latitude : Float
                                , longitude : Float
                                }
                            , stopName : String
                            , times : List String
                            , trip : String
                            , tripHeadsign : String
                            , walkDuration : Float
                            }
                    }
            }
        )
    | Hidden Bool
    | Tick Time.Posix
    | UrlChange Url
    | UrlRequest UrlRequest


type MbtaError
    = NotFound
    | SomethingUnexpected String


type OrsError
    = HttpError
    | ResponseError Int
    | RateLimitExceeded
    | ServiceUnavailable


type alias Model =
    { currentPosition :
        Maybe
            { latitude : Float
            , longitude : Float
            , timestamp : Time.Posix
            }
    , deviceId : String
    , dismissableError : Maybe DismissableError
    , hidden : Bool
    , navigationKey : Key
    , routes :
        Maybe
            { location :
                Maybe
                    { city : String
                    , neighborhood : Maybe String
                    }
            , predictions :
                Result
                    MbtaError
                    { orsError : Maybe OrsError
                    , predictions :
                        List
                            { direction : Int
                            , route : String
                            , routeClass : Int
                            , routeName : String
                            , routePattern : Maybe String
                            , stop : String
                            , stopLocation :
                                { latitude : Float
                                , longitude : Float
                                }
                            , stopName : String
                            , times : List String
                            , trip : String
                            , tripHeadsign : String
                            , walkDuration : Float
                            }
                    }
            }
    , window :
        { height : Int
        , width : Int
        }
    }


type DismissableError
    = GetRoutesError Http.Error


main : Program Flags Model Message
main =
    Browser.application
        { init = init
        , onUrlChange = onUrlChange
        , onUrlRequest = onUrlRequest
        , subscriptions = subscriptions
        , update = update
        , view = view
        }


init : Flags -> Url -> Key -> ( Model, Cmd Message )
init { deviceId, window } _ navigationKey =
    ( { currentPosition = Nothing
      , deviceId = deviceId
      , dismissableError = Nothing
      , hidden = False
      , navigationKey = navigationKey
      , routes = Nothing
      , window = window
      }
    , Ttimes.Ports.watchPosition ()
    )


onUrlChange : Url -> Message
onUrlChange =
    UrlChange


onUrlRequest : UrlRequest -> Message
onUrlRequest =
    UrlRequest


subscriptions : Model -> Sub Message
subscriptions _ =
    Sub.batch
        [ Time.every 1000 Tick
        , Ttimes.Ports.currentPosition CurrentPosition
        , Ttimes.Ports.currentPositionError CurrentPositionError
        , Ttimes.Ports.hidden Hidden
        ]


update : Message -> Model -> ( Model, Cmd Message )
update message model =
    let
        _ =
            Debug.log "message" message
    in
    case message of
        CurrentPosition { latitude, longitude, timestamp } ->
            let
                model1 =
                    { model
                        | currentPosition =
                            Just
                                { latitude = latitude
                                , longitude = longitude
                                , timestamp = Time.millisToPosix timestamp
                                }
                    }

                command =
                    case model.currentPosition of
                        Just _ ->
                            Cmd.none

                        Nothing ->
                            httpGetRoutes model.deviceId latitude longitude
            in
            ( model1, command )

        CurrentPositionError _ ->
            ( model, Cmd.none )

        GetRoutes result ->
            case result of
                Ok routes ->
                    let
                        model1 =
                            { model | routes = Just routes }
                    in
                    ( model1, Cmd.none )

                Err err ->
                    let
                        model1 =
                            { model
                                | dismissableError = Just (GetRoutesError err)
                            }
                    in
                    ( model1, Cmd.none )

        Hidden hidden ->
            ( { model
                | hidden = hidden
              }
            , Cmd.none
            )

        Tick _ ->
            ( model, Cmd.none )

        UrlChange _ ->
            ( model, Cmd.none )

        UrlRequest _ ->
            ( model, Cmd.none )


view : Model -> Browser.Document Message
view =
    let
        viewBodyContent : List (Html message) -> List (Html message) -> List (Html message) -> Html message
        viewBodyContent headerBarChildren mainChildren footerChildren =
            Html.div
                [ Html.Attributes.class "align-items-center display-flex flex-direction-column"
                , Html.Attributes.id "content"
                ]
                [ Html.div
                    [ Html.Attributes.class "align-items-center background-ffffff display-flex gap-m padding-xl-xl-l-xl position-sticky top-0 width-fill z-index-999" ]
                    headerBarChildren
                , Html.main_
                    [ Html.Attributes.class "display-flex flex-direction-column flex-grow-1 gap-l padding-0-xl width-fill" ]
                    mainChildren
                , Html.div
                    [ Html.Attributes.class "color-666666 display-flex font-size-p9rem justify-content-center margin-l-0 padding-l-xl-xl-xl width-fill" ]
                    footerChildren
                ]

        viewHeaderBarChildren : List (Html message)
        viewHeaderBarChildren =
            [ viewLogo
            , Html.div [ Html.Attributes.id "status" ] []
            , viewRefreshIcon
            ]

        viewLocation :
            Maybe
                { city : String
                , neighborhood : Maybe String
                }
            -> Html message
        viewLocation maybeLocation =
            Html.div
                [ Html.Attributes.class "font-size-1p1rem" ]
                [ Html.text
                    (case maybeLocation of
                        Just location ->
                            case location.neighborhood of
                                Just neighborhood ->
                                    "Routes near your in " ++ neighborhood ++ ", " ++ location.city

                                Nothing ->
                                    "Routes near you in " ++ location.city

                        Nothing ->
                            "Routes near you"
                    )
                ]
    in
    \model ->
        { title = "T Times"
        , body =
            [ case model.routes of
                Just routes ->
                    Html.Lazy.lazy
                        (\routes1 ->
                            viewBodyContent
                                viewHeaderBarChildren
                                [ Html.Lazy.lazy viewLocation routes1.location ]
                                [ viewAboutLink ]
                        )
                        routes

                Nothing ->
                    viewBodyContent
                        viewHeaderBarChildren
                        [ viewLocation Nothing
                        , Html.div
                            [ Html.Attributes.class "background-f9f9f9 height-fill width-fill" ]
                            []
                        ]
                        [ viewAboutLink ]
            ]
        }


viewAboutLink : Html message
viewAboutLink =
    Html.a
        [ Html.Attributes.href "/about" ]
        [ Html.text "About" ]


viewLogo : Html message
viewLogo =
    Html.div
        [ Html.Attributes.class "align-items-center cursor-pointer display-flex flex-shrink-0 gap-m" ]
        [ Html.img
            [ Html.Attributes.class "width-2p2em"
            , Html.Attributes.src "/content/eTuq85jcjI9F4xiclT5o-j4Xy0t9TnbO3KIkwMi82FA"
            ]
            []
        , Html.div
            [ Html.Attributes.class "font-size-1p2rem font-weight-500" ]
            [ Html.text "T Times" ]
        ]


viewRefreshIcon : Html message
viewRefreshIcon =
    Html.div
        [ Html.Attributes.class "border-color-004ce1 border-style-solid border-radius-p3rem border-width-1px cursor-pointer display-none opacity-0"
        ]
        [ Html.span
            [ Html.Attributes.class "font-size-1p2rem material-symbols-rounded" ]
            [ Html.text "\u{E5D5}" ]
        ]


httpGetRoutes : String -> Float -> Float -> Cmd Message
httpGetRoutes deviceId latitude longitude =
    Http.get
        { url =
            Url.Builder.absolute
                [ "api", "v2", "routes" ]
                [ Url.Builder.string "device-id" deviceId
                , Url.Builder.string "latitude" (String.fromFloat latitude)
                , Url.Builder.string "longitude" (String.fromFloat longitude)
                ]
        , expect = Http.expectJson GetRoutes httpGetRoutesDecoder
        }


httpGetRoutesDecoder :
    Json.Decode.Decoder
        { location :
            Maybe
                { city : String
                , neighborhood : Maybe String
                }
        , predictions :
            Result
                MbtaError
                { orsError : Maybe OrsError
                , predictions :
                    List
                        { direction : Int
                        , route : String
                        , routeClass : Int
                        , routeName : String
                        , routePattern : Maybe String
                        , stop : String
                        , stopLocation :
                            { latitude : Float
                            , longitude : Float
                            }
                        , stopName : String
                        , times : List String
                        , trip : String
                        , tripHeadsign : String
                        , walkDuration : Float
                        }
                }
        }
httpGetRoutesDecoder =
    let
        innerPredictionsDecoder :
            Json.Decode.Decoder
                (List
                    { direction : Int
                    , route : String
                    , routeClass : Int
                    , routeName : String
                    , routePattern : Maybe String
                    , stop : String
                    , stopLocation :
                        { latitude : Float
                        , longitude : Float
                        }
                    , stopName : String
                    , times : List String
                    , trip : String
                    , tripHeadsign : String
                    , walkDuration : Float
                    }
                )
        innerPredictionsDecoder =
            Json.Decode.list
                (Json.Decode.map8
                    (\direction route routeClass routeName routePattern stop stopLocation (T5 stopName times trip tripHeadsign walkDuration) ->
                        { direction = direction
                        , route = route
                        , routeClass = routeClass
                        , routeName = routeName
                        , routePattern = routePattern
                        , stop = stop
                        , stopLocation = stopLocation
                        , stopName = stopName
                        , times = times
                        , trip = trip
                        , tripHeadsign = tripHeadsign
                        , walkDuration = walkDuration
                        }
                    )
                    (Json.Decode.field "direction" Json.Decode.int)
                    (Json.Decode.field "route" Json.Decode.string)
                    (Json.Decode.field "route-class" Json.Decode.int)
                    (Json.Decode.field "route-name" Json.Decode.string)
                    (Json.Decode.maybe (Json.Decode.field "route-pattern" Json.Decode.string))
                    (Json.Decode.field "stop" Json.Decode.string)
                    (Json.Decode.field "stop-location"
                        (Json.Decode.map2
                            (\latitude longitude ->
                                { latitude = latitude
                                , longitude = longitude
                                }
                            )
                            (Json.Decode.field "latitude" Json.Decode.float)
                            (Json.Decode.field "longitude" Json.Decode.float)
                        )
                    )
                    (Json.Decode.map5
                        (\stopName times trip tripHeadsign walkDuration ->
                            T5 stopName times trip tripHeadsign walkDuration
                        )
                        (Json.Decode.field "stop-name" Json.Decode.string)
                        (Json.Decode.field "times" (Json.Decode.list Json.Decode.string))
                        (Json.Decode.field "trip" Json.Decode.string)
                        (Json.Decode.field "trip-headsign" Json.Decode.string)
                        (Json.Decode.field "walk-duration" Json.Decode.float)
                    )
                )

        locationDecoder :
            Json.Decode.Decoder
                { city : String
                , neighborhood : Maybe String
                }
        locationDecoder =
            Json.Decode.map2
                (\city neighborhood ->
                    { city = city
                    , neighborhood = neighborhood
                    }
                )
                (Json.Decode.field "city" Json.Decode.string)
                (Json.Decode.maybe (Json.Decode.field "neighborhood" Json.Decode.string))

        mbtaErrorDecoder : Json.Decode.Decoder MbtaError
        mbtaErrorDecoder =
            Json.Decode.field "tag" Json.Decode.string
                |> Json.Decode.andThen
                    (\errorTag ->
                        case errorTag of
                            "not-found" ->
                                Json.Decode.succeed NotFound

                            "something-unexpected" ->
                                Json.Decode.field "value" (Json.Decode.field "message" Json.Decode.string)
                                    |> Json.Decode.map SomethingUnexpected

                            _ ->
                                Json.Decode.fail "unexpected tag"
                    )

        orsErrorDecoder : Json.Decode.Decoder OrsError
        orsErrorDecoder =
            Json.Decode.field "tag" Json.Decode.string
                |> Json.Decode.andThen
                    (\errorTag ->
                        case errorTag of
                            "http-error" ->
                                Json.Decode.succeed HttpError

                            "rate-limit-exceeded" ->
                                Json.Decode.succeed RateLimitExceeded

                            "response-error" ->
                                Json.Decode.field "value" (Json.Decode.field "code" Json.Decode.int)
                                    |> Json.Decode.map ResponseError

                            "service-unavailable" ->
                                Json.Decode.succeed ServiceUnavailable

                            _ ->
                                Json.Decode.fail "unexpected tag"
                    )

        outerPredictionsDecoder =
            Json.Decode.map2
                (\orsError predictions ->
                    { orsError = orsError
                    , predictions = predictions
                    }
                )
                (Json.Decode.maybe (Json.Decode.field "ors-error" orsErrorDecoder))
                (Json.Decode.field "predictions" innerPredictionsDecoder)
    in
    Json.Decode.map2
        (\location predictions ->
            { location = location
            , predictions = predictions
            }
        )
        (Json.Decode.maybe (Json.Decode.field "location" locationDecoder))
        (Json.Decode.field
            "predictions"
            (Json.Decode.field "tag" Json.Decode.string
                |> Json.Decode.andThen
                    (\tag ->
                        case tag of
                            "ok" ->
                                Json.Decode.field "value" outerPredictionsDecoder
                                    |> Json.Decode.map Ok

                            "error" ->
                                Json.Decode.field "value" mbtaErrorDecoder
                                    |> Json.Decode.map Err

                            _ ->
                                Json.Decode.fail "unexpected tag"
                    )
            )
        )


type T5 a b c d e
    = T5 a b c d e
