port module Ttimes.Ports exposing (..)

-- Commands


port watchPosition : () -> Cmd message



-- Subscriptions


port currentPosition :
    ({ longitude : Float
     , latitude : Float
     , timestamp : Int
     }
     -> message
    )
    -> Sub message


port currentPositionError :
    ({ code : Int
     , message : String
     }
     -> message
    )
    -> Sub message


port hidden : (Bool -> message) -> Sub message
