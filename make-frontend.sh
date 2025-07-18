#!/bin/bash

set -ex

elm make frontend/Main.elm --optimize --output main.js
uglifyjs main.js --compress 'passes=2,pure_funcs=[F2,F3,F4,F5,F6,F7,F8,F9,A2,A3,A4,A5,A6,A7,A8,A9],pure_getters=true,keep_fargs=false,unsafe_comps=true,unsafe=true' --mangle --output main.min.js
