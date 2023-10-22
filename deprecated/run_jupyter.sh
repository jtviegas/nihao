#!/usr/bin/env bash

# ===> COMMON SECTION START  ===>
# http://bash.cumulonim.biz/NullGlob.html
shopt -s nullglob


# see conda environment
# conda env list

# swith conda env
# conda activate <env_name>

# jupyter notebook --ip 0.0.0.0 --no-browser

jupyter lab --ip 0.0.0.0 --no-browser
