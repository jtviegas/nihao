#!/usr/bin/env bash

# ===> COMMON SECTION START  ===>
# http://bash.cumulonim.biz/NullGlob.html
shopt -s nullglob

if [ -z "$this_folder" ]; then
  this_folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
  if [ -z "$this_folder" ]; then
    this_folder=$(dirname $(readlink -f $0))
  fi
fi
if [ -z "$parent_folder" ]; then
  parent_folder=$(dirname "$this_folder")
fi

# --------------------------------------------------------------
# ---------- CONSTANTS -----------------------------------------
# --------------------------------------------------------------
export INCLUDES_DIR="$parent_folder"
export LIVE_DIR="$parent_folder/atml"
export LIVE_CONTAINER=liveproject
# --------------------------------------------------------------
# --------------------------------------------------------------
# --------------------------------------------------------------

debug(){
    local __msg="$@"
    echo " [DEBUG] `date` ... $__msg "
}

info(){
    local __msg="$@"
    echo " [INFO]  `date` ->>> $__msg "
}

warn(){
    local __msg="$@"
    echo " [WARN]  `date` *** $__msg "
}

err(){
    local __msg="$@"
    echo " [ERR]   `date` !!! $__msg "
}

verify_prereqs(){
  info "[verify_prereqs] ..."
  for arg in "$@"
  do
      debug "[verify_prereqs] ... checking $arg"
      which "$arg" 1>/dev/null
      if [ ! "$?" -eq "0" ] ; then err "[verify_prereqs] please install $arg" && return 1; fi
  done
  info "[verify_prereqs] ...done."
}

if [ ! -f "$INCLUDES_DIR/.variables.inc" ]; then
  debug "we DON'T have a '$INCLUDES_DIR/.variables.inc' file"
else
  . "$INCLUDES_DIR/.variables.inc"
fi

if [ ! -f "$INCLUDES_DIR/.secrets.inc" ]; then
  debug "we DON'T have a '$INCLUDES_DIR/.secrets.inc' file"
else
  . "$INCLUDES_DIR/.secrets.inc"
fi

# <=== COMMON SECTION END  <===

# ===> FUNCTIONS SECTION START  ===>
liveReset()
{
  info "[liveReset|in]"

  docker run -it -v "$LIVE_DIR":/home/manning/liveproject -p 8888:8888 -e QUANDL_API_KEY="$QUANDL_API_KEY" \
    --name "$LIVE_CONTAINER" appliedai/manning:liveproject bash

  info "[liveReset|out]"
}

liveOn()
{
  info "[liveOn|in]"

  docker start -a -i "$LIVE_CONTAINER"

  info "[liveOn|out]"
}

# <=== FUNCTIONS SECTION END  <===

# ===> MAIN SECTION START  ===>

usage()
{
  cat <<EOM
  usages:
  $(basename $0) [ live-reset | live ]
EOM
  exit 1
}

[ -z "$1" ] && { usage; }

info "starting [ $0 $1 $2 ] ..."

case "$1" in
      live-reset)
        liveReset
        ;;
      live)
        liveOn
        ;;
      *)
        usage
esac

info "...[ $0 $1 $2 ] done."
