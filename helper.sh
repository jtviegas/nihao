#!/usr/bin/env bash

# ===> COMMON SECTION START  ===>

# http://bash.cumulonim.biz/NullGlob.html
shopt -s nullglob
# -------------------------------
this_folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -z "$this_folder" ]; then
  this_folder=$(dirname $(readlink -f $0))
fi
parent_folder=$(dirname "$this_folder")
# -------------------------------
debug(){
    local __msg="$1"
    echo " [DEBUG] `date` ... $__msg "
}

info(){
    local __msg="$1"
    echo " [INFO]  `date` ->>> $__msg "
}

warn(){
    local __msg="$1"
    echo " [WARN]  `date` *** $__msg "
}

err(){
    local __msg="$1"
    echo " [ERR]   `date` !!! $__msg "
}
# ---------- CONSTANTS ----------
export FILE_VARIABLES=${FILE_VARIABLES:-".variables"}
export FILE_LOCAL_VARIABLES=${FILE_LOCAL_VARIABLES:-".local_variables"}
export FILE_SECRETS=${FILE_SECRETS:-".secrets"}
export NAME="bashutils"
export INCLUDE_FILE=".${NAME}"
export TAR_NAME="${NAME}.tar.bz2"
# -------------------------------
if [ ! -f "$this_folder/$FILE_VARIABLES" ]; then
  warn "we DON'T have a $FILE_VARIABLES variables file - creating it"
  touch "$this_folder/$FILE_VARIABLES"
else
  . "$this_folder/$FILE_VARIABLES"
fi

if [ ! -f "$this_folder/$FILE_LOCAL_VARIABLES" ]; then
  warn "we DON'T have a $FILE_LOCAL_VARIABLES variables file - creating it"
  touch "$this_folder/$FILE_LOCAL_VARIABLES"
else
  . "$this_folder/$FILE_LOCAL_VARIABLES"
fi

if [ ! -f "$this_folder/$FILE_SECRETS" ]; then
  warn "we DON'T have a $FILE_SECRETS secrets file - creating it"
  touch "$this_folder/$FILE_SECRETS"
else
  . "$this_folder/$FILE_SECRETS"
fi

# ---------- include bashutils ----------
. ${this_folder}/${INCLUDE_FILE}

# ---------- FUNCTIONS ----------

update_bashutils(){
  echo "[update_bashutils] ..."

  tar_file="${NAME}.tar.bz2"
  _pwd=`pwd`
  cd "$this_folder"

  curl -s https://api.github.com/repos/jtviegas/bashutils/releases/latest \
  | grep "browser_download_url.*${NAME}\.tar\.bz2" \
  | cut -d '"' -f 4 | wget -qi -
  tar xjpvf $tar_file
  if [ ! "$?" -eq "0" ] ; then echo "[update_bashutils] could not untar it" && cd "$_pwd" && return 1; fi
  rm $tar_file

  cd "$_pwd"
  echo "[update_bashutils] ...done."
}

# <=== COMMON SECTION END  <===
# -------------------------------------

# =======>    MAIN SECTION    =======>

# ---------- LOCAL CONSTANTS ----------

# ---------- LOCAL FUNCTIONS ----------

build(){
  info "[build] ..."

  _pwd=`pwd`
  cd "$this_folder"

  rm -rf dist
  python3 -m build
  [ "$?" -ne "0" ] && err "[build] ooppss" && exit 1

  cd "$_pwd"
  echo "[build] ...done."
}

publish(){
  info "[publish] ..."

  _pwd=`pwd`
  cd "$this_folder"

  twine upload -u $PYPI_USER -p $PYPI_API_TOKEN dist/*
  [ "$?" -ne "0" ] && err "[publish] ooppss" && exit 1

  cd "$_pwd"
  echo "[publish] ...done."
}

code_lint()
{
    info "[code_lint|in]"
    src_folders="src test"
    info "[code_lint] ... isort..."
    isort --profile black --src $src_folders
    return_value=$?
    info "[code_lint] ... isort...$return_value"
    if [ "$return_value" -eq "0" ]; then
      info "[code_lint] ... autoflake..."
      autoflake --remove-all-unused-imports --in-place --recursive -r $src_folders
      return_value=$?
      info "[code_lint] ... autoflake...$return_value"
    fi
    if [ "$return_value" -eq "0" ]; then
      info "[code_lint] ... black..."
      black -v -t py38 $src_folders
      return_value=$?
      info "[code_lint] ... black...$return_value"
    fi
    [ "$return_value" -ne "0" ] && exit 1
    info "[code_lint|out] => ${return_value}"
}

code_check()
{
    info "[code_check|in]"
    src_folders="src test"
    info "[code_check] ... isort..."
    isort --profile black -v --src $src_folders
    return_value=$?
    info "[code_check] ... isort...$return_value"
    if [ "$return_value" -eq "0" ]; then
      info "[code_check] ... autoflake..."
      autoflake --check -r $src_folders
      return_value=$?
      info "[code_check] ... autoflake...$return_value"
    fi
    if [ "$return_value" -eq "0" ]; then
      info "[code_check] ... black..."
      black --check $src_folders
      return_value=$?
      info "[code_check] ... black...$return_value"
    fi
    [ "$return_value" -ne "0" ] && exit 1
    info "[code_check|out] => ${return_value}"
}

check_coverage()
{
  info "[check_coverage|in]"
  coverage report -m
  result="$?"
  [ "$result" -ne "0" ] && exit 1
  info "[check_coverage|out] => $result"
}

test()
{
    info "[test|in] ($1)"
    python -m pytest -x -vv --durations=0 --cov=src --junitxml=tests-results.xml --cov-report=xml --cov-report=html "$1"
    return_value="$?"
    [ "$return_value" -ne "0" ] && exit 1
    info "[test|out] => ${return_value}"
}

reqs()
{
    info "[reqs|in]"
    pip install -r requirements.txt
    [ "$?" -ne "0" ] && exit 1
    info "[reqs|out]"
}

# -------------------------------------
usage() {
  cat <<EOM
  usage:
  $(basename $0) { package }

      - package: tars the bashutils include file
      - update_bashutils: updates the include '.bashutils' file
      - build
      - publish
      - test
      - coverage
      - code_check
      - code_lint
      - reqs
EOM
  exit 1
}

debug "1: $1 2: $2 3: $3 4: $4 5: $5 6: $6 7: $7 8: $8 9: $9"

case "$1" in
  package)
    package
    ;;
  update_bashutils)
    update_bashutils
    ;;
  build)
    build
    ;;
  publish)
    publish
    ;;
  test)
      test "$2"
      ;;
  coverage)
      check_coverage
      ;;
  code_check)
      code_check
      ;;
  code_lint)
      code_lint
      ;;
  reqs)
      reqs
      ;;
  *)
    usage
    ;;
esac