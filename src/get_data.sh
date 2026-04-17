#!/bin/bash

echo
echo "------------------------------------------------------------"
echo "                     ${BASH_SOURCE##*/}                     "
echo "------------------------------------------------------------"
echo

set -e

#------------------------ Constant Params ------------------------
# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# Python script name
SCRIPT_PY="$SCRIPT_DIR/get_data.py"
# Project path
PROJ_PATH="$SCRIPT_DIR/.."

# conda env file
CONDA_ENV_FILE="$PROJ_PATH/conda.yaml"
#get conda env name
ENV_NAME=$(grep '^name:' $CONDA_ENV_FILE | awk '{print $2}')
# Create conda env
conda env update -f $CONDA_ENV_FILE

#------------------------ Input parameters ------------------------
# 1 --city           type=str    [required]
# 2 --max_lag        type=str    [optional]
#--------------------------- Check args ---------------------------
if [ "$#" -lt 1 ]; then
    echo
    echo "Illegal number of parameters. At least 1"
    echo
    exit 1
fi

#-------------------------- Run conda ---------------------------
conda  run -n "$ENV_NAME" python $SCRIPT_PY $1 $2

# Return exit code
err_code=$?
echo return code: $err_code
exit $err_code