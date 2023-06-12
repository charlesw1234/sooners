#!/bin/bash

if test x$MANAGE_PY = x; then
    MANAGE_PY=./manage.py
fi
export SOURCE_ROOT=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
export SANDBOX_ROOT=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
export PYTHONHOME=/opt/py3.sooners
export LD_LIBRARY_PATH=$PYTHONHOME/lib
$PYTHONHOME/bin/python3 $MANAGE_PY $*
