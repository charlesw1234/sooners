#!/bin/bash

export PYTHONHOME=/opt/py3.sooners
export LD_LIBRARY_PATH=$PYTHONHOME/lib

$PYTHONHOME/bin/python3 $*
