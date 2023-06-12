#!/bin/bash

export PYTHONHOME=/opt/py3.sooners
export LD_LIBRARY_PATH=$PYTHONHOME/lib

# local mirror: -i https://pypi.tuna.tsinghua.edu.cn/simple
$PYTHONHOME/bin/pip3 $*
