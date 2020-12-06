#!/bin/bash

cd /home/cdtest/mskeeper/bin

if [[ $1 != 'stop' ]]; then
        echo "start mskeeper......."
        nohup ./mskeeper > ./mskeeper.out 2>&1 &
fi