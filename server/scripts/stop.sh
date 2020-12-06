#!/bin/bash


cd /home/cdtest/mskeeper/bin

ps_mskeeper=`cat mskeeper.pid`
if [[ $ps_mskeeper != "" ]]; then
        echo "stop mskeeper...$ps_mskeeper"
        kill -TERM $ps_mskeeper
        for i in $(seq 100)
        do
                if [[ ! -f mskeeper.pid ]]; then
                        break
                fi
                sleep 0.5
        done
fi

sleep 1
echo "all stopped"