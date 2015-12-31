#!/bin/bash

function type() {
    cat <<EOF
L
M
EOF
}

function class() {
    cat <<EOF
s
a
EOF
}

function r-type() {
    entries=`type | wc -l`
    type | head -n $[$RANDOM % $entries + 1] | tail -n 1
}

function r-class() {
    entries=`class | wc -l`
    class | head -n $[$RANDOM % $entries + 1] | tail -n 1
}
while true; do
    echo -ne "{ \"type\": \"log\","
    echo -ne "  \"version\": \"0.2\","
    echo -ne "  \"level\": \"application\","
    echo -ne "  \"state\": \"Info\","
    echo -ne "  \"time\": $(($(date +%s%N)/1000000)),"
    echo -ne "  \"text\": \"node start\"}\n"

    echo -ne "{ \"type\": \"metric\","
    echo -ne "  \"version\": \"0.2\","
    echo -ne "  \"level\": \"system\","
    echo -ne "  \"time\": $(($(date +%s%N)/1000000)),"
    echo -ne "  \"text\": \"login.latency:2|c\"}\n"
    sleep 3;
done


