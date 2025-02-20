#!/bin/sh

docker run -d --name edge_logger \
    -e HOST_HOME=$HOME \
    -v $HOME/Documents/Logs:/app/logs \
    edge-logger
