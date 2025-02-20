#!/bin/sh

docker run -d --name edge_logger \
    -v /home/msp/Documents/Logs:/app/Logs \
    edge-logger