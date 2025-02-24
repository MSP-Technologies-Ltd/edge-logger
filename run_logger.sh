#!/bin/sh

docker run -d -v ~/Documents/Logs:/app/Logs --restart unless-stopped edge_logger
# docker run -d -v C:\Users\Catriona\Documents\Logs:/app/Logs --restart unless-stopped edge_logger
 