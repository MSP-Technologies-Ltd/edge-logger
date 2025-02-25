#!/bin/sh

docker run -d -v ~/Documents/Logs:/app/Logs --network host --restart unless-stopped edge_logger  python -u logger.py
# docker run -d -v C:\Users\Catriona\Documents\Logs:/app/Logs --restart unless-stopped edge_logger
 