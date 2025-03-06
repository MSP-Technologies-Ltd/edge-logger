#!/bin/bash

LOG_DIR="$HOME/Documents/Logs"
ARCHIVE_DIR="$HOME/Documents/Archives/Logs"

LAST_MON="$(date -d'monday-14 days' +%Y%m%d)"
LAST_SUN="$(date -dlast-sunday +%Y%m%d)"

ARCHIVE_NAME="Week_${LAST_MON}_${LAST_SUN}"

mkdir -p $ARCHIVE_DIR

mkdir -p $ARCHIVE_DIR/$ARCHIVE_NAME


echo "Finding logs from $LAST_MON to $LAST_SUN"

LOG_FILES="$(find $LOG_DIR -type f -name "*.csv" -newermt $LAST_MON ! -newermt $LAST_SUN)"

if [ -z "$LOG_FILES" ]; then
    echo "No files found"
    exit 1
else
    for file in $LOG_FILES
    do
        if [ -f "$file" ]; then
            mv $file $ARCHIVE_DIR/$ARCHIVE_NAME
        fi
    done
fi