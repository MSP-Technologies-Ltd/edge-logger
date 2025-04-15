#!/bin/bash


LOG_DIR="$PWD/Logs"
# LOG_DIR="$HOME/Documents/Logs"
ARCHIVE_DIR="$PWD/Archive"
# ARCHIVE_DIR="$HOME/Documents/Archives/Logs"

NOW=$(date)

mkdir -p "$ARCHIVE_DIR"

# find log files older than today
LOG_FILES=$(find "$LOG_DIR" -type f -name "*.csv" ! -newermt "$NOW")

# iterate over each log file found matching the above
if [ -z "$LOG_FILES" ]; then
    echo "No log files found older than today, exiting."
    exit 1
else
    echo "$LOG_FILES" | while IFS= read -r file; do
               if [ -f "$file" ]; then
            REL_PATH="${file#$LOG_DIR/}"
            DIR_DEPTH=$(dirname "$REL_PATH" | tr -cd '/' | wc -c)

            MODIFIED=$(stat -c %y "$file" | cut -d'-' -f1,2)
            YEAR=$(echo "$MODIFIED" | cut -d'-' -f1)
            MONTH=$(echo "$MODIFIED" | cut -d'-' -f2)
            
            if [ "$DIR_DEPTH" -ge 1 ]; then
                PARENT_NAME="$(basename "$(dirname "$(dirname "$file")")")"
            else
                PARENT_NAME="$(basename "$(dirname "$file")")"
            fi

            TARGET_DIR="$ARCHIVE_DIR/$YEAR/$MONTH"
            mkdir -p "$TARGET_DIR"

            MONTH_NAME=$(date -d "$YEAR-$MONTH-01" +%B)
            ARCHIVE_FILE="$TARGET_DIR/${PARENT_NAME}-${MONTH_NAME}-${YEAR}.tar.gz"

            # Create archive with relative path preserved
            tar -czf "$ARCHIVE_FILE.tmp" -C "$LOG_DIR" "$REL_PATH"

            if [ -f "$ARCHIVE_FILE" ]; then
                mkdir -p "$TARGET_DIR/temp"
                tar -xf "$ARCHIVE_FILE" -C "$TARGET_DIR/temp"
                tar -xf "$ARCHIVE_FILE.tmp" -C "$TARGET_DIR/temp"
                tar -czf "$ARCHIVE_FILE" -C "$TARGET_DIR/temp" .
                rm -rf "$TARGET_DIR/temp"
                rm "$ARCHIVE_FILE.tmp"
            else
                mv "$ARCHIVE_FILE.tmp" "$ARCHIVE_FILE"
            fi

            # ✅ Delete original file now that it's archived
            echo "Archived and removing: $file"
            rm "$file"
        fi


    done
fi


# for each file:
# get device name from parent/grandparent dir
# get year from each file
# get month from each file
# create nested tar gz files