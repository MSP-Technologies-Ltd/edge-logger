#!/bin/bash

# log and archive directories
LOG_DIR="$HOME/Documents/Logs"
ARCHIVE_DIR="$HOME/Documents/Archives/Logs"

# last mon and sun dates 
LAST_MON="$(date -d'monday-14 days' +%Y%m%d)"
LAST_SUN="$(date -dlast-sunday +%Y%m%d)"

# archive name
ARCHIVE_NAME="${LAST_MON}-${LAST_SUN}"

# make directories
mkdir -p "$ARCHIVE_DIR"
mkdir -p "$ARCHIVE_DIR/$ARCHIVE_NAME"

# find files in log dir from last mon to last sun of file type csv
LOG_FILES=$(find "$LOG_DIR" -type f -name "*.csv" -newermt "$LAST_MON" ! -newermt "$LAST_SUN")

# if log files = null
if [ -z "$LOG_FILES" ]; then
    echo "Folder empty bye"
    exit 1
else
    # Iterate through files
    echo "$LOG_FILES" | while IFS= read -r file; do
        # Check if file exists
        if [ -f "$file" ]; then
            rel_path="${file#$LOG_DIR/}"
            dest_dir="$ARCHIVE_DIR/$ARCHIVE_NAME/$rel_path"

            # Create destination directory if it doesn't exist
            mkdir -p "$(dirname "$dest_dir")"

            # Copy the file to the destination
            cp "$file" "$dest_dir"
        else
            echo "$file does not exist"
        fi
    done
fi

echo "Archived logs to $ARCHIVE_DIR/$ARCHIVE_NAME"

FILE_COUNT=$(find "$LOG_DIR" -type f -name "*.csv" -newermt "$LAST_MON" ! -newermt "$LAST_SUN" | wc -l)
ARCHIVE_COUNT=$(find "$ARCHIVE_DIR/$ARCHIVE_NAME/" -type f | wc -l)

echo "Original file count: $FILE_COUNT"
echo "Archived file count: $ARCHIVE_COUNT"

if [ "$FILE_COUNT" -eq "$ARCHIVE_COUNT" ]; then
    echo "All files archived successfully"

    # create a tar archive
    tar czf "$ARCHIVE_DIR/$ARCHIVE_NAME.tar.gz" -C "$ARCHIVE_DIR" "$ARCHIVE_NAME"

    rm -r "$ARCHIVE_DIR/$ARCHIVE_NAME"

    echo "Archive created at $ARCHIVE_DIR/$ARCHIVE_NAME.tar.gz"

    # check if tar created

    if [ -f "$ARCHIVE_DIR/$ARCHIVE_NAME.tar.gz" ]; then
        echo "Archive created successfully"
    else
        echo "Archive creation failed"
    fi

else
    echo "Some files were not archived - did not complete successfully"
fi