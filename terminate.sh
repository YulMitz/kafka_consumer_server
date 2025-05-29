#!/bin/bash

# Array of your consumer scripts
SCRIPTS=("location_consumer.py" "static_consumer.py")

for SCRIPT in "${SCRIPTS[@]}"; do
    BASENAME=$(basename "$SCRIPT" .py)
    PID_FILE="${BASENAME}.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p $PID > /dev/null; then
            kill $PID
            echo "Stopped $SCRIPT (PID $PID)"
        else
            echo "Process $PID for $SCRIPT not found. It may have already stopped."
        fi
        rm "$PID_FILE"
    else
        echo "No PID file found for $SCRIPT."
    fi
done

