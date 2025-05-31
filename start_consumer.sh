#!/bin/bash

# Array of your consumer scripts
SCRIPTS=("location_consumer.py" "static_consumer.py")

for SCRIPT in "${SCRIPTS[@]}"; do
    BASENAME=$(basename "$SCRIPT" .py)
    nohup python3 "$SCRIPT" > "${BASENAME}.log" 2>&1 &
    echo $! > "${BASENAME}.pid"
    echo "Started $SCRIPT with PID $(cat "${BASENAME}.pid")"
done

