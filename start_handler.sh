#!/bin/bash

# Array of your consumer scripts
SCRIPTS=("request_handler.py" "response_handler.py")

for SCRIPT in "${SCRIPTS[@]}"; do
    BASENAME=$(basename "$SCRIPT" .py)
    nohup python3 "$SCRIPT" > "${BASENAME}.log" 2>&1 &
    echo $! > "${BASENAME}.pid"
    echo "Started $SCRIPT with PID $(cat "${BASENAME}.pid")"
    sleep 1
done

