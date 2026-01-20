#!/bin/bash

# Start NATS server with JetStream enabled in the background
echo "Starting NATS server with JetStream on port 6000..."
nats-server -p 6000 -js &

# Wait a moment for NATS to start
sleep 1

# Check if NATS is running
if pgrep -x "nats-server" > /dev/null; then
    echo "NATS server started successfully"
else
    echo "Warning: NATS server may not have started properly"
fi

# Execute the command passed to the container (or default to bash)
exec "$@"
