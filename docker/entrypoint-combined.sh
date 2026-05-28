#!/bin/bash
set -e

# Generate feeder_client.cfg from environment variable
CFG_DIR="/opt/mdl-client"
CFG_FILE="${CFG_DIR}/feeder_client.cfg"
LOG_DIR="${MDL_CLIENT_LOG_DIR:-/data/quant/mdl_logs/client}"

mkdir -p "$LOG_DIR" "$CFG_DIR"

cat > "$CFG_FILE" <<CFGEOF
{
    "feeder_client" : {
        "Publishers" : [
            {"Type" : "TCP_SERVER", "Address" : "0.0.0.0:9012", "OutputBufferMax" : 512000},
            {
                "Type": "WEBSOCKET_SERVER",
                "Address": "0.0.0.0:9020",
                "OutputBufferMB": 256,
                "ClientIdleTimeout": 10,
                "HeartbeatSendInterval": 3,
                "HeartbeatTimeout": 10,
                "MaxNumSubs": 300
            }
        ]
    },
    "feeder_client_log" : {
        "LogFormat" : "%D[%t][%p]%c - %m%n",
        "LogConsole" : false,
        "LogFiles" : [
            {"LogLevelMax": "debug", "FileName": "${LOG_DIR}/feeder_client.trace.log", "MaxSize": 10240, "MaxBackup": 5},
            {"LogLevelMin": "info",  "FileName": "${LOG_DIR}/feeder_client.log", "MaxSize": 10240, "MaxBackup": 10}
        ]
    },
    "msg_backup" : {
        "BackupDir" : "/data/quant/mdl_msg_backup",
        "WriteFileState" : false,
        "Encoding" : 5,
        "DiskspaceReservedInGB" : 100,
        "FolderCountReserve" : 10
    },
    "Client" : {
        "AutoConfig": {
            "URL": "https://mdl01.datayes.com:19000/subscribe",
            "Token": "${MDL_TOKEN}",
            "Options": {
                "UseCDN": false
            }
        }
    }
}
CFGEOF

echo "[entrypoint] Starting feeder_client..."
cd "$CFG_DIR"
./feeder_client &
CLIENT_PID=$!

# Wait for TCP port 9012 to be ready
echo "[entrypoint] Waiting for feeder_client to listen on 9012..."
for i in $(seq 1 60); do
    if python -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.connect(('127.0.0.1', 9012))
    s.close()
    exit(0)
except:
    exit(1)
" 2>/dev/null; then
        echo "[entrypoint] feeder_client ready on 9012 (pid=${CLIENT_PID})"
        break
    fi
    if ! kill -0 $CLIENT_PID 2>/dev/null; then
        echo "[entrypoint] ERROR: feeder_client exited unexpectedly"
        exit 1
    fi
    sleep 1
done

# Start Python engine
echo "[entrypoint] Starting combined engine..."
exec python -m quant_platform.live_engine.combined_engine
