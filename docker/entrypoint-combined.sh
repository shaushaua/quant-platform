#!/bin/bash
set -e

# Generate feeder_client.cfg from environment variable
CFG_DIR="/opt/mdl-client"
CFG_FILE="${CFG_DIR}/feeder_client.cfg"
LOG_DIR="${MDL_CLIENT_LOG_DIR:-/data/quant/mdl_logs/client}"
BACKUP_DIR="/data/quant/mdl_msg_backup"

mkdir -p "$LOG_DIR" "$CFG_DIR" "$BACKUP_DIR"

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
        "LogConsole" : true,
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
        "UpStreams" : [
            {
                "Address": "mdl-cloud-sh.datayes.com:19012",
                "UserName": "${MDL_TOKEN}",
                "Encoding": 7,
                "EnableServerSelect": true,
                "HeartbeatInterval": 10,
                "HeartbeatTimeout": 30,
                "Services": [
                    {
                        "Version": 101,
                        "Messages": [28, 33, 36]
                    }
                ]
            },
            {
                "Address": "mdl-cloud-sh.datayes.com:19014",
                "UserName": "${MDL_TOKEN}",
                "Encoding": 7,
                "EnableServerSelect": true,
                "HeartbeatInterval": 10,
                "HeartbeatTimeout": 30,
                "Services": [
                    {
                        "Version": 101,
                        "Messages": [4, 24]
                    }
                ]
            }
        ]
    }
}
CFGEOF

echo "[entrypoint] Starting feeder_client..."
cd "$CFG_DIR"
export LD_LIBRARY_PATH="/opt/mdl-client:${LD_LIBRARY_PATH}"
./feeder_client &

# feeder_client forks to background — just wait for port 9012 to be ready
echo "[entrypoint] Waiting for feeder_client to listen on 9012..."
for i in $(seq 1 120); do
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
        echo "[entrypoint] feeder_client ready on 9012"
        break
    fi
    sleep 1
done

# Verify port is actually open
if ! python -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 9012))
s.close()
" 2>/dev/null; then
    echo "[entrypoint] ERROR: feeder_client did not start within 120s"
    cat "${LOG_DIR}/feeder_client.log" 2>/dev/null | tail -20 || true
    exit 1
fi

# Start Python engine
echo "[entrypoint] Starting combined engine..."
exec python -m quant_platform.live_engine.combined_engine
