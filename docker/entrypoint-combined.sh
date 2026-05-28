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
echo "[entrypoint] Config: $(cat $CFG_FILE)"
cd "$CFG_DIR"
export LD_LIBRARY_PATH="/opt/mdl-client:${LD_LIBRARY_PATH}"
echo "[entrypoint] LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "[entrypoint] Files in /opt/mdl-client:"
ls -la /opt/mdl-client/
echo "[entrypoint] ldd check:"
ldd /opt/mdl-client/feeder_client 2>&1 || true
./feeder_client 2>&1 &
CLIENT_PID=$!

# Wait for TCP port 9012 to be ready
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
        echo "[entrypoint] feeder_client ready on 9012 (pid=${CLIENT_PID})"
        break
    fi
    if ! kill -0 $CLIENT_PID 2>/dev/null; then
        # feeder_client may fork to background, check port instead
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
            echo "[entrypoint] feeder_client ready on 9012 (forked)"
            break
        fi
        echo "[entrypoint] ERROR: feeder_client exited unexpectedly"
        echo "[entrypoint] Checking logs..."
        cat "${LOG_DIR}/feeder_client.log" 2>/dev/null | tail -20 || echo "No log file found"
        exit 1
    fi
    sleep 1
done

# Start Python engine
echo "[entrypoint] Starting combined engine..."
exec python -m quant_platform.live_engine.combined_engine
