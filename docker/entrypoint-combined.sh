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
            {"Type" : "TCP_SERVER", "Address" : "0.0.0.0:9012", "OutputBufferMax" : ${MDL_TCP_OUTPUT_BUFFER_MAX:-2048000}},
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
        "FolderCountReserve": 10
    },
    "Client" : {
        "AutoConfig" : {
            "URL": "https://mdl01.datayes.com:19000/subscribe",
            "Token": "${MDL_TOKEN}",
            "Options": {
                "UseCDN": false,
                "ExMsgs": "4.6,4.16,4.17,4.20,4.21,4.22,4.23,6.29,6.30,6.31,6.42,6.43,6.44,6.45,6.46,6.47,6.50,6.51,6.54"
            }
        }
    }
}
CFGEOF

echo "[entrypoint] Starting feeder_client..."
cd "$CFG_DIR"
export LD_LIBRARY_PATH="/opt/mdl-client:${LD_LIBRARY_PATH}"
./feeder_client &

# Wait for feeder_client TCP port 9012 to be ready
echo "[entrypoint] Waiting for feeder_client to listen on 9012..."
for i in $(seq 1 120); do
    if (echo > /dev/tcp/127.0.0.1/9012) 2>/dev/null; then
        echo "[entrypoint] feeder_client ready on 9012"
        break
    fi
    sleep 1
done

# Final check
if ! (echo > /dev/tcp/127.0.0.1/9012) 2>/dev/null; then
    echo "[entrypoint] ERROR: feeder_client did not start within 120s"
    cat "${LOG_DIR}/feeder_client.log" 2>/dev/null | tail -20 || true
    exit 1
fi

echo "[entrypoint] Cleaning invalid native SHM files..."
python - <<'PY'
import glob
import os
import struct

magic = 0x514D444C53484D31
removed = 0
for path in glob.glob(os.environ.get("NATIVE_SHM_DIR", "/data/quant/shm") + "/*.mmap"):
    try:
        with open(path, "rb") as f:
            header = f.read(64)
        if len(header) < 64:
            os.remove(path)
            removed += 1
            continue
        values = struct.unpack("<QQQQQQQQ", header)
        if values[0] != magic or values[1] != 2:
            os.remove(path)
            removed += 1
    except FileNotFoundError:
        pass
    except Exception:
        try:
            os.remove(path)
            removed += 1
        except FileNotFoundError:
            pass
print(f"[entrypoint] removed invalid native SHM files: {removed}")
PY

echo "[entrypoint] Starting native-mdl-collector + native engine..."

export LD_LIBRARY_PATH="/opt/native-mdl-collector/lib:${LD_LIBRARY_PATH}"
/opt/native-mdl-collector/bin/native-mdl-collector &
collector_pid=$!
echo "[entrypoint] native-mdl-collector started (pid=$collector_pid)"

echo "[entrypoint] Starting native engine..."
python -m quant_platform.live_engine.native_engine &
engine_pid=$!

wait -n "$collector_pid" "$engine_pid" 2>/dev/null || true
echo "[entrypoint] One process exited, shutting down..."
kill "$collector_pid" "$engine_pid" 2>/dev/null || true
wait
exit 1
