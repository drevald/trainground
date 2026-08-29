#!/bin/bash

THREADS_LIST=(5000)
PAGES_LIST=(3)

RUN_DIR=~/trainground/test-results-$(date +%Y%m%d-%H%M%S)
mkdir -p "$RUN_DIR"

SUMMARY_FILE="$RUN_DIR/summary.csv"
echo "testId,threads,pages,sent,succeeded,failed" > "$SUMMARY_FILE"

collect_resources() {
    local outfile=$1
    echo "timestamp,loadAvg1,memFreeMB,pgActiveConnections,pgLockWaits" > "$outfile"
    while true; do
        TS=$(date +%s)
        LOAD=$(uptime | grep -oP 'load average: \K[0-9.]+')
        MEM_FREE=$(free -m | awk '/Mem:/{print $4}')
        PG_ACTIVE=$(kubectl -n amazon-lab exec $(kubectl -n amazon-lab get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}') -- psql -U postgres -t -c "SELECT count(*) FROM pg_stat_activity WHERE state != 'idle';" 2>/dev/null | tr -d ' ')
        PG_LOCKS=$(kubectl -n amazon-lab exec $(kubectl -n amazon-lab get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}') -- psql -U postgres -t -c "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type IN ('Lock','LWLock');" 2>/dev/null | tr -d ' ')
        echo "$TS,$LOAD,$MEM_FREE,$PG_ACTIVE,$PG_LOCKS" >> "$outfile"
        sleep 3
    done
}

for threads in "${THREADS_LIST[@]}"; do
  for pages in "${PAGES_LIST[@]}"; do
    TESTID="threads${threads}_pages${pages}"
    echo "=== Running $TESTID ==="

    RESOURCE_FILE="$RUN_DIR/${TESTID}_resources.csv"
    collect_resources "$RESOURCE_FILE" &
    COLLECTOR_PID=$!

    ~/trainground/scripts/full-test-cycle.sh 8 "$threads" 120 "$pages" > "$RUN_DIR/${TESTID}_output.log" 2>&1

    kill $COLLECTOR_PID 2>/dev/null
    wait $COLLECTOR_PID 2>/dev/null

    RESULT=$(curl -s "http://192.168.0.116:30081/shop/status")
    SENT=$(echo "$RESULT" | grep -oP 'sent=\K[0-9]+')
    SUCCEEDED=$(echo "$RESULT" | grep -oP 'succeeded=\K[0-9]+')
    FAILED=$(echo "$RESULT" | grep -oP 'failed=\K[0-9]+')

    echo "$TESTID,$threads,$pages,$SENT,$SUCCEEDED,$FAILED" >> "$SUMMARY_FILE"

    echo "=== Done $TESTID: sent=$SENT succeeded=$SUCCEEDED failed=$FAILED ==="
    sleep 5
  done
done

echo "=== All done. Results in $RUN_DIR ==="
cat "$SUMMARY_FILE"
echo "$RUN_DIR" > /tmp/last-run-dir.txt
