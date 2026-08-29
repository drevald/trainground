#!/bin/bash

THREADS_LIST=(2000 5000)
PAGES_LIST=(0 3)

collect_metrics() {
    local testid=$1
    while true; do
        TS=$(date +%s)
        LOAD=$(uptime | grep -oP 'load average: \K[0-9.]+')
        MEM_FREE=$(free -m | awk '/Mem:/{print $4}')
        PG_STATS=$(kubectl -n amazon-lab exec $(kubectl -n amazon-lab get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}') -- psql -U postgres -t -c "SELECT count(*) FROM pg_stat_activity WHERE state != 'idle';" 2>/dev/null | tr -d ' ')
        echo "{\"operation\":\"systemMetrics\",\"testId\":\"$testid\",\"timestamp\":$TS,\"loadAvg\":\"$LOAD\",\"memFreeMB\":$MEM_FREE,\"pgActiveConnections\":$PG_STATS}" | kubectl -n kafka exec -i my-cluster-dual-role-0 -- bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic perf-metrics 2>/dev/null
        sleep 5
    done
}

for threads in "${THREADS_LIST[@]}"; do
  for pages in "${PAGES_LIST[@]}"; do
    TESTID="threads${threads}_pages${pages}"
    echo "=== Running $TESTID ==="

    collect_metrics "$TESTID" &
    COLLECTOR_PID=$!

    ~/trainground/scripts/full-test-cycle.sh 8 "$threads" 120 "$pages"

    kill $COLLECTOR_PID 2>/dev/null || true
    wait $COLLECTOR_PID 2>/dev/null || true

    echo "{\"operation\":\"testCompleted\",\"testId\":\"$TESTID\",\"timestamp\":$(date +%s)}" | kubectl -n kafka exec -i my-cluster-dual-role-0 -- bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic perf-metrics 2>/dev/null

    echo "=== Done $TESTID ==="
    sleep 5
  done
done

echo "All tests complete."
