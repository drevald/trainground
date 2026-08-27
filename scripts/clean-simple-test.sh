#!/bin/bash
set -e

REPLICAS=${1:-1}
THREADS=${2:-1000}
DURATION=${3:-60}
BROWSE_PAGES=${4:-5}

POSTGRES_POD=$(kubectl -n amazon-lab get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')

echo "=== 1. Stopping any active shopaholic test ==="
curl -s -X POST "http://192.168.0.116:30081/shop/stop" || true

echo "=== 2. Cleaning orders table ==="
kubectl -n amazon-lab exec -it $POSTGRES_POD -- psql -U postgres -d amazonlab -c "TRUNCATE orders RESTART IDENTITY;"

echo "=== 3. Setting order-service replicas to $REPLICAS ==="
kubectl -n amazon-lab scale deployment order-service --replicas=$REPLICAS

echo "=== 4. Waiting for all replicas to stabilize ==="
kubectl -n amazon-lab rollout status deployment order-service --timeout=180s

echo "=== 5. Extra pause to ensure health checks are stable ==="
sleep 10
kubectl -n amazon-lab get pods -l app=order-service

echo "=== 6. Ready. Run test with: ==="
echo "curl -X POST \"http://192.168.0.116:30081/shop/start?threads=$THREADS&durationSeconds=$DURATION&browsePages=$BROWSE_PAGES&useKafka=false\""
