#!/bin/bash
set -e

REPLICAS=${1:-8}
THREADS=${2:-2000}
DURATION=${3:-60}
BROWSE_PAGES=${4:-5}

echo "=== 1. Stopping any active test ==="
curl -s -X POST "http://192.168.0.116:30081/shop/stop" || true
sleep 2

echo "=== 2. Restarting Postgres to clear stuck connections ==="
kubectl -n amazon-lab rollout restart deployment postgres
kubectl -n amazon-lab rollout status deployment postgres --timeout=60s
kubectl -n amazon-lab wait --for=condition=ready pod -l app=postgres --timeout=60s
sleep 5

POSTGRES_POD=$(kubectl -n amazon-lab get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')

echo "=== 3. Cleaning orders table ==="
kubectl -n amazon-lab exec -it $POSTGRES_POD -- psql -U postgres -d amazonlab -c "TRUNCATE orders RESTART IDENTITY;"

echo "=== 4. Setting order-service replicas to $REPLICAS ==="
kubectl -n amazon-lab scale deployment order-service --replicas=$REPLICAS
kubectl -n amazon-lab rollout status deployment order-service --timeout=180s
sleep 15

echo "=== 5. Current pod state ==="
kubectl -n amazon-lab get pods -l app=order-service

echo "=== 6. Starting test: threads=$THREADS duration=${DURATION}s browsePages=$BROWSE_PAGES ==="
curl -s -X POST "http://192.168.0.116:30081/shop/start?threads=$THREADS&durationSeconds=$DURATION&browsePages=$BROWSE_PAGES&useKafka=false"
echo ""

echo "=== 7. Waiting for test to complete (${DURATION}s + buffer) ==="
sleep $((DURATION + 10))

echo "=== 8. Final result ==="
curl -s "http://192.168.0.116:30081/shop/status"
echo ""

echo "=== 9. Pod state after test ==="
kubectl -n amazon-lab get pods -l app=order-service
