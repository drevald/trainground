#!/bin/bash
set -e

POSTGRES_POD=$(kubectl -n amazon-lab get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')

echo "=== 1. Cleaning orders only ==="
kubectl -n amazon-lab exec -it $POSTGRES_POD -- psql -U postgres -d amazonlab -c "TRUNCATE orders RESTART IDENTITY;"

echo "=== 2. Recreating Kafka topic ==="
kubectl -n kafka delete kafkatopic order-requests --ignore-not-found
kubectl apply -f ~/trainground/k8s/order-requests-topic.yaml

echo "=== 3. Restarting order-service (to avoid stale topic ID) ==="
kubectl -n amazon-lab rollout restart deployment order-service
kubectl -n amazon-lab rollout status deployment order-service --timeout=120s

echo "=== 4. Verifying clean state ==="
kubectl -n amazon-lab exec -it $POSTGRES_POD -- psql -U postgres -d amazonlab -c "SELECT count(*) AS orders FROM orders;"
kubectl -n amazon-lab exec -it $POSTGRES_POD -- psql -U postgres -d amazonlab -c "SELECT min(id), max(id), count(*) FROM customer;"

echo "=== 5. Ready for test. Example: ==="
echo "curl -X POST \"http://192.168.0.116:30081/shop/start?threads=2000&durationSeconds=60&browsePages=0\""
