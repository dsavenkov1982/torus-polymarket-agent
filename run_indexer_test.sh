#!/bin/bash
# run_indexer_test.sh - Test run the indexer for a limited time

echo "=========================================="
echo "POLYMARKET INDEXER TEST RUN"
echo "=========================================="
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set PYTHONPATH to include project root
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"

# Configuration
RUN_TIME=120  # Run for 2 minutes (120 seconds)

echo "Starting indexer..."
echo "Will run for $RUN_TIME seconds"
echo ""

# Start the indexer in the background
cd "$SCRIPT_DIR"
python core/tasks/blockchain_indexer.py &

# Save the process ID
INDEXER_PID=$!

echo "Indexer started with PID: $INDEXER_PID"
echo ""

# Wait for specified time
echo "Indexing blockchain data..."
for i in $(seq $RUN_TIME -10 0); do
    if [ $((i % 30)) -eq 0 ]; then
        echo "  Time remaining: ${i}s"
    fi
    sleep 10
done

echo ""
echo "Stopping indexer..."

# Stop the indexer
kill $INDEXER_PID 2>/dev/null

# Wait a moment for graceful shutdown
sleep 2

# Force kill if still running
kill -9 $INDEXER_PID 2>/dev/null

echo "Indexer stopped"
echo ""

# Check what was indexed
echo "=========================================="
echo "INDEXING RESULTS"
echo "=========================================="
echo ""

docker exec -it postgres-polymarket psql -U postgres -d polymarket_indexer << EOF
SELECT
    name,
    last_processed_block,
    total_events_processed,
    status,
    updated_at
FROM indexer_state;

SELECT
    'Trades' as type,
    COUNT(*) as count
FROM trades
UNION ALL
SELECT
    'Conditions',
    COUNT(*)
FROM conditions
UNION ALL
SELECT
    'Users',
    COUNT(*)
FROM user_stats;
EOF

echo ""
echo "Test run complete!"