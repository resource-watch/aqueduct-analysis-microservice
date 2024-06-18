#!/bin/bash
set -e

case "$1" in
    develop)
        echo "Running Development Server"
        exec python main.py
        ;;
    test)
        echo "Running Test"
        exec pytest -v tests/
        ;;
    start)
        echo "Running Start"
        exec gunicorn -c gunicorn.py aqueduct:app --timeout=60
        ;;
    worker)
        echo "Running worker"
        exec python aqueduct/workers/supply-chain-worker.py
        ;;
    *)
        exec "$@"
esac
