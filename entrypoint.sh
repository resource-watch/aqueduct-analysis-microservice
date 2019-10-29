#!/bin/bash
set -e

case "$1" in
    develop)
        echo "Running Development Server"
        exec python main.py
        ;;
    test)
        echo "Running Test"
        exec pytest
        ;;
    start)
        echo "Running Start"
        exec gunicorn -c gunicorn.py aqueduct:app
        ;;
    *)
        exec "$@"
esac
