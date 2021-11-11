#!/bin/bash

case "$1" in
    develop)
        type docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose is required but it's not installed.  Aborting."; exit 1; }
        docker-compose -f docker-compose-develop.yml build && docker-compose -f docker-compose-develop.yml up
        ;;
    gr_develop)
        type docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose is required but it's not installed.  Aborting."; exit 1; }
        docker-compose -f docker-compose-gr.yml build && docker-compose -f docker-compose-gr.yml up
        ;;
    shell)
        type docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose is required but it's not installed.  Aborting."; exit 1; }
        docker-compose -f docker-compose-gr.yml run develop bash
        ;;
    test)
        type docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose is required but it's not installed.  Aborting."; exit 1; }
        docker-compose -f docker-compose-test.yml build && docker-compose -f docker-compose-test.yml up --abort-on-container-exit
        ;;
    *)
        echo "Usage: aqueduct.sh {develop|shell|test}" >&2
        exit 1
        ;;
esac

exit 0
