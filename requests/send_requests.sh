#!/bin/bash
num_requests=1000

while [[ $# -gt 0 ]] ;
do
    case "$1" in
    -n | --num-requests)
        num_requests="$2"
        shift 2
        ;;
    --help)
        echo "Usage: $0 --language/-l [go|rust] [--num-requests/-n <NUMBER>]"
        echo "  --num-requests  Number of requests to run. Defaults to 1000."
        exit 0
        ;;
    *)
        echo "Unknown option: $1"
        exit 1
        ;;
    esac
done

running=$(lsof -i :8000)

if [[ -z $running ]]
then
    echo "Nothing running on port 8000, exiting..."
    exit 1
fi

go run main.go $num_requests
