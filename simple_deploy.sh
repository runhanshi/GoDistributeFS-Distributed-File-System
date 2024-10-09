#!/bin/bash

echo "Building Storage server"
go build -o StorageServer storage_server/main/main.go
echo "Building Naming server"
go build -o NamingServer naming_server/main/main.go
echo "Built"

mkdir -p run/naming_server
mkdir -p run/storage_server_1
mkdir -p run/storage_server_2

export NAMING_SERVER_ADDRESS="localhost"
export NAMING_SERVER_PORT="5678"
export STORAGE_SERVER_1_PORT="1967"
export STORAGE_SERVER_2_PORT="1968"
export STORAGE_SERVER_1_ALIAS="storage01"
export STORAGE_SERVER_2_ALIAS="storage02"

bash -c "cd run/naming_server \
  && ADDRESS=0.0.0.0:${NAMING_SERVER_PORT} \
     ../../NamingServer" &
bash -c "sleep 1 && cd run/storage_server_1 \
  && ADDRESS=0.0.0.0:${STORAGE_SERVER_1_PORT} \
     NAMING_SERVER_ADDRESS=${NAMING_SERVER_ADDRESS}:${NAMING_SERVER_PORT} \
     ALIAS=${STORAGE_SERVER_1_ALIAS} \
     PUBLIC_HOSTNAME=localhost \
     ../../StorageServer" &
bash -c "sleep 1 && cd run/storage_server_2 \
  && ADDRESS=0.0.0.0:${STORAGE_SERVER_2_PORT} \
     NAMING_SERVER_ADDRESS=${NAMING_SERVER_ADDRESS}:${NAMING_SERVER_PORT} \
     ALIAS=${STORAGE_SERVER_2_ALIAS} \
     PUBLIC_HOSTNAME=localhost \
     ../../StorageServer" &

echo "Servers started. Type anything to stop. Don't forget to press Return in the end."

# Read user input to a dummy variable to wait
# shellcheck disable=SC2034
read -r var1

pkill NamingServer
pkill StorageServer
echo "All servers killed."

