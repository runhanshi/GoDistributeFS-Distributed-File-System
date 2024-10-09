#!/bin/sh

protoc --go_out=../pb --go-grpc_out=../pb -I. *.proto
