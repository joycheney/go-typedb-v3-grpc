#!/bin/bash

# Generate Go code from proto files
echo "Generating Go code from proto files..."

# Create pb directory if it doesn't exist
mkdir -p pb

# Generate Go code for each proto file
protoc --go_out=pb --go_opt=paths=source_relative \
       --go-grpc_out=pb --go-grpc_opt=paths=source_relative \
       -I proto \
       proto/*.proto

echo "Proto generation complete!"