#!/bin/bash

echo "Building fat jar with all dependencies..."

# Clean previous builds
mvn clean

# Package the project with all dependencies
mvn package

echo "Fat jar created: target/benchmark_example.jar"
echo "You can run it with: java -jar target/benchmark_example.jar" 