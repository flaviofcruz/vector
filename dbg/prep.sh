#!/bin/bash
set -e

cargo build
mkdir -p dbg
cp target/debug/vector dbg/vector
strip dbg/vector
docker build -f dbg/debug.dockerfile -t vector-debug:latest ./dbg
docker tag vector-debug:latest harbor-aws-eu-west-1.dev.databricks.com/manual/universe/vector:custom
docker push harbor-aws-eu-west-1.dev.databricks.com/manual/universe/vector:custom
kubectl delete -f dbg/pod.yml --ignore-not-found
kubectl apply -f dbg/pod.yml
