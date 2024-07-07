#!/usr/bin/env bash
set -e

docker run --name hadoop-build --rm -itd \
    -v "./:/home/hadoop/hdfs-trace-repair" \
    -v "${HOME}/.m2:/home/hadoop/.m2"  \
    -h hadoop-build \
    hadoop-build

docker exec hadoop-build mvn package -Pdist,native -Dtar -DskipTests -Dmaven.javadoc.skip

docker stop hadoop-build 
