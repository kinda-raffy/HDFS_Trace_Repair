#!/usr/bin/env bash
# set -e

# docker run --name hadoop-test --rm -itd \
#     -v "./:/home/hadoop/hdfs-trace-repair" \
#     -v "${HOME}/.m2:/home/hadoop/.m2"  \
#     -p "5005:5005" \
#     -h hadoop-test \
#     hadoop-build

# docker exec hadoop-test mvn -Dtest="TestReconstructStripedFile#testRecoverOneDataBlockSmallFile" \
#     -DfailIfNoTests=false \
#     -Dmaven.surefire.skip=true \
#     -Dmaven.surefire.debug \
#     test

# docker exec hadoop-test exit

mvn -Dtest="TestReconstructStripedFile#testRecoverOneDataBlockSmallFile" \
    -DfailIfNoTests=false \
    -Dmaven.surefire.skip=true \
    -Dmaven.surefire.debug \
    test