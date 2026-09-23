# Flink 1.20 LTS with the four JARs this project needs. They are downloaded at
# build time rather than committed: they total about 150 MB, and a jar in a git
# repository is a supply-chain artifact nobody reviews.
#
# The versions are pinned and so are the bytes. A version pin names an
# artifact; it does not identify one. Each download below is checked against a
# sha512 recorded here, so a pinned version served as different bytes fails the
# build instead of being installed.
#
# ICEBERG 1.10.0 is not an arbitrary choice. It is the same version as the
# iceberg-rest-fixture image in compose.yaml. A Flink runtime built against a
# different Iceberg minor than the catalog it talks to is a class of failure
# that surfaces as an unrelated-looking serialization error at commit time.
#
# The base is pinned by digest as well as by patch version. A tag such as
# flink:1.20 moves with every patch and every rebuild of the JDK underneath
# it, so the same Dockerfile would build a different image on a different
# day. Re-derive the digest with `docker pull flink:1.20.5` and
# `docker image inspect flink:1.20.5 --format '{{json .RepoDigests}}'`.
FROM flink:1.20.5@sha256:4edbe20a69541be74b829b0571cc84855e63a5586b51efdef39a21f2dc93812d

# HADOOP is required even though nothing here uses HDFS. Iceberg's Flink
# catalog factory constructs an org.apache.hadoop.conf.Configuration
# unconditionally, so the class has to be on the classpath even when every byte
# is going to S3FileIO against MinIO. Without it the CREATE CATALOG statement
# fails with a bare ClassNotFoundException that names Hadoop and explains
# nothing. The shaded uber jar is one artifact instead of a transitive tree.
ARG HADOOP_UBER_VERSION=2.8.3-10.0
ARG ICEBERG_VERSION=1.10.0
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG M2=https://repo1.maven.org/maven2

# The digests are LITERALS and deliberately not ARGs. The versions above are
# ARGs because a reader may reasonably want to try another one; a checksum that
# could be overridden from the command line is a gate with an off switch, and
# `--build-arg SHA=<whatever the attacker served>` would pass while looking
# exactly like a pinned build. Re-derive them with, for each artifact:
#
#   curl -fsSL "$M2/<path>/<jar>" | sha512sum
#
# Maven Central publishes .sha512 beside the iceberg jars (both verified equal
# to these on 2026-09-16) and only .sha1 beside the two flink ones, which were
# verified against that weaker digest and then recorded here at sha512.
RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fsSLO "${M2}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar"; \
    printf '%s  %s\n' \
      "dee0d9a44a9c45ba2bb70a9eadc95eea5c19d18ec2ca368bea5858bbe055bd7572bef23d2abceabfb4bb75d5c6ce35be72fe75513e42ff97df4b73b1f3798036" \
      "iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar" \
      "150b885dd827713af0b105e655ec24a47f823ce7269d218522b7149fdb1606039458470e5006da31c9fdc5bb839c59447279e7968e04bd6d7984c57d46980ccb" \
      "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "b744f0f3c418a45cf23ac72d477f5f8ee73f8f96a5b733b21a953a0207facd52c5495383eaad2416dbffc17e9fa5b53b3cc49e70c3cb7a2ead38a7ecac0211df" \
      "flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      "c04d217fb53123054c58c5c492cc8d87e75aa72b798e3b4858757cb4d389ccd9866c224662a012e1e9b012c05e481372e2bbc97cd45746f924814733d864591f" \
      "flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar" \
      > /tmp/jars.sha512; \
    sha512sum -c /tmp/jars.sha512; \
    rm -f /tmp/jars.sha512; \
    ls -l iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
          iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
          flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
          flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar

# The checkpoint directory must exist in the image, owned by the FLINK user.
# Docker seeds an empty named volume from the image path it is mounted over,
# ownership included. Without this the volume mounts as root, Flink runs as
# uid 9999, and the job dies with "Failed to create directory for shared
# state" (a message that points at the state backend rather than at a
# permission problem).
RUN mkdir -p /flink-state/checkpoints /flink-state/savepoints \
 && chown -R flink:flink /flink-state
