# Flink 1.20 LTS with the three JARs this project needs. They are downloaded at
# build time rather than committed: they total about 100 MB, and a jar in a git
# repository is a supply-chain artifact nobody reviews. The versions are pinned.
#
# ICEBERG 1.10.0 is not an arbitrary choice. It is the same version as the
# iceberg-rest-fixture image in compose.yaml. A Flink runtime built against a
# different Iceberg minor than the catalog it talks to is a class of failure
# that surfaces as an unrelated-looking serialization error at commit time.
FROM flink:1.20

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

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fsSLO "${M2}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar"; \
    ls -l iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
          iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
          flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

# The checkpoint directory must exist in the image, owned by the FLINK user.
# Docker seeds an empty named volume from the image path it is mounted over,
# ownership included. Without this the volume mounts as root, Flink runs as
# uid 9999, and the job dies with "Failed to create directory for shared
# state" (a message that points at the state backend rather than at a
# permission problem).
RUN mkdir -p /flink-state/checkpoints /flink-state/savepoints \
 && chown -R flink:flink /flink-state
