# Portability-only variant of k8s/kafka-connect-mqtt/docker/Dockerfile.
# That one is built on quay.io/strimzi/kafka -- a Strimzi-specific image
# whose entrypoint/env-var contract is designed for the Strimzi operator
# to drive, not for standalone `docker run`/Compose use. This uses the
# official apache/kafka image instead (Apache 2.0, includes the full
# Kafka distribution's bin/ scripts, connect-distributed.sh among them)
# with the exact same Camel MQTT connector plugin installed the same way.
FROM docker.io/apache/kafka:4.3.1

USER root
ENV CONNECTOR_VERSION=4.18.0
# apache/kafka's base image is Alpine, not Debian (confirmed by actually
# running this build, not assumed) -- no curl and no apt-get, but wget is
# already present, so use that instead of adding a package.
RUN mkdir -p /opt/kafka/plugins/camel-mqtt && \
    wget -O /tmp/camel-mqtt.tar.gz \
      https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector/camel-mqtt-source-kafka-connector/${CONNECTOR_VERSION}/camel-mqtt-source-kafka-connector-${CONNECTOR_VERSION}-package.tar.gz && \
    tar -xzf /tmp/camel-mqtt.tar.gz -C /opt/kafka/plugins/camel-mqtt --strip-components=1 && \
    rm /tmp/camel-mqtt.tar.gz && \
    rm -f /opt/kafka/plugins/camel-mqtt/connect-json-*.jar

COPY connect-worker.properties /opt/kafka/config/connect-worker.properties

# Root was only needed to install the plugin above. The base image's own
# unprivileged user runs the worker, matching the k8s variant's USER 1001
# (found by tests/static/test_dockerfiles.py: this image used to stay root).
USER appuser

ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/connect-worker.properties"]
