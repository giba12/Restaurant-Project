FROM quay.io/strimzi/kafka:1.2.0-kafka-4.3.1
USER root:root
ENV CONNECTOR_VERSION=4.18.0
RUN mkdir -p /opt/kafka/plugins/camel-mqtt && \
    curl -fSL -o /tmp/camel-mqtt.tar.gz \
      https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector/camel-mqtt-source-kafka-connector/${CONNECTOR_VERSION}/camel-mqtt-source-kafka-connector-${CONNECTOR_VERSION}-package.tar.gz && \
    tar -xzf /tmp/camel-mqtt.tar.gz -C /opt/kafka/plugins/camel-mqtt --strip-components=1 && \
    rm /tmp/camel-mqtt.tar.gz && \
    rm -f /opt/kafka/plugins/camel-mqtt/connect-json-*.jar
USER 1001
