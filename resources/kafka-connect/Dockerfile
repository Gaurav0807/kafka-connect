FROM confluentinc/cp-kafka-connect:7.7.1

# Switch to root to install packages and create directories
USER root

# Install S3 connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.3.0

# Install Confluent transforms SMTs
RUN confluent-hub install --no-prompt confluentinc/connect-transforms:1.6.2

# Create directories for schemas and custom plugins
RUN mkdir -p /opt/kafka/test-schemas /opt/kafka/custom-plugins && \
    chmod -R 777 /opt/kafka/test-schemas /opt/kafka/custom-plugins

# Switch back to appuser
USER appuser