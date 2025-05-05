# Kafka IAM Producer

A simple Kafka producer application that connects to Amazon MSK with IAM authentication.

## Prerequisites

- Java 11 or later
- Maven
- AWS credentials configured in your environment

## Build

```bash
mvn clean package
```

## Run

```bash
java -jar target/kafka-iam-producer-1.0-SNAPSHOT.jar
```

## Configuration

The application is configured to:
- Connect to MSK cluster using IAM authentication
- Produce 5 test messages to the `test_iam` topic
- Use the specified bootstrap servers

## Note

This application assumes that AWS credentials are available in the environment through one of the standard AWS credential providers:
- Environment variables
- Java system properties
- Credential profiles file (~/.aws/credentials)
- EC2 instance profile or ECS task role