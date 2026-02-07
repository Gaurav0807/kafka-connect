# kafka-connect
Github Repo Description :- Kafka Connect setup and creation of custom SMT 

** Apache Kafka 

Apache Kafka is a real-time data streaming system.

kafka is Different from Queue. In kafka we can reprocess data stays for retention period. In kafka we can have many consumers. 

Typical Pipeline:- 

** Apps → Kafka → Spark/Flink → S3/Delta → BI

Apache Kafka do One Job Durably store and streams events.

Kafka Connect Job is :- 
Move data between Kafka and External System with Zero Code.

Two types of connectors :- 
1.) Source Connector :- External System --> Kafka
2.) Sink Connector :- Kafka --> External System

If you don't want to write any code, Kafka Connect is easier because it's just JSON to configure and run

Kafka Connect supports Single Message Transform for making changes to data as it passes through the pipeline (dropping fields, changing data types,Fillig up null values  etc ).


# Kafka UI


![Kafka Connect UI](/Kafka_UI.png)