# Real-Time-Road-Network-Data-Processing-and-Analysis
Spark-Kafka-HBase Real-Time Data Processing Framework

# Project description:
This project implements a real-time road network data processing and analysis system based on Spark, Kafka, and HBase. The goal of the system is to obtain road network information from real-time data streams, process and analyze it, and store the results in HBase. The specific functions are as follows:

# Real-time data collection and processing:
Kafka is used as a message queue, combined with Spark Streaming to achieve real-time processing of road network data streams.
Application of graph algorithms: Spark GraphX is used to build a road network graph, process the data stored in HDFS, and apply the Pregel algorithm for graph calculations such as shortest path.

# Data persistence: 
The processed data is stored in HBase to achieve data persistence and fast query.

# Technology stack:
Big data processing: Spark Core, Spark Streaming, Spark GraphX
Message queue: Kafka
Distributed database: HBase
Distributed file system: HDFS

# Functional highlights:
An efficient real-time data processing framework that can support real-time stream processing of a large amount of road network data.
The Pregel algorithm is applied for complex graph calculations and path queries.
Efficient data storage and fast query are achieved by using HBase.

# Project limitations and future prospects:
The current project has not yet implemented a web interface, and users cannot interact through a graphical interface.
In the future, it is planned to optimize the system performance, add a web interface to display real-time data and processing results, and further expand the project functions.

# Contribution statement:
Responsible for the design and implementation of the entire data processing flow, including the consumption and production of Kafka message streams, Spark Streaming processing, and the application of graph calculation algorithms.
