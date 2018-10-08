# Visual-Analytics-Framework-for-High-Dimensional-Data-Streams
The whole project is developed having intention to do master thesis which core task is to develop web-based framework to do dimensionality reduction of streaming data. Though, the project is all about the streaming data but due to the lack of the streaming data source static csv files are converted into the streaming data source.

### Presentation Layer
Technology used: React.js
Folder: User_Interface

For visualization, plotly.js is used/

### Business Layer
Technology used: Spring, pySpark
Folder: spark_spring_kafka_visualization_project

Two algorithms are imlpemented using pyspark. The algorithm implementation along with other tasks are done in https://github.com/khanhafizurrahman80/Visual-Analytics-Framework-for-High-Dimensional-Data-Streams/blob/master/KafkaStreamAnalysis/sparkStreamingProject_draft/index.py. The file is executed from the Spring side by executing a bash script file (spark_start.sh).There is another bash script known as (kafka_start.sh) which is used to start the Apache Kafka 

### Persistance Layer
Technology used: Apache Kafka

### Demo Data set:
Source: https://archive.ics.uci.edu/ml/datasets/daily+and+sports+activities
Preprocessing task: the preprocessing task like to convert image to array data and others are listed into activity_recognition_preprocessing_code.



