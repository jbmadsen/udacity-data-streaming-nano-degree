# Optimizing Public Transportation

## Kafka Producer

| Status | Criteria |
| --- | --- | 
| [x] | Kafka topics are created with appropriate settings | 
| [x] | Kafka messages are produced successfully | 
| [x] | All messages have an associated value schema | 


## Kafka Consumer

| Status | Criteria |
| --- | --- | 
| [x] | Messages are consumed from Kafka | 
| [x] | Stations data is consumed from the beginning of the topic | 


## Kafka REST Proxy

| Status | Criteria |
| --- | --- | 
| [x] | Kafka REST Proxy successfully delivers messages to the Kafka Topic | 
| [x] | Messages produced to the Kafka REST Proxy include a value schema | 


## Kafka Connect

| Status | Criteria |
| --- | --- | 
| [x] | Kafka Connect successfully loads Station data from Postgres to Kafka | 
| [x] | Kafka Connect is configured to define a Schema | 
| [x] | Kafka Connect is configured to load on an incrementing ID | 


## Faust Streams

| Status | Criteria |
| --- | --- | 
| [x] | The Faust application ingests data from the stations topic | 
| [x] | Data is translated correctly from the Kafka Connect format to the Faust table format | 
| [x] | Transformed Station Data is Present for each Station ID in the Kafka Topic | 


## KSQL

| Status | Criteria |
| --- | --- | 
| [x] | Turnstile topic is translated into a KSQL Table | 
| [x] | Turnstile table is aggregated into a summary table | 

