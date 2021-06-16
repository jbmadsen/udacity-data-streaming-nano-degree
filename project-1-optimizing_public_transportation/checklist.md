# Optimizing Public Transportation

## Kafka Producer

| Status | Criteria |
| --- | Kafka topics are created with appropriate settings | 
| [ ] | Kafka messages are produced successfully | 
| [ ] | All messages have an associated value schema | 


## Kafka Consumer

| Status | Criteria |
| --- | --- | 
| [ ] | Messages are consumed from Kafka | 
| [ ] | Stations data is consumed from the beginning of the topic | 


## Kafka REST Proxy

| Status | Criteria |
| --- | --- | 
| [ ] | Kafka REST Proxy successfully delivers messages to the Kafka Topic | 
| [ ] | Messages produced to the Kafka REST Proxy include a value schema | 


## Kafka Connect

| Status | Criteria |
| --- | --- | 
| [ ] | Kafka Connect successfully loads Station data from Postgres to Kafka | 
| [ ] | Kafka Connect is configured to define a Schema | 
| [ ] | Kafka Connect is configured to load on an incrementing ID | 


## Faust Streams

| Status | Criteria |
| --- | --- | 
| [ ] | The Faust application ingests data from the stations topic | 
| [ ] | Data is translated correctly from the Kafka Connect format to the Faust table format | 
| [ ] | Transformed Station Data is Present for each Station ID in the Kafka Topic | 


## KSQL

| Status | Criteria |
| --- | --- | 
| [ ] | Turnstile topic is translated into a KSQL Table | 
| [ ] | Turnstile table is aggregated into a summary table | 

