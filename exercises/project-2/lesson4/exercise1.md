# Manually Save and Read With Redis and Kafka

- Click the button to start Kafka: <button id="ulab-button-341c8451" class="ulab-btn--primary"></button>
- Run the Redis CLI:
     - From the terminal type: ```/data/redis/redis-stable/src/redis-cli -a notreally```
     - From the terminal type: ```keys **```
     - You will see the list of all the Redis tables
     - From another terminal type ```/data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic redis-server```
     - From the first terminal type: ```zadd testkey 0 testvalue```
     - Open the second terminal
     - A JSON message will appear from the redis-server topic
     - The key contains the base64 encoded name of the Redis table
     - zSetEntries and zsetEntries contain the changes made to the Redis sorted set. 
- To decode the name of the Redis table, open a third terminal, and type: ```echo "[encoded table]" | base64 -d```, for example ```echo "dGVzdGtleQ==" | base64 -d```
- To decode the changes made, from the zSetEntries table, copy the "element" value, then from the third terminal type: ```echo "[encoded value]" | base64 -d```, for example ```echo "dGVzdGtleQ==" | base64 -d```

