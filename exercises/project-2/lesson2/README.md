# Create a Spark Streaming Dataframe with a Kafka source and write it to the console

> (for Udacity Workspace)

- Click the button to start Kafka and Banking Simulation: <button id="ulab-button-341c8451" class="ulab-btn--primary"></button>
- Start the spark master and spark worker
     - From the terminal type: ```/home/workspace/spark/sbin/start-master.sh```
     - View the log and copy down the Spark URI
     - From the terminal type: ```/home/workspace/spark/sbin/start-slave.sh [Spark URI]```
- Complete the kafkaconsole.py python script
- Submit the application to the spark cluster:
     - From the terminal type: 
     ```/home/workspace/submit-kakfaconsole.sh```
- Watch the terminal for the values to scroll past
