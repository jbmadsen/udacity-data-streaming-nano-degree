"""Defines trends calculations for stations"""
import logging
from os import stat

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that:
# 1) ingests data from the Kafka Connect stations topic, and
# 2) places it into a new topic with only the necessary information.
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#application-parameters
# "id" also defines the Consumer Group. See: https://faust.readthedocs.io/en/latest/userguide/settings.html#guide-settings
app = faust.App(id="stations-stream", broker="kafka://localhost:9092", store="memory://")

# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-topic-create-a-topic-description
# For topic name, see: producers/connector.py: "topic.prefix"
topic = app.topic("org.chicago.cta.stations", value_type=Station)

# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.transformed", partitions=1)

# TODO: Define a Faust Table
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-table-define-a-new-table
table = app.Table(
    "org.chicago.cta.stations.table.transformed",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. 
# Note that "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
# Based on: https://readthedocs.org/projects/faust/downloads/pdf/latest/ (page 8), 
# the input parameter and return has been updated to reflect what I suspect is more correct 
@app.agent(topic)
async def process_transform_station(stations: faust.Stream[Station]) -> None:
    # Converting line boolean to string text lambda
    convert_line = lambda line: "red" if line.red else "green" if line.green else "blue"
    # Process data
    async for station in stations:
        #print("station.station_name:", station.station_name)
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=convert_line(station)
        )


if __name__ == "__main__":
    app.main()
