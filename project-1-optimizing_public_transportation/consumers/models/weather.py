"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        # TODO: Process incoming weather messages. Set the temperature and status.
        try:
            # See producers/models/schemas/weather_value.json for message.value() content
            weather_data = message.value()
            self.temperature = weather_data["temperature"]
            self.status = weather_data["status"]
        except Exception as e:
            logger.error(f"Exception parsing weather message: {str(e)}")
