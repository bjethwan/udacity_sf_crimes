from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        with open(self.input_file, 'r') as f:
            messages = json.loads(f.read())
            for message in messages:
                messageBytes = self.dict_to_binary(message)
                self.send(self.topic, messageBytes)
                time.sleep(1)

    def dict_to_binary(self, json_dict):
        return bytes(json.dumps(json_dict), "utf-8")
        