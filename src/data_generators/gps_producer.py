import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

class GPSDataProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.fake = Faker()
        
        # Dubai coordinates boundaries
        self.dubai_bounds = {
            'lat': (25.0, 25.3),
            'lon': (55.1, 55.4)
        }
        
    def generate_gps_data(self, source_id):
        status_choices = ['Moving', 'Idle', 'Stopped']
        area_choices = ['Dubai', 'Sharjah', 'Abu Dhabi', 'Ajman']
        
        return {
            'SourceID': source_id,
            'CustomerID': f'CUST{random.randint(1000, 9999)}',
            'CarID': f'CAR{random.randint(100, 999)}',
            'OfficeID': f'OFF{random.randint(10, 99)}',
            'AgentID': f'AGT{random.randint(100, 999)}',
            'TrxnTimestamp': datetime.now().isoformat(),
            'CarDrivingStatus': random.choice(status_choices),
            'CurrentLongitude': random.uniform(self.dubai_bounds['lon'][0], self.dubai_bounds['lon'][1]),
            'CurrentLatitude': random.uniform(self.dubai_bounds['lat'][0], self.dubai_bounds['lat'][1]),
            'CurrentArea': random.choice(area_choices),
            'KM': round(random.uniform(0, 1000), 2)
        }
    
    def produce_data(self, interval=1, num_messages=None):
        count = 0
        while True if num_messages is None else count < num_messages:
            for src_id in range(1, 5):
                data = self.generate_gps_data(f"src0{src_id}")  # Include SourceID
                self.producer.send(self.topic, data)  # Send to the single topic
                print(f"Sent data to topic {self.topic}: {data}")
            
            count += 1
            time.sleep(interval)


if __name__ == '__main__':
    gps_producer = GPSDataProducer(bootstrap_servers='kafka:29092', topic='gps_data')
    gps_producer.produce_data()
