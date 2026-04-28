from confluent_kafka import Producer
from settings import BOOTSTRAP_SERVERS

prod = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
prod.produce('trigger', 'close')
prod.flush()