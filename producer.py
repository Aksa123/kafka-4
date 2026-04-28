from confluent_kafka import Producer
from settings import BOOTSTRAP_SERVERS
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--topic', type=str, required=True)
parser.add_argument('--msg', type=str, required=True)
args = parser.parse_args()

"""
python3 producer.py --topic mytopic --msg mymessage
"""

prod = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
prod.produce(topic=args.topic, value=args.msg)
prod.flush()