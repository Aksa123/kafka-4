from confluent_kafka import Producer
from settings import BOOTSTRAP_SERVERS
from argparse import ArgumentParser

prod = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--topic', type=str, required=True)
    parser.add_argument('--msg', type=str, required=True)
    parser.add_argument('--part', type=int, default=0)
    args = parser.parse_args()

    """
    python3 producer.py --topic mytopic --msg mymessage
    """


    prod.produce(topic=args.topic, value=args.msg, partition=args.part)
    prod.flush()