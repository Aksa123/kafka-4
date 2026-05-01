from argparse import ArgumentParser
from settings import admin, BOOTSTRAP_SERVERS
from confluent_kafka.admin import NewTopic
from confluent_kafka import Producer
from loggers import logger
from utils import create_topics_and_wait


if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument('--create-topic', type=str, required=False)
    parser.add_argument('--partitions', type=int, default=-1)
    parser.add_argument('--list-topic', const=True, nargs='?', required=False)
    args = parser.parse_args()

    prod = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    if args.create_topic:
        conf = {'cleanup.policy': 'delete'}
        new = NewTopic(args.create_topic, num_partitions=args.partitions, config=conf)
        create_topics_and_wait(new_topics=[new])
        logger.info(f'topic {args.create_topic} created!')
        
    if args.list_topic:
        topics = admin.list_topics().topics
        logger.info(f'topics list: {[i for i in topics]}')