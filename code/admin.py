from argparse import ArgumentParser
from settings import admin, BOOTSTRAP_SERVERS
from confluent_kafka.admin import NewTopic
from confluent_kafka import Producer
from loggers import logger
from concurrent.futures import wait


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
        ftr = admin.create_topics(new_topics=[new,], operation_timeout=10, request_timeout=5)
        # .create_topics return futures, must wait
        for t, f in ftr.items(): 
            wait([f])
        logger.info(f'topic {args.create_topic} created!')
        
    if args.list_topic:
        topics = admin.list_topics().topics
        logger.info(f'topics list: {[i for i in topics]}')