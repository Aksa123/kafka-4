from confluent_kafka.admin import NewTopic
from concurrent.futures import wait
from settings import admin
from producer import prod
import re

def get_topics_by_regex(r: str = r'^paytm_.*'):
    topics_all =  [i for i in admin.list_topics().topics]
    topics_paytm = list(filter(lambda n: re.match(r, n), topics_all))
    topics_paytm.extend(['trigger'])
    return topics_paytm


def create_topics_and_wait(new_topics: list[NewTopic], **kwargs):
    ftr = admin.create_topics(new_topics, **kwargs)
    wait([f for f in ftr.values()])
    return True


TRIGGER_PARTITIONS = 2
def create_trigger_topic():
    t = NewTopic('trigger', 
            config={
                'cleanup.policy': 'delete',
                'retention.ms': 10000,
                'segment.ms': 60000},
            num_partitions=TRIGGER_PARTITIONS    # As many as the consumer nodes
    )
    create_topics_and_wait([t])
    

# Send to ALL partitions
def send_trigger_message(msg):
    for i in range(0, TRIGGER_PARTITIONS):
        prod.produce(topic='trigger', key=str(i), value=msg, partition=i)
        prod.flush()