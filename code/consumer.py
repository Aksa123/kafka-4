from confluent_kafka import Consumer
from collections import deque
from settings import BOOTSTRAP_SERVERS, GROUP_ID, GROUP_INSTANCE_ID,\
                 TRIGGER_CODE_CHECK_NEW_TOPICS, TRIGGER_CODE_LIST_TOPICS, TRIGGER_CODE_FLUSH, TRIGGER_CODE_CLOSE
from utils import get_topics_by_regex
from loggers import logger
import re


queue = deque()

def generate_consumer():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'group.instance.id': GROUP_INSTANCE_ID,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    return consumer


def start():
    pattern = r'^paytm_.*'
    topics = get_topics_by_regex(r=pattern)
    consumer = generate_consumer()
    consumer.subscribe(topics=topics)
    logger.info(f'Consumer {GROUP_INSTANCE_ID} subscribing to: {topics}')

    while True:
        try:
            msg = consumer.poll()
            if not msg:
                continue
            # Handle consumer by trigger message value
            elif msg.topic() == 'trigger':
                val = msg.value().decode()
                if val == TRIGGER_CODE_CHECK_NEW_TOPICS:
                    new_relevant_topics = get_topics_by_regex(r=pattern)
                    if len(new_relevant_topics) > len(topics):
                        topics = new_relevant_topics
                        logger.info('Resubscribing...')
                        consumer.subscribe(topics)
                        logger.info(f'New subscription topics: {topics}' )
                    else:
                        logger.info('No new topics...')
                
                elif val == TRIGGER_CODE_LIST_TOPICS:
                    logger.info('Topic list', topics)
                
                elif val == TRIGGER_CODE_CLOSE:
                    logger.info('Shutting down gracefully...')
                    consumer.close()
                    break

                elif val == TRIGGER_CODE_FLUSH:
                    if not queue:
                        consumer.commit()
                        continue
                    bulk_text = f"{GROUP_INSTANCE_ID}\n"
                    for c,i in enumerate(queue, start=1):
                        bulk_text += (str(c) + '. ' + i + '\n')
                    logger.info(bulk_text)
                    queue.clear()
                    consumer.commit()   # Acknowledge messages after successful flush
                
                else:
                    logger.info(f'Invalid trigger code: {val}')
                
            else:
                text = msg.topic() + ' - ' + msg.value().decode()
                queue.append(text)

        except KeyboardInterrupt:
            consumer.close()
            break

if __name__ == '__main__':
    logger.info(f'<< Starting consumer {GROUP_INSTANCE_ID} >>')
    start()