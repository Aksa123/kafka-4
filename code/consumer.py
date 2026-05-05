from confluent_kafka import Consumer
from psycopg.sql import SQL, Identifier, Placeholder
from collections import deque
from settings import BOOTSTRAP_SERVERS, GROUP_ID, GROUP_INSTANCE_ID,\
                 TRIGGER_CODE_CHECK_NEW_TOPICS, TRIGGER_CODE_LIST_TOPICS, TRIGGER_CODE_FLUSH, TRIGGER_CODE_CLOSE
from utils import get_topics_by_regex, generate_query, create_schema_if_not_exists, create_table_if_not_exists
from loggers import logger
from connections import conn_pg
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
            
            topic = msg.topic()
            value = msg.value().decode()

            # Handle consumer by trigger message value
            if topic == 'trigger':
                if value == TRIGGER_CODE_CHECK_NEW_TOPICS:
                    new_relevant_topics = get_topics_by_regex(r=pattern)
                    if len(new_relevant_topics) > len(topics):
                        topics = new_relevant_topics
                        logger.info('Resubscribing...')
                        consumer.subscribe(topics)
                        logger.info(f'New subscription topics: {topics}' )
                    else:
                        logger.info('No new topics...')
                
                elif value == TRIGGER_CODE_LIST_TOPICS:
                    logger.info('Topic list', topics)
                
                elif value == TRIGGER_CODE_CLOSE:
                    logger.info('Shutting down gracefully...')
                    consumer.close()
                    break

                elif value == TRIGGER_CODE_FLUSH:
                    if not queue:
                        consumer.commit()
                        continue
                    
                    conn_pg.execute_pipeline(queue)
                    
                    # text = 'Flushed:\n' + '\n'.join([v['json_data'] for k,v in queue])
                    # logger.info(text)
                
                    queue.clear()
                    consumer.commit()   # Acknowledge messages after successful flush
                else:
                    logger.info(f'Invalid trigger code: {value}')
            
            else:
                table_name = re.sub('[\.-]', '_', topic)
                query = generate_query(table_name, value)
                queue.append(query)

        except KeyboardInterrupt:
            consumer.close()
            break

if __name__ == '__main__':
    logger.info(f'<< Starting consumer {GROUP_INSTANCE_ID} >>')
    start()