from confluent_kafka import Consumer
from psycopg.sql import SQL, Identifier, Placeholder
from collections import deque
from settings import BOOTSTRAP_SERVERS, GROUP_ID, GROUP_INSTANCE_ID,\
                 TRIGGER_CODE_CHECK_NEW_TOPICS, TRIGGER_CODE_LIST_TOPICS, TRIGGER_CODE_FLUSH, TRIGGER_CODE_CLOSE
from utils import get_topics_by_regex, generate_query
from loggers import logger
from connections import conn_pg
from external.slack import slack_pusher
import json
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


def validate_json(data: str) -> bool:
    try:
        json.loads(data)
        return True
    except Exception as err:
        return False


def flush_to_db():
    if not queue:
        return
    conn_pg.execute_pipeline(queue)
    text = 'Flushed:\n' + '\n'.join([v['json_data'] for k,v in queue])
    logger.info(text)
    queue.clear()
    

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
                        flush_to_db()
                        consumer.commit()
                        consumer.subscribe(topics)
                        logger.info(f'New subscription topics: {topics}' )
                        continue
                    else:
                        logger.info('No new topics...')
                
                elif value == TRIGGER_CODE_LIST_TOPICS:
                    logger.info('Topic list', topics)
                
                elif value == TRIGGER_CODE_CLOSE:
                    logger.info('Shutting down gracefully...')
                    flush_to_db()
                    consumer.commit()
                    consumer.close()
                    break

                elif value == TRIGGER_CODE_FLUSH:
                    flush_to_db()
                    
                else:
                    logger.info(f'Invalid trigger code: {value}')
            
            else:
                if not validate_json(value):
                    consumer.commit()
                    logger.warning('Data is not JSON-formatted')
                    continue
                table_name = re.sub(r'[\s\.-]+', '_',  topic)    # Replace dot and dash with underscore
                query = generate_query(table_name, value)
                queue.append(query)
            
            consumer.commit()

        except KeyboardInterrupt:
            flush_to_db()
            consumer.commit()
            consumer.close()
            break

        except Exception as err:
            logger.error(err)
            slack_pusher.notify_slack(f"Kafka Consumer Error: \n{err}")
            

if __name__ == '__main__':
    logger.info(f'<< Starting consumer {GROUP_INSTANCE_ID} >>')
    start()