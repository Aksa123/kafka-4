from confluent_kafka import Producer
from settings import BOOTSTRAP_SERVERS, TRIGGER_CODE_CHECK_NEW_TOPICS, TRIGGER_CODE_FLUSH, TRIGGER_CODE_CLOSE
from utils import get_topics_by_regex, send_trigger_message
from time import sleep
from loggers import logger


if __name__ == '__main__':
    INTERVAL = 20
    topics = get_topics_by_regex()
    
    while True:
        prod = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
        relevant_topics = get_topics_by_regex()
        new_topics = list(filter(lambda x: x not in topics, relevant_topics))
        if new_topics:
            logger.info('new topics detected! sending trigger msg...')
            send_trigger_message(TRIGGER_CODE_CHECK_NEW_TOPICS)
            topics = relevant_topics
        
        send_trigger_message(TRIGGER_CODE_FLUSH)

        sleep(INTERVAL)
