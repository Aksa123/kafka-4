from confluent_kafka.admin import NewTopic
from settings import admin
from loggers import logger
from utils import create_topics_and_wait, create_trigger_topic


topics = ['paytm_products', 'paytm_categories', 'flip_users']
topics_obj = []
for t in topics:
    topic_conf = {
        'cleanup.policy': 'delete',
    }
    obj = NewTopic(t, config=topic_conf)
    topics_obj.append(obj)


if __name__ == '__main__':
    create_trigger_topic()
    create_topics_and_wait(new_topics=topics_obj, operation_timeout=10, request_timeout=5)
    logger.info(admin.list_topics().topics)