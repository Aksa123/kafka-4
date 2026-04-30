from confluent_kafka.admin import NewTopic
from settings import admin
from loggers import logger
from utils import create_trigger_topic


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
    admin.create_topics(new_topics=topics_obj, operation_timeout=10, request_timeout=5)
    logger.info(admin.list_topics().topics)