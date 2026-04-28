from confluent_kafka.admin import AdminClient, NewTopic

from settings import BOOTSTRAP_SERVERS

admin_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS
}
admin = AdminClient(conf=admin_conf)


topics = ['paytm_products', 'paytm_categories', 'flip_users']
topics_obj = []
for t in topics:
    topic_conf = {
        'cleanup.policy': 'delete',
    }
    obj = NewTopic(t, config=topic_conf)
    topics_obj.append(obj)

# Trigger topic msg is removed every 10s 
topics_obj.append(
    NewTopic('trigger', 
             config={
                    'cleanup.policy': 'delete',
                    'retention.ms': 10000
            },
            num_partitions=1
    ))

if __name__ == '__main__':
    admin.create_topics(new_topics=topics_obj, operation_timeout=10, request_timeout=5)
    print(admin.list_topics().topics)