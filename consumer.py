from confluent_kafka import Consumer
from collections import deque
from settings import BOOTSTRAP_SERVERS, TRIGGER_CODE_CHECK_NEW_TOPICS, TRIGGER_CODE_LIST_TOPICS, TRIGGER_CODE_FLUSH, TRIGGER_CODE_CLOSE
from admin import admin
import re


queue = deque()

def generate_consumer():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'axa',
        'group.instance.id': 'axa-1',
        'enable.auto.commit': 'true',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    return consumer


def get_topics_by_regex(r: str):
    topics_all =  [i for i in admin.list_topics().topics]
    topics_paytm = list(filter(lambda n: re.match(r, n), topics_all)) 
    topics_paytm.extend(['trigger'])
    return topics_paytm


def start():
    pattern = r'^paytm_.*'
    topics = get_topics_by_regex(r=pattern)
    consumer = generate_consumer()
    consumer.subscribe(topics=topics)
    print('subscribing to ', topics)

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
                        print('resubscribing...')
                        consumer.subscribe(topics)
                        print('new subscription topics', topics )
                
                elif val == TRIGGER_CODE_LIST_TOPICS:
                    print('topic list', topics)
                
                elif val == TRIGGER_CODE_CLOSE:
                    print('shutting down gracefully...')
                    consumer.close()
                    break

                elif val == TRIGGER_CODE_FLUSH:
                    bulk_text = ""
                    for c,i in enumerate(queue, start=1):
                        bulk_text += (str(c) + '. ' + i + '\n')
                    print(bulk_text)
                    queue.clear()
                
                else:
                    print(f'invalid trigger code: {val}')
                
            else:
                text = msg.topic() + ' - ' + msg.value().decode()
                queue.append(text)

        except KeyboardInterrupt:
            consumer.close()
            break

if __name__ == '__main__':
    start()