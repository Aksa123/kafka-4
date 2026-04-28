from confluent_kafka import Consumer
from collections import deque
from settings import BOOTSTRAP_SERVERS
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


def get_relevant_topics():
    topics_all =  [i for i in admin.list_topics().topics]
    topics_paytm = list(filter(lambda n: re.match(r'^paytm_.*', n), topics_all)) 
    topics_paytm.extend(['trigger'])
    return topics_paytm


def start():
    consumer = generate_consumer()
    topics = get_relevant_topics()
    print('subscribing to ', topics)
    consumer.subscribe(topics=topics)

    while True:
        try:
            msg = consumer.poll()
            if not msg:
                continue
            elif msg.topic() == 'trigger':
                val = msg.value().decode()
                if val == 'check-new-topics':
                    new_relevant_topics = get_relevant_topics()
                    if len(new_relevant_topics) > len(topics):
                        print('resubscribing...')
                        consumer.subscribe(new_relevant_topics)
                elif val == 'list-topics':
                    print('topic list', topics)
                elif val == 'close':
                    print('shutting down gracefully...')
                    consumer.close()
                    return 'close'
                elif val == 'flush':
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
            return 'close'

if __name__ == '__main__':
    def main():
        res = start()
        if res == 'restart':
            print('restarting subscription...')
            main()
    main()