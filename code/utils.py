from confluent_kafka.admin import NewTopic
from psycopg.sql import SQL, Identifier, Placeholder
from concurrent.futures import wait
from settings import admin
from producer import prod
from connections import conn_pg
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
        prod.produce(topic='trigger', value=msg, partition=i)
        prod.flush()


def create_schema_if_not_exists(schema_name='l1_data'):
    q = f"create schema if not exists {schema_name}"
    conn_pg.execute(q)


def create_table_if_not_exists(table_name: str):
    q = f"""
        CREATE TABLE IF NOT EXISTS l1_data.{table_name} (
                    json_data VARCHAR,
                    ingest_date DATE DEFAULT CURRENT_DATE,
                    ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
    """
    conn_pg.execute(q)


def generate_query(table_name: str, json_data: str ):
    query = SQL("insert into {} (json_data) values ({}) ") \
                .format(
                    Identifier('l1_data', table_name),
                    Placeholder('json_data')
                )
    return (query, {'json_data': json_data})