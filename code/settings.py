from confluent_kafka.admin import AdminClient
from dotenv import dotenv_values
from pathlib import Path
import os

BASE_PATH = Path(__file__).parent.parent
IS_DOCKER = os.getenv('IS_DOCKER', '0') == '1'

LIMIT_LOG_WRITES_PER_HOUR = 60
LOG_FILE_PATH = BASE_PATH / 'app.log'

if IS_DOCKER:
    env_getter = os.getenv
else:
    ENV = dotenv_values(BASE_PATH / '.env')
    env_getter = ENV.get

BOOTSTRAP_SERVERS = env_getter('BOOTSTRAP_SERVERS')    # must be comma-separated
GROUP_ID = env_getter('GROUP_ID')
GROUP_INSTANCE_ID = env_getter('GROUP_INSTANCE_ID')

TRIGGER_CODE_CHECK_NEW_TOPICS = env_getter('TRIGGER_CODE_CHECK_NEW_TOPICS')
TRIGGER_CODE_LIST_TOPICS = env_getter('TRIGGER_CODE_LIST_TOPICS')
TRIGGER_CODE_CLOSE = env_getter('TRIGGER_CODE_CLOSE')
TRIGGER_CODE_FLUSH = env_getter('TRIGGER_CODE_FLUSH')

DB_HOST = env_getter('DB_HOST')
DB_PORT = int(env_getter('DB_PORT'))
DB_NAME = env_getter('DB_NAME')
DB_USER = env_getter('DB_USER')
DB_PASSWORD = env_getter('DB_PASSWORD')

admin_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS
}
admin = AdminClient(conf=admin_conf)