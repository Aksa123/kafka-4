from dotenv import dotenv_values
from pathlib import Path


BASE_PATH = Path(__file__).parent

ENV = dotenv_values(BASE_PATH / '.env')
BOOTSTRAP_SERVERS = ENV['BOOTSTRAP_SERVERS']    # must be comma-separated