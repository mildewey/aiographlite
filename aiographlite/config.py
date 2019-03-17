import os
import sys
import logging
import yaml
from collections import namedtuple

Configuration = namedtuple('Configuration', ['database', 'schema'])

def init():
    database = os.environ.get('DB_FILE', 'default.db')

    schema_file = os.environ.get('SCHEMA_FILE')
    if schema_file:
        with open(schema_file, "r") as f:
            schema = f.read()
    else:
        schema = ""

    log_level = logging.getLevelName(os.environ.get('LOG_LEVEL', 'INFO'))
    log_file = os.environ.get('LOG_FILE')
    if log_file:
        logging.basicConfig(filename=log_file, level=log_level)
    else:
        logging.basicConfig(stream=sys.stdout, level=log_level)

    return Configuration(database, schema)
