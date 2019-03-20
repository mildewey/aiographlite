from aiohttp import web
from routes import setup_routes
import aiosqlite as sql

import logging
logger = logging.getLogger(__name__)

import schema
import db
import config

logger.info('Configuring Schema')
conf = config.init()
sch = schema.create_schema(conf.schema)

async def initialize_app():
    logger.info('Initializing Graph')
    await db.make_graph(conf.database)

    logger.info('Initializing Attributes')
    tables = schema.schema_to_tables(conf.schema)
    await db.attributes(conf.database, tables)

    logger.info('Initializing Webapp')
    app = web.Application()
    app['conf'] = {
        'database': conf.database,
        'types': tables.keys()
    }
    app['schema'] = sch
    setup_routes(app)

    return app

web.run_app(initialize_app())
