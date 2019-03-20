from aiohttp import web
from routes import setup_routes
import aiosqlite as sql

import schema
import db

import config
conf = config.init()

import logging
logger = logging.getLogger(__name__)

async def initialize_app():
    logger.info('Parsing Schema')
    schema.update_schema(schema.schema, conf.schema)

    await db.attributes(conf.database, schema.schema_to_tables(conf.schema))

    logger.info('Initializing app')
    app = web.Application()
    app['conf'] = conf

    logger.info('Setting up routes')
    setup_routes(app)

    return app

web.run_app(initialize_app())
