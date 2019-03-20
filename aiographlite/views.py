from graphql import graphql
from aiohttp import web
import logging
logger = logging.getLogger(__name__)

import schema

async def handle_graphql(request):
    query = await request.text()
    result = await graphql(request.app['schema'], query, context_value=request.app['conf'])

    errors= [error.message for error in result.errors] if result.errors else []
    for error in errors:
        logger.warning(error)

    return web.json_response(dict(data=result.data, errors=errors))
