from aiohttp import web
import logging
logger = logging.getLogger(__name__)

import schema

async def handle_graphql(request):
    query = await request.text()
    
    return web.Response(text="Let's roll!")
