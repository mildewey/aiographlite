import aiosqlite as sql

import logging
logger = logging.getLogger(__name__)

def with_conn(fun):
    async def wrapped(dbfile, *args, **kwargs):
        async with sql.connect(dbfile) as db:
            result = await fun(db, *args, **kwargs)
            return result
    return wrapped

@with_conn
async def attributes(db, tables):
    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table';")
    dbtables = await cursor.fetchall()
    for table in dbtables:
        logger.info(table)
        if table in tables:
            cursor = await db.execute("PRAGMA table_info(%s)" % table)
            columns = await cursor.fetchall()
            logger.debug(columns)
