import aiosqlite as sql

import logging
logger = logging.getLogger(__name__)

def with_conn(fun):
    async def wrapped(dbfile, *args, **kwargs):
        async with sql.connect(dbfile) as db:
            result = await fun(db, *args, **kwargs)
            return result
    return wrapped

def schema_agrees(gql, sql):
    return gql_to_sql(gql) == sql

def gql_to_sql(gql):
    conv = dict(
        String='TEXT',
        Int='INTEGER',
        Float='REAL',
        Boolean='INTEGER'
    )
    return [(name, conv[type]) for (name, type) in gql]

async def get_schema(db, table):
    schema = []
    async with db.execute("PRAGMA table_info(%s)" % table) as cursor:
        async for row in cursor:
            schema.append(row[1:3])
    return schema

@with_conn
async def attributes(db, tables):
    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table';")
    dbtables = await cursor.fetchall()
    dbtables = [val[0] for val in dbtables]
    currentTables = {}
    for table in dbtables:
        if table in tables:
            currentTables[table] = await get_schema(db, table)
    logger.info("Current Schema: %s" % currentTables)
    for (table, columns) in tables.items():
        if table in dbtables:
            if schema_agrees(columns, currentTables[table]):
                continue
            else:
                raise Exception('Schema disagreement')
        else:
            await db.execute("CREATE TABLE %s(%s, PRIMARY KEY(%s));" % (
                table,
                ', '.join(['%s %s' %(cname, ctype) for (cname, ctype) in gql_to_sql(columns)]),
                columns[0][0]))
    await db.commit()
    return
