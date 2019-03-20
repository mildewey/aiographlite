import aiosqlite as sql
import uuid

import logging
logger = logging.getLogger(__name__)

def with_conn(fun):
    async def wrapped(dbfile, *args, **kwargs):
        if not isinstance(dbfile, str):
            result = await fun(dbfile, *args, **kwargs)
            return result

        async with sql.connect(dbfile) as db:
            result = await fun(db, *args, **kwargs)
            return result

    return wrapped

async def get_tables(db):
    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table';")
    dbtables = await cursor.fetchall()
    return [val[0] for val in dbtables]

@with_conn
async def make_graph(db):
    await db.execute("CREATE TABLE IF NOT EXISTS aiographlite_spt (source TEXT, predicate TEXT, target TEXT, PRIMARY KEY (source, predicate))")
    await db.execute("CREATE TABLE IF NOT EXISTS aiographlite_stp (source TEXT, target TEXT, predicate TEXT, PRIMARY KEY (source, target))")
    await db.execute("CREATE TABLE IF NOT EXISTS aiographlite_pst (predicate TEXT, source TEXT, target TEXT, PRIMARY KEY (predicate, source))")
    await db.execute("CREATE TABLE IF NOT EXISTS aiographlite_pts (predicate TEXT, target TEXT, source TEXT, PRIMARY KEY (predicate, target))")
    await db.execute("CREATE TABLE IF NOT EXISTS aiographlite_tsp (target TEXT, source TEXT, predicate TEXT, PRIMARY KEY (target, source))")
    await db.execute("CREATE TABLE IF NOT EXISTS aiographlite_tps (target TEXT, predicate TEXT, source TEXT, PRIMARY KEY (target, predicate))")
    await db.commit()

@with_conn
async def insert_link(db, source=None, predicate=None, target=None):
    source = uuid.UUID(source) if source else uuid.uuid4()
    predicate = uuid.UUID(predicate) if predicate else uuid.uuid4()
    target = uuid.UUID(target) if target else uuid.uuid4()

    source = source.hex
    predicate = predicate.hex
    target = target.hex

    await db.execute("INSERT INTO aiographlite_spt (source, predicate, target) VALUES ('%s', '%s', '%s')" % (source, predicate, target))
    await db.execute("INSERT INTO aiographlite_stp (source, predicate, target) VALUES ('%s', '%s', '%s')" % (source, predicate, target))
    await db.execute("INSERT INTO aiographlite_pst (source, predicate, target) VALUES ('%s', '%s', '%s')" % (source, predicate, target))
    await db.execute("INSERT INTO aiographlite_pts (source, predicate, target) VALUES ('%s', '%s', '%s')" % (source, predicate, target))
    await db.execute("INSERT INTO aiographlite_tsp (source, predicate, target) VALUES ('%s', '%s', '%s')" % (source, predicate, target))
    await db.execute("INSERT INTO aiographlite_tps (source, predicate, target) VALUES ('%s', '%s', '%s')" % (source, predicate, target))
    await db.commit()

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
    dbtables = await get_tables(db)
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
            await db.execute("CREATE TABLE %s(%s, PRIMARY KEY(id));" % (
                table,
                ', '.join(['%s %s' %(cname, ctype) for (cname, ctype) in gql_to_sql(columns)])))
    await db.commit()
    return

@with_conn
async def insert_attr(db, table, **value_map):
    schema = await get_schema(db, table)
    id = uuid.uuid4().hex
    value_map['id'] = id
    columns = ', '.join([col for (col, _) in schema])
    values = ', '.join([value_map[col] if type != 'TEXT' else "'%s'" % value_map[col] for (col, type) in schema])

    await db.execute("INSERT INTO %s (%s) VALUES (%s)" % (table, columns, values))
    await db.commit()
    return id

@with_conn
async def get_attr(db, table, id):
    schema = await get_schema(db, table)
    async with db.execute("SELECT * FROM %s WHERE id='%s'" % (table, id)) as cursor:
        attr = await cursor.fetchone()
        return {col: val for ((col, _), val) in zip(schema, attr)} if attr else attr

@with_conn
async def types(db, types, id):
    discovered = []
    for type in types:
        attr = await get_attr(db, type, id)
        if attr:
            discovered.append(type)
    return discovered

@with_conn
async def outgoing(db, id):
    async with db.execute("SELECT predicate FROM aiographlite_spt WHERE source='%s'" % id) as cursor:
        results = await cursor.fetchall()
        return [result[0] for result in results] if results else []

@with_conn
async def incoming(db, id):
    async with db.execute("SELECT predicate FROM aiographlite_tps WHERE target='%s'" % id) as cursor:
        results = await cursor.fetchall()
        return [result[0] for result in results] if results else []

@with_conn
async def source(db, id):
    async with db.execute("SELECT source FROM aiographlite_pst WHERE predicate='%s'" % id) as cursor:
        results = await cursor.fetchone()
        return results[0] if results else None

@with_conn
async def target(db, id):
    async with db.execute("SELECT target FROM aiographlite_pts WHERE predicate='%s'" % id) as cursor:
        results = await cursor.fetchone()
        return results[0] if results else None
