import db

import logging
logger = logging.getLogger(__name__)

def query_node(root, info, id):
    return id

def query_edge(root, info, id):
    return id

async def query_attr(root, info, id, type):
    attr = await db.get_attr(info.context['database'], type, id)
    if attr:
        attr['type'] = type
    return attr

async def query_types(root, info, id=None):
    types = await db.types(info.context['database'], info.context['types'], id) if id else info.context['types']
    return types

def type_Node(id, info):
    return id

def type_Edge(id, info):
    return id

def field_id(id, info):
    return id

def field_attr(id, info, type):
    return (id, type)

async def field_outgoing(id, info):
    outgoing = await db.outgoing(info.context['database'], id)
    return outgoing

async def field_incoming(id, info):
    incoming = await db.incoming(info.context['database'], id)
    return incoming

async def field_source(id, info):
    source = await db.source(info.context['database'], id)
    return source

async def field_target(id, info):
    target = await db.target(info.context['database'], id)
    return target

async def create_link(root, info, source, link, target):
    link = await db.insert_link(info.context['database'], source, link, target)
    return link

def meta_field(column):
    return lambda attr, *_: attr[column]

def meta_create_attribute(attr_type):
    async def creator(root, info, **kwargs):
        id = await db.insert_attr(info.context['database'], attr_type, **kwargs)
        return id
    return creator
