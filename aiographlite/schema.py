import graphql

import logging
logger = logging.getLogger(__name__)

import resolvers

def create_schema(extension):
    schema = graphql.build_schema("""
        type Query {
            node (id: String!): Node
            edge (id: String!): Edge
            attr (id: String!, type: String!): Attributes
            types (id: String): [String!]!
        }

        type Mutation {
            addLink (source: String!, link: String!, target: String!): String
        }

        interface Attributes {
            id: String!
        }

        type Node {
            id: String!
            outgoing: [Edge!]
            incoming: [Edge!]
            attr (type: String!): Attributes!
        }

        type Edge {
            id: String!
            source: Node!
            target: Node!
            attr (type: String!): Attributes!
        }
    """)

    schema.get_type('Attributes').resolve_type = lambda attr, *_: attr['type']

    fields = schema.query_type.fields
    fields['node'].resolve = resolvers.query_node
    fields['edge'].resolve = resolvers.query_edge
    fields['attr'].resolve = resolvers.query_attr
    fields['types'].resolve = resolvers.query_types

    fields = schema.mutation_type.fields
    fields['addLink'].resolve = resolvers.create_link

    fields = schema.get_type('Node').fields
    fields['id'].resolve = resolvers.field_id
    fields['outgoing'].resolve = resolvers.field_outgoing
    fields['incoming'].resolve = resolvers.field_incoming
    fields['attr'].resolve = resolvers.field_attr

    fields = schema.get_type('Edge').fields
    fields['id'].resolve = resolvers.field_id
    fields['source'].resolve = resolvers.field_source
    fields['target'].resolve = resolvers.field_target
    fields['attr'].resolve = resolvers.field_attr

    return update_schema(schema, extension)

def add_type_mutators(schema, table, columns):
    column_str =", ".join("%s: %s" % column for column in columns[1:]) # omits the id field that's always first
    addAttr = "add{table} ({columns}): String".format(table=table, columns=column_str) # These always return the ID created or null for errors

    mutations = "extend type Mutation { %s }" % addAttr
    schema = graphql.extend_schema(schema, graphql.parse(mutations))
    schema.mutation_type.fields["add%s" % table].resolve = resolvers.meta_create_attribute(table)

    return schema

def add_type_fields(schema, table, columns):
    fields = schema.get_type(table).fields
    for col in columns:
        fields[col[0]].resolve = resolvers.meta_field(col[0])

    return schema

def update_schema(schema, extension):
    logger.debug("Extending base schema with:\n%s" % extension)
    schema = graphql.extend_schema(schema, graphql.parse(extension))
    tables = schema_to_tables(extension)

    for (table, columns) in tables.items():
        schema = add_type_fields(schema, table, columns)
        schema = add_type_mutators(schema, table, columns)

    return schema

def schema_to_tables(sch):
    tree = graphql.parse(sch)
    tables = {}
    def extract(loc):
        return sch[loc.start:loc.end]

    for defs in tree.definitions:
        table_name = extract(defs.name.loc)
        columns = []
        for field in defs.fields:
            column_name = extract(field.name.loc)
            if isinstance(field.type, graphql.NonNullTypeNode):
                column_type = extract(field.type.type.loc)
            else:
                column_type = extract(field.type.loc)
            columns.append((column_name, column_type))
        tables[table_name] = columns

    return tables
