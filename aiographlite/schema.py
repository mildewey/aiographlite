import graphql

import logging
logger = logging.getLogger(__name__)

schema = graphql.build_schema("""
    type Query {
        node (id: String!): Node!
        edge (id: String!): Edge!
        attr (id: String!): Attributes!
    }

    type Mutation {
        addNode (id: String): Node
        addEdge (id: String, target: String!, source: String!): Edge
    }

    interface Attributes {
        id: String!
    }

    type Node {
        id: String!
        outgoing: [Edge!]
        incoming: [Edge!]
        attr: Attributes!
    }

    type Edge {
        id: String!
        source: Node!
        target: Node!
        attr: Attributes!
    }
""")

def update_schema(schema, extension):
    logger.debug("Extending base schema with:\n%s" % extension)
    schema = graphql.extend_schema(schema, graphql.parse(extension))

def schema_to_tables(sch):
    tree = graphql.parse(sch)
    tables = {}
    def extract(loc):
        return sch[loc.start:loc.end]

    for defs in tree.definitions:
        table_name = extract(defs.name.loc)
        columns = {}
        for field in defs.fields:
            column_name = extract(field.name.loc)
            if isinstance(field.type, graphql.NonNullTypeNode):
                column_type = extract(field.type.type.loc)
            else:
                column_type = extract(field.type.loc)
            columns[column_name] = column_type
        tables[table_name] = columns

    return tables
