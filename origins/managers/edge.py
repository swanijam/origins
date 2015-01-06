from origins.graph import cypher
from .resource import ResourceManagedManager


class EdgeManager(ResourceManagedManager):
    edge_statement = '''

        MATCH (s$start_model {`origins:uuid`: { start }}),
              (e$end_model {`origins:uuid`: { end }})

        CREATE (s)-[:edge]->(e)

        RETURN 1
    '''

    def add_query(self, instance):
        queries = super(EdgeManager, self).add_query(instance)

        start = instance.rels['prov:influencer']
        end = instance.rels['prov:influencee']

        statement = cypher.prepare(self.edge_statement,
                                   start_model=start.__class__.__name__,
                                   end_model=end.__class__.__name__)

        queries.append({
            'statement': statement,
            'parameters': {
                'start': start.uuid,
                'end': end.uuid,
            },
            'excepted': 1,
        })

        return queries
