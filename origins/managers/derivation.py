from origins.graph import cypher
from .resource import ResourceManagedManager


class DerivationManager(ResourceManagedManager):
    revision_edge_statement = '''

        MATCH (s$start_model {`origins:uuid`: { start }}),
              (e$end_model {`origins:uuid`: { end }})

        CREATE (s)<-[:revisionOf]-(e)

        RETURN 1
    '''

    def add_query(self, instance):
        queries = super(DerivationManager, self).add_query(instance)

        # Optimization for revision-based derivations. An edge is formed
        # directly between the two entities
        if instance.attrs.get('prov:type') == 'prov:Revision':
            start = instance.rels['prov:usedEntity']
            end = instance.rels['prov:generatedEntity']

            statement = cypher.prepare(self.revision_edge_statement,
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
