import logging
from origins.graph import neo4j, cypher
from .manager import Manager


logger = logging.getLogger(__name__)


class LinkManager(Manager):
    "Manager for topology-contained nodes."

    match_statement = '''

        MATCH (:Topology {`origins:uuid`: { topology }})-[:manages]->(n$model $predicate)
        WHERE NOT (n)<-[:revisionOf]-()
        WITH n
        MATCH (n)-[:`origins:start`]->(s)<-[:manages]-(sr),
              (n)-[:`origins:end`]->(e)<-[:manages]-(er)
        OPTIONAL MATCH (s)<-[:`prov:entity`]-(si:Invalidation)
        OPTIONAL MATCH (e)<-[:`prov:entity`]-(ei:Invalidation)
        RETURN n, s, sr, e, er, (si is not null or ei is not null)

    '''  # noqa

    search_statement = '''

        MATCH (:Topology {`origins:uuid`: { topology }})-[:manages]->(n$model)
        WHERE NOT (n)<-[:revisionOf]-() AND $predicate
        WITH n
        MATCH (n)-[:`origins:start`]->(s)<-[:manages]-(sr),
              (n)-[:`origins:end`]->(e)<-[:manages]-(er)
        OPTIONAL MATCH (s)<-[:`prov:entity`]-(si:Invalidation)
        OPTIONAL MATCH (e)<-[:`prov:entity`]-(ei:Invalidation)
        RETURN n, s, sr, e, er, (si is not null or ei is not null)

    '''

    get_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }}),
              (n)-[:`origins:start`]->(s)<-[:manages]-(sr),
              (n)-[:`origins:end`]->(e)<-[:manages]-(er)
        OPTIONAL MATCH (s)<-[:`prov:entity`]-(si:Invalidation)
        OPTIONAL MATCH (e)<-[:`prov:entity`]-(ei:Invalidation)
        RETURN n, s, sr, e, er, (si is not null or ei is not null)
        LIMIT 1

    '''  # noqa

    get_by_id_statement = '''

        MATCH (:Topology {`origins:uuid`: { topology }})-[:manages]->(n$model {`origins:id`: { id }}),
              (n)-[:`origins:start`]->(s)<-[:manages]-(sr),
              (n)-[:`origins:end`]->(e)<-[:manages]-(er)
        OPTIONAL MATCH (s)<-[:`prov:entity`]-(si:Invalidation)
        OPTIONAL MATCH (e)<-[:`prov:entity`]-(ei:Invalidation)
        RETURN n, s, sr, e, er, (si is not null or ei is not null)
        LIMIT 1

    '''  # noqa

    exists_by_id_statement = '''

        MATCH (:Topology {`origins:uuid`: { topology }})-[:manages]->(n$model {`origins:id`: { id }})
        RETURN true
        LIMIT 1

    '''  # noqa

    add_statement = '''

        MATCH (r:Topology {`origins:uuid`: { topology }})
        CREATE (n$model { attrs })
        CREATE (r)-[:manages]->(n)
        RETURN 1

    '''

    link_statement = '''

        MATCH (s$start_model {`origins:uuid`: { start }}),
              (e$end_model {`origins:uuid`: { end }})

        CREATE (s)-[:link]->(e)

        RETURN 1
    '''

    remove_statement = '''
        MATCH (n$model {`origins:uuid`: { uuid }}),
              (n)-[sr:`origins:start`]->(s),
              (n)-[er:`origins:end`]->(e),
              (s)-[lr:link]->(e),
              (n)-[r]-()

        DELETE r, sr, er, lr, n

        RETURN 1

    '''

    def match_query(self, topology, *args, **kwargs):
        query = super(LinkManager, self).match_query(*args, **kwargs)

        query['parameters']['topology'] = topology

        return query

    def search_query(self, topology, *args, **kwargs):
        query = super(LinkManager, self).search_query(*args, **kwargs)

        query['parameters']['topology'] = topology

        return query

    def get_by_id_query(self, topology, attrs):
        query = super(LinkManager, self).get_by_id_query(attrs)

        query['parameters']['topology'] = topology

        return query

    def exists_by_id_query(self, topology, attrs):
        query = super(LinkManager, self).exists_by_id_query(attrs)

        query['parameters']['topology'] = topology

        return query

    def add_query(self, instance):
        queries = super(LinkManager, self).add_query(instance)

        queries[0]['parameters']['topology'] = instance.topology.uuid

        # Optimization for revision-based derivations. An edge is formed
        # directly between the two entities
        start = instance.rels['origins:start']
        end = instance.rels['origins:end']

        statement = cypher.prepare(self.link_statement,
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

    def match(self, topology, predicate=None, limit=None, skip=None,
              tx=neo4j.tx):
        query = self.match_query(topology,
                                 predicate=predicate,
                                 limit=limit,
                                 skip=skip)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def search(self, topology, predicate, operator=None, limit=None, skip=None,
               tx=neo4j.tx):

        query = self.search_query(topology,
                                  predicate=predicate,
                                  operator=operator,
                                  limit=limit,
                                  skip=skip)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def get(self, topology, attrs, tx=neo4j.tx):
        if 'origins:uuid' in attrs:
            query = self.get_query(attrs)
        elif 'origins:id' in attrs:
            query = self.get_by_id_query(topology, attrs)
        else:
            raise KeyError('uuid or id required')

        result = tx.send(query)

        if result:
            return self.model.parse(*result[0])

    def exists(self, topology, attrs, tx=neo4j.tx):
        if 'origins:uuid' in attrs:
            query = self.exists_query(attrs)
        elif 'origins:id' in attrs:
            query = self.exists_by_id_query(topology, attrs)
        else:
            raise KeyError('uuid or id required')

        result = tx.send(query)

        return True if result else False
