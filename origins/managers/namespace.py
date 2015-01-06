import logging
from origins.graph import neo4j, traverse, cypher


logger = logging.getLogger(__name__)


class NamespaceManager(object):
    "Manager for Occurrent-based models."

    match_statement = '''

        MATCH (n$model $predicate)
        RETURN n

    '''

    search_statement = '''

        MATCH (n$model)
        WHERE $predicate
        RETURN n

    '''

    get_statement = '''

        MATCH (n$model {`uri`: { uri }})
        RETURN n
        LIMIT 1

    '''

    exists_statement = '''

        MATCH (n$model {`uri`: { uri }})
        RETURN true
        LIMIT 1

    '''

    add_statement = '''

        CREATE (n$model { attrs })
        RETURN 1

    '''

    update_statement = '''

        MATCH (n$model {`uri`: { uri }})
        SET n = $attrs
        RETURN 1

    '''

    remove_statement = '''

        MATCH (n$model {`uri`: { uri }})
        DELETE n
        RETURN 1

    '''

    def __init__(self, model):
        self.model = model

    def serialize(self, instance):
        return instance.serialize()

    @property
    def label(self):
        "The primary label of the model."
        return self.model.__name__

    def match_query(self, predicate=None, limit=None, skip=None):
        return traverse.match(self.match_statement,
                              predicate=predicate,
                              limit=limit,
                              skip=skip,
                              model=self.label)

    def search_query(self, predicate, operator=None, limit=None, skip=None):
        return traverse.search(self.search_statement,
                               predicate=predicate,
                               operator=operator,
                               limit=limit,
                               skip=skip,
                               model=self.label)

    def get_query(self, attrs):
        statement = cypher.prepare(self.get_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uri': attrs['uri'],
            },
        }

    def exists_query(self, attrs):
        statement = cypher.prepare(self.exists_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uri': attrs['uri'],
            },
        }

    def add_query(self, instance):
        statement = cypher.prepare(self.add_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'attrs': self.serialize(instance),
            },
        }

    def update_query(self, instance, previous):
        attrs = self.serialize(instance)

        statement = cypher.prepare(self.update_statement,
                                   model=self.label,
                                   attrs=cypher.map(attrs.keys(), 'attrs'))

        return {
            'statement': statement,
            'parameters': {
                'attrs': attrs,
            }
        }

    def remove_query(self, instance):
        statement = cypher.prepare(self.remove_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uri': instance.uri
            },
        }

    def match(self, predicate=None, limit=None, skip=None, tx=neo4j.tx):
        query = self.match_query(predicate=predicate,
                                 limit=limit,
                                 skip=skip)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def search(self, predicate, operator=None, limit=None, skip=None,
               tx=neo4j.tx):

        query = self.search_query(predicate=predicate,
                                  operator=operator,
                                  limit=limit,
                                  skip=skip)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def get(self, attrs, tx=neo4j.tx):
        query = self.get_query(attrs)
        result = tx.send(query)

        if result:
            return self.model.parse(*result[0])

    def exists(self, attrs, tx=neo4j.tx):
        query = self.exists_query(attrs)
        result = tx.send(query)

        return True if result else False

    def add(self, instance, tx=neo4j.tx, defer=False):
        query = self.add_query(instance)

        if not query:
            logger.info('no add queries')
            return

        with tx as tx:
            result = tx.send(query, defer=defer)

            if not defer and not result:
                raise Exception('error adding {}'.format(instance))

    def update(self, instance, previous, tx=neo4j.tx, defer=False):
        query = self.update_query(instance, previous)

        if not query:
            logger.info('no update queries')
            return

        with tx as tx:
            result = tx.send(query, defer=defer)

            if not defer and not result:
                raise Exception('error updating {}'.format(instance))

    def remove(self, instance, tx=neo4j.tx, defer=False):
        query = self.remove_query(instance)

        if not query:
            logger.info('no remove queries')
            return

        with tx as tx:
            result = tx.send(query, defer=defer)

            if not defer and not result:
                raise Exception('error removing {}'.format(instance))
