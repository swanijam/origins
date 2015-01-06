import logging
from origins.exceptions import QueryError
from origins.graph import neo4j, traverse, cypher


logger = logging.getLogger(__name__)


class Manager(object):
    "Base manager."

    match_statement = '''

        MATCH (n$model $predicate)
        RETURN n

    '''

    match_by_relation_statement = '''

        MATCH (n$model)
        WITH n

    '''

    search_statement = '''

        MATCH (n$model)
        WHERE $predicate
        RETURN n

    '''

    get_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }})
        RETURN n
        LIMIT 1

    '''

    get_by_id_statement = '''

        MATCH (n$model {`origins:id`: { id }})
        RETURN n
        LIMIT 1

    '''

    exists_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }})
        RETURN true
        LIMIT 1

    '''

    exists_by_id_statement = '''

        MATCH (n$model {`origins:id`: { id }})
        RETURN true
        LIMIT 1

    '''

    add_statement = '''

        CREATE (n$model { attrs })
        RETURN 1

    '''

    update_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }})
        SET n = $attrs
        RETURN 1

    '''

    remove_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }})
        DELETE n
        RETURN 1

    '''

    relation_edge_statement = '''

        MATCH (s$start_model {`origins:uuid`: { start }}),
              (e$end_model {`origins:uuid`: { end }})

        CREATE (s)-[$type]->(e)

        RETURN 1
    '''

    def __init__(self, model):
        self.model = model

    def serialize(self, instance):
        "Serializes and prepares an instance for query execution."
        data = instance.serialize()

        # Remove relations and None values
        for k in tuple(data):
            if k in instance.relations or data[k] is None:
                data.pop(k)
            elif isinstance(data[k], dict):
                raise TypeError('{!r} is not supported'.format(data[k]))

        return data

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

    def match_by_relation_query(self, predicate):
        matches = []
        mapping = {}
        parameters = {}

        for i, attr in enumerate(predicate):
            if attr not in self.model.relations:
                raise QueryError('{} not a {} relation'
                                 .format(attr, self.model.name))

            model, required = self.model.relations[attr]

            rv = 'r{}'.format(i)
            mv = 'm{}'.format(i)
            iv = 'i{}'.format(i)

            matches.append('(n)-[${}]->(${} {{`origins:uuid`: {{ {} }} }})'
                           .format(rv, mv, iv))

            mapping[rv] = cypher.labels(attr)
            mapping[mv] = cypher.labels(model)
            parameters[iv] = predicate[attr]

        if not mapping:
            raise QueryError('at least one relation must be provided')

        relation_statement = '{} MATCH {} RETURN n' \
                             .format(self.match_by_relation_statement,
                                     ', '.join(matches))

        statement = cypher.prepare(relation_statement,
                                   model=self.label,
                                   **mapping)

        return {
            'statement': statement,
            'parameters': parameters,
        }

    def get_query(self, uuid):
        statement = cypher.prepare(self.get_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            },
        }

    def get_by_id_query(self, id):
        statement = cypher.prepare(self.get_by_id_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'id': id,
            },
        }

    def exists_query(self, uuid):
        statement = cypher.prepare(self.exists_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            },
        }

    def exists_by_id_query(self, id):
        statement = cypher.prepare(self.exists_by_id_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'id': id,
            },
        }

    def add_query(self, instance):
        statement = cypher.prepare(self.add_statement,
                                   model=self.label)

        queries = [{
            'statement': statement,
            'parameters': {
                'attrs': self.serialize(instance),
            },
            'expected': 1,
        }]

        start_model_name = instance.__class__.__name__

        # Add relation-based edges and remove them from the attrs parameter
        for attr, (model, required) in instance.relations.items():
            if not instance.attrs.get(attr):
                continue

            end = instance.rels[attr]

            queries.append({
                'statement': cypher.prepare(self.relation_edge_statement,
                                            type=cypher.labels(attr),
                                            start_model=start_model_name,
                                            end_model=end.__class__.__name__),
                'parameters': {
                    'start': instance.uuid,
                    'end': end.uuid,
                },
                'expected': 1,
            })

        return queries

    def update_query(self, instance):
        attrs = self.serialize(instance)

        statement = cypher.prepare(self.update_statement,
                                   model=self.label,
                                   attrs=cypher.map(attrs.keys(), 'attrs'))

        return {
            'statement': statement,
            'parameters': {
                'attrs': attrs,
                'uuid': instance.uuid,
            },
            'expected': 1,
        }

    def remove_query(self, instance):
        statement = cypher.prepare(self.remove_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': instance.uuid
            },
            'expected': 1,
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

    def match_by_relation(self, attrs, tx=neo4j.tx):
        query = self.match_by_relation_query(attrs)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def get(self, uuid, tx=neo4j.tx):
        query = self.get_query(uuid)

        result = tx.send(query)

        if result:
            return self.model.parse(*result[0])

    def get_by_id(self, id, tx=neo4j.tx):
        query = self.get_by_id_query(id)

        result = tx.send(query)

        if result:
            return self.model.parse(*result[0])

    def exists(self, uuid, tx=neo4j.tx):
        query = self.exists_query(uuid)

        result = tx.send(query)

        return True if result else False

    def exists_by_id(self, id, tx=neo4j.tx):
        query = self.exists_by_id_query(id)

        result = tx.send(query)

        return True if result else False

    def add(self, instance, tx=neo4j.tx, defer=False):
        query = self.add_query(instance)

        tx.send(query, defer=defer)

    def update(self, instance, tx=neo4j.tx, defer=False):
        query = self.update_query(instance)

        tx.send(query, defer=defer)

    def remove(self, instance, tx=neo4j.tx, defer=False):
        query = self.remove_query(instance)

        tx.send(query, defer=defer)
