import logging
from collections import defaultdict
from origins.graph import neo4j, cypher
from .. import models
from ..models import Entity
from .manager import Manager


logger = logging.getLogger(__name__)


class ResourceManager(Manager):

    remove_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }})
        OPTIONAL MATCH (n)-[r:manages]->(d)
        OPTIONAL MATCH (d)-[r2]-()
        DELETE r2, r, d, n
        RETURN 1

    '''

    entities_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        RETURN e

    '''

    entity_roots_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-() AND NOT (e)<-[:edge]-()
        RETURN e

    '''

    entity_types_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        RETURN DISTINCT e.`prov:type`, COUNT(e)

    '''

    entity_count_statement = '''

        MATCH (r$model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        RETURN count(e)

    '''

    links_statement = '''
        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e
        MATCH (e)<-[a:`origins:start`|`origins:end`]-(l:Link)
        WITH l
        MATCH (l)-[:`origins:start`]->(s)<-[:manages]-(sr),
              (l)-[:`origins:end`]->(e)<-[:manages]-(er)
        RETURN l, s, sr, e, er
    '''

    link_count_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e
        MATCH (e)<-[a:`origins:start`|`origins:end`]-(l:Link)
        RETURN count(DISTINCT l)

    '''

    broken_link_count_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e
        MATCH (e)<-[a:`origins:start`|`origins:end`]-(l:Link)
        WITH DISTINCT l
        MATCH (l)-[:`origins:start`]->(s),
              (l)-[:`origins:end`]->(e)
        WHERE (s)<-[:`prov:entity`]-(:Invalidation)
            OR (e)<-[:`prov:entity`]-(:Invalidation)
        RETURN count(DISTINCT l)

    '''

    broken_links_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e
        MATCH (e)<-[a:`origins:start`|`origins:end`]-(l:Link)
        WITH DISTINCT l
        MATCH (l)-[:`origins:start`]->(s),
              (l)-[:`origins:end`]->(e)
        WHERE (s)<-[:`prov:entity`]-(:Invalidation)
            OR (e)<-[:`prov:entity`]-(:Invalidation)
        RETURN DISTINCT l

    '''

    # Generation events. The derivation information is included.
    generation_events_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)
        WITH e
        MATCH (e)<-[:`prov:entity`]-(g:Generation)
        OPTIONAL MATCH (g)<-[:`prov:generation`]-(d:Derivation)
        OPTIONAL MATCH (d)-[:`prov:usedEntity`]->(e2:Entity)
        OPTIONAL MATCH (d)-[:`prov:usage`]->(u:Usage)
        OPTIONAL MATCH (g)-[:`prov:activity`]->(a:Activity)
        OPTIONAL MATCH (a)<-[:`prov:activity`]-(as:Association)
        OPTIONAL MATCH (as)-[:`prov:agent`]->(ag:Agent)
        RETURN DISTINCT e, g, d, e2, u, a, as, ag
        ORDER BY g.`prov:time`, g.`origins:time` ASC

    '''  # noqa

    # Invalidation not caused by a revision.
    invalidation_events_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Entity)<-[:`prov:entity`]-(i:Invalidation)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e, i
        OPTIONAL MATCH (i)-[:`prov:activity`]->(a:Activity)
        OPTIONAL MATCH (a)<-[:`prov:activity`]-(as:Association)
        OPTIONAL MATCH (as)-[:`prov:agent`]->(ag:Agent)
        RETURN DISTINCT e, i, a, as, ag
        ORDER BY i.`prov:time`, i.`origins:time` ASC

    '''  # noqa

    subscribers_statement = '''
        MATCH (:Resource {`origins:uuid`: { uuid }})<-[:subscribedTo]-(u:User)
        RETURN u
    '''

    def entity_types_query(self, uuid):
        statement = cypher.prepare(self.entity_types_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid
            }
        }

    def entities_query(self, uuid):
        statement = cypher.prepare(self.entities_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def entity_roots_query(self, uuid):
        statement = cypher.prepare(self.entity_roots_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def entity_count_query(self, uuid):
        statement = cypher.prepare(self.entity_count_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def links_query(self, uuid):
        statement = cypher.prepare(self.links_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def link_count_query(self, uuid):
        statement = cypher.prepare(self.link_count_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def broken_link_count_query(self, uuid):
        statement = cypher.prepare(self.broken_link_count_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def generation_events_query(self, uuid):
        statement = cypher.prepare(self.generation_events_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def invalidation_events_query(self, uuid):
        statement = cypher.prepare(self.invalidation_events_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def subscribers_query(self, uuid):
        statement = cypher.prepare(self.subscribers_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def generation_events(self, uuid, tx=neo4j.tx):
        query = self.generation_events_query(uuid)

        results = tx.send(query)

        events = []
        event = None

        for result in results:
            e, g, d, e2, u, a, _as, ag = result

            # There could be multiple associations to this event,
            # so this block only adds a new event if it has changed.
            if not event or g['origins:uuid'] != event['generation'].uuid:
                event = {
                    'type': 'generation',
                    'entity': models.parse(e),
                    'generation': models.parse(g),
                    'derivation': models.parse(d),
                    'used_entity': models.parse(e2),
                    'usage': models.parse(u),
                    'activity': models.parse(a),
                    'associations': [],
                }

                # Add uuid and time directly onto the event
                event['uuid'] = event['generation'].uuid
                event['time'] = event['generation'].time

                events.append(event)

            if _as:
                event['associations'].append({
                    'association': models.parse(_as),
                    'agent': models.parse(ag),
                })

        return events

    def invalidation_events(self, uuid, tx=neo4j.tx):
        query = self.invalidation_events_query(uuid)

        results = tx.send(query)

        events = []
        event = None

        for result in results:
            e, i, a, _as, ag = result

            # There could be multiple associations to this event,
            # so this block only adds a new event if it has changed.
            if not event or i['origins:uuid'] != event['invalidation'].uuid:
                event = {
                    'type': 'invalidation',
                    'entity': models.parse(e),
                    'invalidation': models.parse(i),
                    'activity': models.parse(a),
                    'associations': [],
                }

                # Add uuid and time directly onto the event
                event['uuid'] = event['invalidation'].uuid
                event['time'] = event['invalidation'].time

                events.append(event)

            if _as:
                event['associations'].append({
                    'association': models.parse(_as),
                    'agent': models.parse(ag),
                })

        return events

    def entities(self, uuid, tx=neo4j.tx):
        query = self.entities_query(uuid)

        result = tx.send(query)

        return [Entity.parse(r[0]) for r in result]

    def entity_roots(self, uuid, tx=neo4j.tx):
        query = self.entity_roots_query(uuid)

        result = tx.send(query)

        return [Entity.parse(r[0]) for r in result]

    def entity_types(self, uuid, tx=neo4j.tx):
        query = self.entity_types_query(uuid)

        result = tx.send(query)

        counts = defaultdict(int)

        # Some entities have multiple types. This is not easy to do in the
        # query itself, so we handle it here
        for type, count in result:
            if isinstance(type, list):
                for t in type:
                    counts[t] += count
            else:
                counts[type] += count

        return [{
            'type': k,
            'count': v,
        } for k, v in counts.items()]

    def entity_count(self, uuid, tx=neo4j.tx):
        query = self.entity_count_query(uuid)

        result = tx.send(query)

        return result[0][0]

    def links(self, uuid, tx=neo4j.tx):
        query = self.links_query(uuid)

        links = []

        for l, s, sr, e, er in tx.send(query):
            link = models.parse(l)
            start = models.parse(s, resource=models.parse(sr))
            end = models.parse(e, resource=models.parse(er))

            links.append((link, start, end))

        return links

    def link_count(self, uuid, tx=neo4j.tx):
        query = self.link_count_query(uuid)

        result = tx.send(query)

        return result[0][0]

    def broken_link_count(self, uuid, tx=neo4j.tx):
        query = self.broken_link_count_query(uuid)

        result = tx.send(query)

        return result[0][0]

    def feed(self, uuid, tx=neo4j.tx):
        generation_events = self.generation_events(uuid, tx=tx)
        invalidation_events = self.invalidation_events(uuid, tx=tx)

        feed = []

        i = g = None

        # Sort events by time
        while True:
            if not g and generation_events:
                g = generation_events.pop()

            if not i and invalidation_events:
                i = invalidation_events.pop()

            if not g and not i:
                break

            if not g:
                feed.append(i)
                i = None
            elif not i:
                feed.append(g)
                g = None
            elif i['time'] > g['time']:
                feed.append(i)
                i = None
            else:
                feed.append(g)
                g = None

        return feed

    def subscribers(self, uuid, tx=neo4j.tx):
        query = self.subscribers_query(uuid)

        result = tx.send(query)

        return [r[0] for r in result]


class ResourceManagedManager(Manager):
    "Manager for resource-contained nodes."

    match_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model $predicate)
        RETURN n

    '''  # noqa

    search_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model)
        WHERE $predicate
        RETURN n

    '''

    match_by_relation_statement = '''
        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model)
        WITH n
    '''

    get_by_id_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model {`origins:id`: { id }})
        RETURN n
        LIMIT 1

    '''  # noqa

    exists_by_id_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model {`origins:id`: { id }})
        RETURN true
        LIMIT 1

    '''  # noqa

    add_statement = '''

        MATCH (r:Resource {`origins:uuid`: { resource }})
        CREATE (n$model { attrs })
        CREATE (r)-[:manages]->(n)
        RETURN 1

    '''

    resource_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})<-[:manages]-(r:Resource)
        RETURN r

    '''

    def match_query(self, resource, *args, **kwargs):
        query = super(ResourceManagedManager, self)\
            .match_query(*args, **kwargs)

        query['parameters']['resource'] = resource

        return query

    def search_query(self, resource, *args, **kwargs):
        query = super(ResourceManagedManager, self)\
            .search_query(*args, **kwargs)

        query['parameters']['resource'] = resource

        return query

    def match_by_relation_query(self, resource, *args, **kwargs):
        query = super(ResourceManagedManager, self)\
            .match_by_relation_query(*args, **kwargs)

        query['parameters']['resource'] = resource

        return query

    def get_by_id_query(self, resource, id):
        query = super(ResourceManagedManager, self).get_by_id_query(id)

        query['parameters']['resource'] = resource

        return query

    def exists_by_id_query(self, resource, id):
        query = super(ResourceManagedManager, self).exists_by_id_query(id)

        query['parameters']['resource'] = resource

        return query

    def add_query(self, instance):
        queries = super(ResourceManagedManager, self).add_query(instance)

        queries[0]['parameters']['resource'] = instance.resource.uuid

        return queries

    def resource_query(self, uuid):
        statement = cypher.prepare(self.resource_statement,
                                   model=self.label)
        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def match(self, resource, predicate=None, limit=None, skip=None,
              tx=neo4j.tx):
        query = self.match_query(resource,
                                 predicate=predicate,
                                 limit=limit,
                                 skip=skip)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def search(self, resource, predicate, operator=None, limit=None, skip=None,
               tx=neo4j.tx):

        query = self.search_query(resource,
                                  predicate=predicate,
                                  operator=operator,
                                  limit=limit,
                                  skip=skip)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def match_by_relation(self, resource, attrs, tx=neo4j.tx):
        query = self.match_by_relation_query(resource, attrs)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def get_by_id(self, resource, id, tx=neo4j.tx):
        query = self.get_by_id_query(resource, id)

        result = tx.send(query)

        if result:
            return self.model.parse(*result[0])

    def exists_by_id(self, resource, id, tx=neo4j.tx):
        query = self.exists_by_id_query(resource, id)

        result = tx.send(query)

        return True if result else False

    def resource(self, uuid, tx=neo4j.tx):
        query = self.resource_query(uuid)

        result = tx.send(query)

        return models.parse(result[0][0])
