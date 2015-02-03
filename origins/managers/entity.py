from origins.graph import neo4j, cypher
from origins import models
from origins.graph import traverse
from .resource import ResourceManagedManager


class EntityManager(ResourceManagedManager):

    match_all_statement = '''

        MATCH (n$model $predicate)
        WHERE NOT (n)<-[:revisionOf]-()
        RETURN n

    '''

    # Match against the latest revision of each entity
    match_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model $predicate)
        WHERE NOT (n)<-[:revisionOf]-()
        RETURN n

    '''  # noqa

    search_all_statement = '''

        MATCH (n$model)
        WHERE NOT (n)<-[:revisionOf]-() AND $predicate
        RETURN n

    '''

    search_all_count_statement = '''

        MATCH (n$model)
        WHERE NOT (n)<-[:revisionOf]-() AND $predicate
        RETURN count(n)

    '''

    search_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model)
        WHERE NOT (n)<-[:revisionOf]-() AND $predicate
        RETURN n

    '''

    # Adds the `origins:search` attribute which is a concatenated string of
    # label, description, and type of the entity as well as the the resource
    # label, and the labels of ancestors of the path.
    search_index_statement = '''

        MATCH (e:Entity)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e
        MATCH p=(e)<-[:edge*0..1000]-(n)
        WITH e, extract(x in nodes(p) | n.`prov:label`) as path
        MATCH (e)<-[:manages]-(r:Resource)
        with e, filter(t in [
            e.`prov:label`,
            e.`origins:description`,
            r.`prov:label`,
            e.`prov:type`
        ] + path where t is not null) as tags
        SET e.`origins:search` = lower(reduce(s = '', t in tags | s + ' ' + t))

    '''

    match_by_relation_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model)
        WHERE NOT (n)<-[:revisionOf]-()
        WITH n

    '''  # noqa

    get_by_id_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model {`origins:id`: { id }})
        WHERE NOT (n)<-[:revisionOf]-({`origins:id`: n.`origins:id`})
        RETURN n
        LIMIT 1

    '''  # noqa

    exists_by_id_statement = '''

        MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n$model {`origins:id`: { id }})
        WHERE NOT (n)<-[:revisionOf]-({`origins:id`: n.`origins:id`})
        RETURN true
        LIMIT 1

    '''  # noqa

    link_count_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})<-[:`origins:start`|`origins:end`]-(l:Link)
        RETURN count(DISTINCT l)

    '''  # noqa

    links_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})<-[:`origins:start`|`origins:end`]-(l:Link)
        WITH l
        MATCH (l)-[:`origins:start`]->(s)<-[:manages]-(sr),
              (l)-[:`origins:end`]->(e)<-[:manages]-(er)
        RETURN l, s, sr, e, er

    '''  # noqa

    revisions_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:revisionOf*]->(n)
        RETURN n

    '''  # noqa

    derivations_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})<-[:`prov:usedEntity`]-(d:Derivation)
        WHERE d.`prov:type` <> 'prov:Revision'
        WITH d
        MATCH (d)-[:`prov:generatedEntity`]->(n)
        RETURN n

    '''  # noqa

    # Generation of an entity. The derivation information is included.
    # The arbitrary range is a hack to ensure at least a path with the
    # current entity is returned.
    generation_events_statement = '''

        MATCH p=($model {`origins:uuid`: { uuid }})-[:revisionOf*0..1000]->()
        UNWIND nodes(p) AS e
        MATCH (e)<-[:`prov:entity`]-(g:Generation)
        OPTIONAL MATCH (g)<-[:`prov:generation`]-(d:Derivation)
        OPTIONAL MATCH (d)-[:`prov:usedEntity`]->(e2$model)
        OPTIONAL MATCH (d)-[:`prov:usage`]->(u:Usage)
        OPTIONAL MATCH (g)-[:`prov:activity`]->(a:Activity)
        OPTIONAL MATCH (a)<-[:`prov:activity`]-(as:Association)
        OPTIONAL MATCH (as)-[:`prov:agent`]->(ag:Agent)
        RETURN DISTINCT e, g, d, e2, u, a, as, ag
        ORDER BY g.`prov:time`, g.`origins:time` ASC

    '''  # noqa

    # Derivation of an entity. The derivation information is included.
    # The arbitrary range is a hack to ensure at least a path with the
    # current entity is returned.
    revision_events_statement = '''

        MATCH p=($model {`origins:uuid`: { uuid }})-[:revisionOf*]->()
        UNWIND nodes(p) AS e
        MATCH (e)<-[:`prov:generatedEntity`]-(d:Derivation {`prov:type`: 'prov:Revision'})
        OPTIONAL MATCH (d)-[:`prov:generation`]->(g:Generation)
        OPTIONAL MATCH (d)-[:`prov:usedEntity`]->(e2$model)
        OPTIONAL MATCH (d)-[:`prov:usage`]->(u:Usage)
        OPTIONAL MATCH (g)-[:`prov:activity`]->(a:Activity)
        OPTIONAL MATCH (a)<-[:`prov:activity`]-(as:Association)
        OPTIONAL MATCH (as)-[:`prov:agent`]->(ag:Agent)
        RETURN DISTINCT e, d, g, e2, u, a, as, ag
        ORDER BY g.`prov:time`, g.`origins:time` ASC

    '''  # noqa

    # Invalidation not caused by a revision.
    invalidation_events_statement = '''

        MATCH (e$model {`origins:uuid`: { uuid }})<-[:`prov:entity`]-(i:Invalidation)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e, i
        OPTIONAL MATCH (i)-[:`prov:activity`]->(a:Activity)
        OPTIONAL MATCH (a)<-[:`prov:activity`]-(as:Association)
        OPTIONAL MATCH (as)-[:`prov:agent`]->(ag:Agent)
        RETURN DISTINCT e, i, a, as, ag
        ORDER BY i.`prov:time`, i.`origins:time` ASC

    '''  # noqa

    atrributions_statement = '''

        MATCH (e$model {`origins:uuid`: { uuid }})<-[:`prov:entity`]-(at:Attribution)
        OPTIONAL MATCH (at)-[:`prov:agent`]->(ag:Agent)
        RETURN e, at, ag

    '''  # noqa

    # Odd optimization, but using WITH reduces the time by 5x
    path_statement = '''

        MATCH (e$model {`origins:uuid`: { uuid }})
        WITH e
        MATCH (e)<-[:edge*]-(n)
        WHERE NOT (n)<-[:revisionOf]-()
        RETURN n

    '''

    children_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:edge]->(n)
        RETURN n

    '''

    def match_all_query(self, predicate=None, limit=None, skip=None):
        return traverse.match(self.match_all_statement,
                              predicate=predicate,
                              limit=limit,
                              skip=skip,
                              model=self.label)

    def search_all_query(self, query, operator=None, limit=None,
                         skip=None):
        return traverse.fulltext_search(self.search_all_statement,
                                        query=query,
                                        limit=limit,
                                        skip=skip,
                                        model=self.label)

    def search_all_count_query(self, query, operator=None):
        return traverse.fulltext_search(self.search_all_count_statement,
                                        query=query,
                                        model=self.label)

    def search_index_query(self):
        statement = cypher.prepare(self.search_index_statement)

        return {
            'statement': statement,
            'parameters': None
        }

    def revisions_query(self, uuid):
        statement = cypher.prepare(self.revisions_statement,
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

    def derivations_query(self, uuid):
        statement = cypher.prepare(self.derivations_statement,
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

    def path_query(self, uuid):
        statement = cypher.prepare(self.path_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def children_query(self, uuid):
        statement = cypher.prepare(self.children_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def match_all(self, predicate=None, limit=None, skip=None, tx=neo4j.tx):
        query = self.match_all_query(predicate=predicate,
                                     limit=limit,
                                     skip=skip)

        result = tx.send(query)

        return [self.model.parse(*r) for r in result]

    def search_all(self, query, limit=None, skip=None, tx=neo4j.tx):
        count_query = self.search_all_count_query(query=query)

        query = self.search_all_query(query=query,
                                      limit=limit,
                                      skip=skip)

        result = tx.send([
            count_query,
            query,
        ])

        count = result[0][0]

        return count, [self.model.parse(*r) for r in result[1:]]

    def build_search_index(self, tx=neo4j.tx):
        query = self.search_index_query()

        tx.send(query)

    def derivations(self, uuid, tx=neo4j.tx):
        query = self.derivations_query(uuid)

        result = tx.send(query)

        return [models.parse(r) for r in result]

    def revisions(self, uuid, tx=neo4j.tx):
        query = self.revisions_query(uuid)

        result = tx.send(query)

        return [models.parse(r) for r in result]

    def link_count(self, uuid, tx=neo4j.tx):
        query = self.link_count_query(uuid)

        result = tx.send(query)

        if result:
            return result[0][0]

        return 0

    def links(self, uuid, tx=neo4j.tx):
        query = self.links_query(uuid)

        links = []
        result = tx.send(query)

        for l, s, sr, e, er in result:
            link = models.parse(l)
            start = models.parse(s, resource=models.parse(sr))
            end = models.parse(e, resource=models.parse(er))

            links.append((link, start, end))

        return links

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

                # Add time directly onto the event
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

                # Add time directly onto the event
                event['uuid'] = event['invalidation'].uuid
                event['time'] = event['invalidation'].time

                events.append(event)

            if _as:
                event['associations'].append({
                    'association': models.parse(_as),
                    'agent': models.parse(ag),
                })

        return events

    def attribution(self, uuid, tx=neo4j.tx):
        query = self.attribution_query(uuid)

        return [models.parse(r[0]) for r in tx.send(query)]

    def path(self, uuid, tx=neo4j.tx):
        query = self.path_query(uuid)

        return [models.parse(r[0]) for r in tx.send(query)]

    def children(self, uuid, tx=neo4j.tx):
        query = self.children_query(uuid)

        return [models.parse(r[0]) for r in tx.send(query)]

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
