from origins import models
from origins.graph import cypher, neo4j
from .manager import Manager


def _build_paths(links, tail=None, path=None):
    if path is None:
        path = []

    for l, s, sr, e, er in links:
        link = models.parse(l)
        start = models.parse(s, resource=models.parse(sr))
        end = models.parse(e, resource=models.parse(er))

        # First link or the path continues
        if tail is None or tail == start:
            _build_paths(links[1:], end, path + [(link, start, end)])
        else:
            return path


class TopologyManager(Manager):
    remove_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }})
        OPTIONAL MATCH (n)-[r:manages]->(d)
        OPTIONAL MATCH (d)-[r2]-()
        DELETE r2, r, d, n
        RETURN 1

    '''

    link_count_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(l:Link)
        RETURN count(l)

    '''

    links_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(l:Link)
        WHERE NOT (l)<-[:revisionOf]-()
        WITH l
        MATCH (l)-[:`origins:start`]->(s)<-[:manages]-(sr),
              (l)-[:`origins:end`]->(e)<-[:manages]-(er)
        OPTIONAL MATCH (s)<-[:`prov:entity`]-(si:Invalidation)
        OPTIONAL MATCH (e)<-[:`prov:entity`]-(ei:Invalidation)
        RETURN l, s, sr, e, er, (si is not null or ei is not null)

    '''

    broken_link_count_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(l:Link)
        WHERE NOT (l)<-[:revisionOf]-()
        WITH l
        MATCH (l)-[:`origins:start`]->(s),
              (l)-[:`origins:end`]->(e)
        WHERE (s)<-[:`prov:entity`]-(:Invalidation)
            OR (e)<-[:`prov:entity`]-(:Invalidation)
        WITH l
        RETURN count(DISTINCT l)

    '''

    paths_statement = '''

        MATCH (:Topology {`origins:uuid`: { uuid }})-[:manages]->(l:Link)
        WHERE NOT (l)<-[:revisionOf]-()
        WITH l
        MATCH (l)-[:`origins:start`]->(s:Entity)
        WHERE NOT (s)<-[:link]-()
        WITH s
        MATCH p=(s)-[:link*]->(e)
        UNWIND rels(p) as r
        WITH DISTINCT startNode(r) as s, endNode(r) as e
        MATCH (s)<-[:`origins:start`]-(l:Link),
              (l)-[:`origins:end`]->(e),
              (s)<-[:manages]-(sr),
              (e)<-[:manages]-(er)
        RETURN l, s, sr, e, er

    '''

    entity_count_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(:Link)-[:`origins:start`|`origins:end`]->(e)
        RETURN count(DISTINCT e)

    '''  # noqa

    entities_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(:Link)-[:`origins:start`|`origins:end`]->(e)
        WITH DISTINCT e
        MATCH (e)<-[:manages]-(r:Resource)
        RETURN e, r

    '''  # noqa

    linked_resources_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(l:Link)
        WHERE NOT (l)<-[:revisionOf]-()
        WITH l
        MATCH (l)-[:`origins:start`|`origins:end`]->(e)<-[:manages]-(r)
        RETURN r, count(distinct l), count(distinct e)

    '''  # noqa

    # Generation events. The derivation information is included.
    generation_events_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Link)
        WITH e
        MATCH (e)<-[:`prov:entity`]-(g:Generation)
        OPTIONAL MATCH (g)<-[:`prov:generation`]-(d:Derivation)
        OPTIONAL MATCH (d)-[:`prov:usedEntity`]->(e2:Link)
        OPTIONAL MATCH (d)-[:`prov:usage`]->(u:Usage)
        OPTIONAL MATCH (g)-[:`prov:activity`]->(a:Activity)
        OPTIONAL MATCH (a)<-[:`prov:activity`]-(as:Association)
        OPTIONAL MATCH (as)-[:`prov:agent`]->(ag:Agent)
        RETURN DISTINCT e, g, d, e2, u, a, as, ag
        ORDER BY g.`prov:time`, g.`origins:time` ASC

    '''  # noqa

    # Invalidation not caused by a revision.
    invalidation_events_statement = '''

        MATCH ($model {`origins:uuid`: { uuid }})-[:manages]->(e:Link)<-[:`prov:entity`]-(i:Invalidation)
        WHERE NOT (e)<-[:revisionOf]-()
        WITH e, i
        OPTIONAL MATCH (i)-[:`prov:activity`]->(a:Activity)
        OPTIONAL MATCH (a)<-[:`prov:activity`]-(as:Association)
        OPTIONAL MATCH (as)-[:`prov:agent`]->(ag:Agent)
        RETURN DISTINCT e, i, a, as, ag
        ORDER BY i.`prov:time`, i.`origins:time` ASC

    '''  # noqa

    def link_count_query(self, uuid):
        statement = cypher.prepare(self.link_count_statement,
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

    def broken_link_count_query(self, uuid):
        statement = cypher.prepare(self.broken_link_count_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def paths_query(self, uuid):
        statement = cypher.prepare(self.paths_statement,
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

    def entities_query(self, uuid):
        statement = cypher.prepare(self.entities_statement,
                                   model=self.label)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    def linked_resources_query(self, uuid):
        statement = cypher.prepare(self.linked_resources_statement,
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

    def link_count(self, uuid, tx=neo4j.tx):
        query = self.link_count_query(uuid)

        result = tx.send(query)

        if result:
            return result[0][0]

    def links(self, uuid, tx=neo4j.tx):
        query = self.links_query(uuid)

        links = []

        for l, s, sr, e, er, broken in tx.send(query):
            link = models.parse(l)
            start = models.parse(s, resource=models.parse(sr))
            end = models.parse(e, resource=models.parse(er))

            links.append((link, start, end, broken))

        return links

    def broken_link_count(self, uuid, tx=neo4j.tx):
        query = self.broken_link_count_query(uuid)

        result = tx.send(query)

        if result:
            return result[0][0]

    def paths(self, uuid, tx=neo4j.tx):
        query = self.paths_query(uuid)

        # Index of end node to the a list path indices
        ends = defaultdict(list)
        paths = []
        path = []
        tail = None

        for l, s, sr, e, er in tx.send(query):
            link = models.parse(l)
            start = models.parse(s, resource=models.parse(sr))
            end = models.parse(e, resource=models.parse(er))

            if path:
                # Continue the chain
                if start != tail:
                    return path

            path.append((link, start, end))

        return links

    def entity_count(self, uuid, tx=neo4j.tx):
        query = self.entity_count_query(uuid)

        result = tx.send(query)

        if result:
            return result[0][0]

    def entities(self, uuid, tx=neo4j.tx):
        query = self.entities_query(uuid)

        entities = []

        for e, r in tx.send(query):
            entity = models.parse(e, resource=models.parse(r))
            entities.append(entity)

        return entities

    def linked_resources(self, uuid, tx=neo4j.tx):
        query = self.linked_resources_query(uuid)

        linked = []

        for r, lc, ec in tx.send(query):
            resource = models.parse(r)

            linked.append({
                'resource': resource,
                'link_count': lc,
                'entity_count': ec,
            })

        return linked

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
            elif i.time > g.time:
                feed.append(i)
                i = None
            else:
                feed.append(g)
                g = None

        return feed
