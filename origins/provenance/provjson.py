import logging
from collections import defaultdict
from origins import models, managers
from origins.graph import neo4j
from origins.log import Statement
from origins.models import Entity, Activity, Generation, Usage, \
    Derivation, Invalidation


logger = logging.getLogger(__name__)


VALID_ENTITY_STATEMENT = '''

    MATCH (:Resource {`origins:uuid`: { resource }})-[:manages]->(n:Entity)
    WHERE NOT (n)<-[:`prov:entity`]-(:Invalidation)
    RETURN n.`origins:uuid`

'''  # noqa


XSD_CONVERTERS = {
    'xsd:long': int,
    'xsd:integer': int,
    'xsd:double': float,
    'xsd:boolean': bool,
}


# List of concepts in order of dependency
ORDERED_PROV_TERMS = (
    'bundle',
    'entity',
    'activity',
    'wasGeneratedBy',
    'used',
    'wasInformedBy',
    'wasStartedBy',
    'wasEndedBy',
    'wasInvalidatedBy',
    'wasDerivedFrom',
    'agent',
    'wasAttributedTo',
    'wasAssociatedWith',
    'actedOnBehalfOf',
    'wasInfluencedBy',
    'specializationOf',
    'alternateOf',
    'hadMember',
    'mentionOf',
)


IDENTIFIERS = {'origins:id', 'origins:uuid'}


MODEL_PROV_MAP = {
    'Entity': 'entity',
    'Activity': 'activity',
    'Agent': 'agent',
    'Generation': 'wasGeneratedBy',
    'Usage': 'used',
    'Communication': 'wasInformedBy',
    'Start': 'wasStartedBy',
    'End': 'wasEndedBy',
    'Invalidation': 'wasInvalidatedBy',
    'Derivation': 'wasDerivedFrom',
    'Attribution': 'wasAttributedTo',
    'Association': 'wasAssociatedWith',
    'Delegation': 'actedOnBehalfOf',
    'Influence': 'wasInfluencedBy',
    'Alternate': 'alternateOf',
    'Specialization': 'specializationOf',
    'Membership': 'hadMember',
}


MODEL_PROV_RMAP = {v: k for k, v in MODEL_PROV_MAP.items()}


def parse_xsd(value):
    "Converts an XSD declared type into a native type."
    if isinstance(value, dict):
        if '$' in value:
            if value['type'] in XSD_CONVERTERS:
                converter = XSD_CONVERTERS[value['type']]
                return converter(value['$'])

            return value['$']

    return value


def merge_attrs(seq):
    attrs = {}

    for s in seq:
        attrs.update(s)

    return attrs


class Handler(dict):
    """Type representing a PROV document.

    Builds inverted indexes of related processed in a provenance document.

    The index enables passing an identifier and getting the processed pointing
    to it. For example, if `prov:entity` is being indexed for `generation`,
    each `prov:entity` value will be keyed to the generation event pointing
    to it.
    """

    def __init__(self, resource, desc, invalidate=False, tx=neo4j.tx):
        self.resource = resource
        self.desc = desc
        self.invalidate = invalidate
        self.tx = tx
        self.indexes = {}
        self.seen = set()
        self.statements = defaultdict(list)
        self.processed = {}
        self.cid_statements = {}

    def _index_get(self, concept, model, attr, value):
        """Finds an instance of `model` with the specified attribute value.

        For example, a generation event that has a prov:entity attribute
        to an identifier `foo`. The lookup builds an index for the concept
        and attribute.
        """
        # Concept does not exist in description
        if concept not in self.desc:
            return (None, None)

        index_key = (concept, attr)

        if index_key not in self.indexes:
            index = {}

            # Build the index of processed for this lookup. If an instance
            # is not in the cache, the object will be initialized.
            for cid in self.desc[concept]:
                object_key = (concept, cid)

                # Get or initialize instance
                if object_key in self.processed:
                    instance = self.processed[object_key]
                    attrs = instance.attrs
                else:
                    attrs = self.parse_item(concept, cid)
                    instance = model(attrs)

                if attr not in attrs:
                    continue

                # Index the concept id and instance
                index[attrs[attr]] = (cid, instance)

            self.indexes[index_key] = index
        else:
            index = self.indexes[index_key]

        result = index.get(value)

        if result:
            return result

        return (None, None)

    def _queue(self, keyword, concept, cid, instance):
        "Builds and queues a statement for the instance."
        object_key = (concept, cid)

        if object_key in self.processed:
            return

        # Bind the resource and add it to the object cache so subsequent
        # uses of the object do not result in redundant statements.
        instance.resource = self.resource
        self.processed[object_key] = instance

        stmt = Statement(keyword, instance=instance, id=cid)
        self.statements[concept].append(stmt)
        self.cid_statements[cid] = stmt

    def _get_remote(self, concept, model, cid, attrs):
        "Attempts to find a remote node that matches this description."
        manager = managers.get(model)

        # If an Origins identifier is present, fetch the remote for
        # comparison, otherwise assume it's a new node.
        if 'origins:uuid' in attrs:
            return manager.get(attrs['origins:uuid'],
                               tx=self.tx)

        if 'origins:id' in attrs:
            return manager.get_by_id(self.resource.uuid,
                                     attrs['origins:id'],
                                     tx=self.tx)

        if not model.relations:
            return

        # For concepts that support relations, a remote is lookup by the
        # unique combination of relations it has. If all of the relations
        # have stayed the same locally, then this description may be comparable
        # to the remote. If exactly one remote is found, it is processed as
        # normal. If more than one remote is found, each remote is compared
        # against to prevent adding a redundant node. If no exact matches are
        # found a new node is added rather than updating an existing one since
        # which one is ambiguous.
        predicate = {}

        for attr in model.relations:
            if attr in attrs and attrs[attr] is not None:
                try:
                    stmt = self.cid_statements[attrs[attr]]
                except KeyError:
                    raise KeyError('{}#{} points to non-existent object {}'
                                   .format(concept, attr,
                                           attrs[attr]))

                # Relations that have not changed can be included
                if stmt.keyword == 'noop':
                    uuid = stmt.instance.uuid
                    predicate[attr] = uuid

        # At least one relation must be present
        if predicate:
            remotes = manager.match_by_relation(self.resource.uuid,
                                                predicate, tx=self.tx)

            # Exactly one remote, process as normal
            if len(remotes) == 1:
                return remotes[0]

            # Multiple remotes, if any of them match exactly, use that
            # remote (which will be handled as a no-op). Otherwise the remote
            # is assumed not to exist.
            for r in remotes:
                if not r.diff(attrs):
                    return r

    def process(self, model, concept, cid, attrs):
        """Processes a provenance description.

        Some descriptions may only contain identifiers which are used as
        references for other descriptions to be linked to. This is common
        when new descriptions are available for existing entities.

        If a remote value exists it will be compared with the local value to
        determine if an update is required.
        """
        object_key = (concept, cid)

        # Already handled
        if object_key in self.processed:
            return self.processed[object_key]

        remote = self._get_remote(concept, model, cid, attrs)

        # Reference description (remote and only identifiers)
        if remote and not set(attrs) - IDENTIFIERS:
            self._queue('noop', concept, cid, remote)
            return remote

        # Entities may be continuants so a new revision could result
        # if a remote exists.
        if model is Entity:
            instance = self.process_continuant(model, concept, cid, attrs,
                                               remote)
            self.seen.add(instance.uuid)
            return instance

        # Process everything else an occurrent.
        return self.process_occurrent(model, concept, cid, attrs, remote)

    def process_unseen(self):
        """Fetches all valid entities in the graph and invalidates those that
        are not present in the description by initializing a reference node
        with the remote UUID.

        This is only appropriate if the description is representative of all
        entities in the resource.
        """

        # TODO for multi-source resources, how could this lookup be restricted
        # to only get entities from a particular source? The nodes would need
        # to be flagged with this information in some way.
        result = self.tx.send({
            'statement': VALID_ENTITY_STATEMENT,
            'parameters': {
                'resource': self.resource.uuid,
            }
        })

        unseen = set([r[0] for r in result]) - self.seen

        for uuid in unseen:
            remote = Entity({
                'origins:uuid': uuid,
            }, defaults=False)

            self.remove_continuant('entity', remote)

    def process_continuant(self, model, concept, cid, attrs, remote):
        """Process a continuant-based description. This currently is limited
        to entities.

        If a remote exists, the local will be derived and diffed against it to
        determine if a new revision should be created.
        """
        if remote:
            # TODO is this always desired? The semantics or origins:id/uuid
            # suggests this to be the case.
            local = remote.derive(attrs)
            diff = local.diff(remote)

            # If a difference, process the update, otherwise queue a reference
            # to the remote.
            if diff:
                self.update_continuant(cid, local, remote)
            else:
                self._queue('noop', concept, cid, remote)
        else:
            local = model(attrs)
            self.add_continuant(concept, cid, local)

        return local

    def process_occurrent(self, model, concept, cid, attrs, remote):
        """Process an occurrent-based description.

        If a remote exists, the object will be updated in-place if it differs.
        """
        if remote:
            local = remote.derive(attrs)
            diff = local.diff(remote)

            if diff:
                self._queue('update', concept, cid, local)
            else:
                self._queue('noop', concept, cid, remote)
        else:
            local = model(attrs)
            self._queue('add', concept, cid, local)

        return local

    def add_continuant(self, concept, cid, local):
        self._queue('add', concept, cid, local)

        # Get the generation activity if one exists, otherwise
        # create one and an associated activity.
        gid, gen = self._index_get('wasGeneratedBy', Generation,
                                   'prov:entity', cid)

        if gen:
            self._queue('add', 'wasGeneratedBy', gid, gen)
        else:
            act = Activity({
                'prov:type': 'origins:AutoGenerate',
                'prov:label': 'Origins Auto-Generate',
            })

            gen = Generation({
                'prov:entity': cid,
                'prov:activity': act.uuid,
            })

            self._queue('add', 'activity', act.uuid, act)
            self._queue('add', 'wasGeneratedBy', gen.uuid, gen)

    def update_continuant(self, concept, cid, local, remote):
        logger.debug('updating %s', cid)

        # Add local entity and reference to remote entity to document
        self._queue('add', concept, cid, local)

        rid = remote.uuid
        self._queue('noop', concept, rid, remote)

        # Create usage of the remote entity
        usg = Usage({
            'prov:entity': rid,
        })
        uid = usg.uuid

        self._queue('add', 'used', uid, usg)

        # Get the generation activity if one exists, otherwise
        # create one and an associated activity.
        gid, gen = self._index_get('wasGeneratedBy', Generation,
                                   'prov:entity', cid)

        # Activity ID. If one is defined for the generation event, then we
        # can reference that in the derivation.
        act = None
        aid = None

        if gen:
            self._queue('add', 'wasGeneratedBy', gid, gen)

            if gen.attrs.get('prov:activity') is not None:
                aid = gen.attrs['prov:activity']
                act = Activity(self.parse_item('activity', aid))

                self._queue('add', 'activity', aid, act)
        else:
            act = Activity({
                'prov:type': 'origins:AutoVersion',
                'prov:label': 'Origins Auto-Version',
            })
            aid = act.uuid

            gen = Generation({
                'prov:entity': local,
                'prov:activity': act,
            })
            gid = gen.uuid

            self._queue('add', 'activity', aid, act)
            self._queue('add', 'wasGeneratedBy', gid, gen)

        # Create derivation of remote to local
        der = Derivation({
            'prov:usedEntity': rid,
            'prov:generatedEntity': cid,
            'prov:usage': uid,
            'prov:activity': aid,
            'prov:generation': gid,
            'prov:type': 'prov:Revision',
        })

        # Invalidate the remote entity
        inv = Invalidation({
            'prov:entity': rid,
            'prov:activity': aid,
        })

        self._queue('add', 'wasDerivedFrom', der.uuid, der)
        self._queue('add', 'wasInvalidatedBy', inv.uuid, inv)

    def remove_continuant(self, concept, remote):
        act = Activity({
            'prov:type': 'origins:AutoInvalidate',
            'prov:label': 'Origins Auto-Invalidate',
        })

        inv = Invalidation({
            'prov:entity': remote.uuid,
            'prov:activity': act.uuid,
        })

        self._queue('noop', concept, remote.uuid, remote)
        self._queue('add', 'activity', act.uuid, act)
        self._queue('add', 'wasInvalidatedBy', inv.uuid, inv)

    def parse_item(self, concept, cid):
        "Parse a concept's attributes."
        attrs = self.desc[concept][cid]

        # List of attrs for the same ID can be merged in order
        if isinstance(attrs, (list, tuple)):
            attrs = merge_attrs(attrs)

        attrs = dict(attrs)

        for key, value in list(attrs.items()):
            if value is None:
                attrs.pop(key)
            elif isinstance(value, (list, tuple)):
                attrs[key] = [parse_xsd(v) for v in value]
            else:
                attrs[key] = parse_xsd(value)

        return attrs

    def evaluate(self):
        """Takes a PROV description, evaluates it against the graph, and produces
        a PROV document that can be loaded.
        """

        # Evaluate in the order of dependency
        for concept in ORDERED_PROV_TERMS:
            # Skip missing concepts
            if concept not in self.desc:
                continue

            for cid in self.desc[concept]:
                # Get the current item
                attrs = self.parse_item(concept, cid)
                model = models.get(concept, attrs)
                self.process(model, concept, cid, attrs)

        if self.invalidate:
            self.process_unseen()

        # Emit statements in order
        statements = []

        for concept in ORDERED_PROV_TERMS:
            if concept in self.statements:
                statements.extend(self.statements[concept])

        return statements


def evaluate(resource, desc, invalidate=False, tx=neo4j.tx):
    handler = Handler(resource, desc, invalidate=invalidate, tx=tx)
    return handler.evaluate()


def reconcile(resource):
    """Reconcile the resource by checking for redundant provenance and
    inferring relations between processed.

    Rules are based on the PROV constraints document:
    http://www.w3.org/TR/2013/REC-prov-constraints-20130430/
    """
