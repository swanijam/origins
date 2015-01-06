import json
import codecs
import traceback
from flask import request
from flask.ext import restful
from origins.exceptions import ValidationError
from origins import log, provenance, managers, packer, utils
from origins.models import Resource, Entity
from origins.graph import neo4j
from .utils import header_links
from . import generator, entities


manager = managers.get(Resource)


def list_headers():
    "Link header for list."
    header = {}

    # TODO add pagination
    links = {
        'self': {
            'endpoint': 'resources',
        },
    }

    header['Link'] = header_links(links)

    link_templates = {
        'self': {
            'endpoint': 'resource',
            'id': '{id}',
        },
        'entities': {
            'endpoint': 'resource-entities',
            'id': '{id}',
        },
        'entity_roots': {
            'endpoint': 'resource-entity-roots',
            'id': '{id}',
        },
        'links': {
            'endpoint': 'resource-links',
            'id': '{id}',
        },
        'feed': {
            'endpoint': 'resource-feed',
            'id': '{id}',
        },
        'import': {
            'endpoint': 'resource-import',
            'id': '{id}',
        },
    }

    header['Link-Template'] = header_links(link_templates, template=True)

    return header


def item_headers(item):
    id = item['id']

    header = {}

    links = {
        'self': {
            'endpoint': 'resource',
            'id': id,
        },
        'entities': {
            'endpoint': 'resource-entities',
            'id': id,
        },
        'entity_roots': {
            'endpoint': 'resource-entity-roots',
            'id': id,
        },
        'links': {
            'endpoint': 'resource-links',
            'id': id,
        },
        'feed': {
            'endpoint': 'resource-feed',
            'id': id,
        },
        'import': {
            'endpoint': 'resource-import',
            'id': id,
        },
    }

    header['Link'] = header_links(links)

    return header


def prepare_instance(instance):
    attrs = instance.serialize(unpack=True)

    attrs['entity_types'] = manager.entity_types(instance.uuid)
    attrs['entity_count'] = manager.entity_count(instance.uuid)
    attrs['link_count'] = manager.link_count(instance.uuid)
    attrs['broken_link_count'] = manager.broken_link_count(instance.uuid)

    return attrs


allowed_attrs = {'id', 'label', 'description', 'type'}


def clean_post(attrs):
    if not attrs.get('label'):
        raise ValidationError('Label required')

    _attrs = {k: attrs.get(k) for k in allowed_attrs}

    return packer.pack(_attrs)


def clean_put(attrs):
    if not attrs.get('label'):
        raise ValidationError('Label required')

    if not attrs.get('id'):
        raise ValidationError('Id required')

    _attrs = {k: attrs.get(k) for k in allowed_attrs}

    return packer.pack(_attrs)


class ListResource(restful.Resource):
    def get(self):
        instances = manager.match()

        items = [prepare_instance(r) for r in instances]

        return items, 200, list_headers()

    def post(self):
        attrs = request.json

        # Validate
        try:
            attrs = clean_post(attrs)
        except ValidationError as e:
            return {
                'type': 'validation',
                'message': str(e),
            }, 422

        instance = Resource(attrs)
        manager.add(instance)

        item = prepare_instance(instance)

        return item, 201, item_headers(item)


class ItemResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        item = prepare_instance(instance)

        return item, 200, item_headers(item)

    def put(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        attrs = request.json

        try:
            attrs = clean_put(attrs)
        except ValidationError as e:
            return {
                'type': 'validation',
                'message': str(e),
            }, 422

        if instance.diff(attrs):
            instance.attrs.update(attrs)
            manager.update(instance)

        return '', 204

    def delete(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        manager.remove(instance)

        return '', 204


class EntitiesResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        _entities = manager.entities(instance.uuid)
        resource = instance.serialize(unpack=True)

        items = []

        for e in _entities:
            item = e.serialize(unpack=True)
            item['resource'] = resource
            items.append(item)

        return items, 200, entities.list_headers()


class EntityRootsResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        _entities = manager.entity_roots(instance.uuid)
        resource = instance.serialize(unpack=True)

        items = []

        for e in _entities:
            item = e.serialize(unpack=True)
            item['resource'] = resource
            items.append(item)

        return items, 200


class FeedResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        events = manager.feed(instance.uuid)

        return events, 200


class LinksResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        links = []

        for link, start, end in manager.links(instance.uuid):
            attrs = link.serialize(unpack=True)

            attrs['start'] = start.serialize(unpack=True)
            attrs['end'] = end.serialize(unpack=True)

            links.append(attrs)

        return links


class ImportResource(restful.Resource):
    def post(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        # The import may be a JSON-encoded body or a file
        if request.json:
            data = request.json
        elif request.files:
            try:
                # Wrap file to stream convert bytes to str
                reader = codecs.getreader('utf8')
                data = json.load(reader(request.files['file']))
            except ValueError:
                return {
                    'type': 'serialization',
                    'message': 'file must be JSON',
                }, 422
        else:
            return {
                'type': 'validation',
                'message': 'request body or file required',
            }, 422

        # Data may be a request to generate
        if 'source' in data:
            resp = generator.send_request('post',
                                          source=data['source'],
                                          data=data['options'])
            content, code, headers = resp

            if code != 200:
                return {
                    'type': 'validation',
                    'message': content,
                }, 422

            data = json.loads(content)

        counts = {}
        fake = 'fake' in request.args

        try:
            with neo4j.client.transaction() as tx:
                statements = provenance.evaluate(instance, data, tx=tx)
                commands = log.process(statements, tx=tx)

                for cmd in commands:
                    if cmd.command == 'noop':
                        continue

                    if cmd.command not in counts:
                        counts[cmd.command] = {}

                    counts[cmd.command].setdefault(cmd.model.name, 0)
                    counts[cmd.command][cmd.model.name] += 1

                # TODO: this is not very resource friendly since all of the
                # transaction needs to be held by the server. However this is
                # the only way to support statements affecting the same data.
                if fake:
                    tx.rollback()
                else:
                    # Update timestamp on resource to account for sync time
                    instance.attrs['origins:time'] = utils.timestamp()
                    manager.update(instance, tx=tx)

                    # TODO: have better method of checking if any statements
                    # committed
                    if counts:
                        # Update search index on entities.
                        # TODO: this updates *all* entities which could get
                        # expensive
                        entity_manager = managers.get(Entity)
                        entity_manager.build_search_index(tx=tx)

        except Exception:
            return {
                'message': traceback.format_exc()
            }, 422

        return counts, 200
