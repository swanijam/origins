from flask import request, url_for
from flask.ext import restful
from origins import managers, packer
from origins.exceptions import ValidationError
from origins.models import Topology, Link
from . import utils, entities


manager = managers.get(Topology)


def list_headers():
    header = {}

    links = {
        'self': {
            'endpoint': 'topologies',
        },
    }

    header['Link'] = utils.header_links(links)

    link_templates = {
        'self': {
            'endpoint': 'topology',
            'id': '{id}',
        },
        'links': {
            'endpoint': 'topology-links',
            'id': '{id}',
        },
        'entities': {
            'endpoint': 'topology-entities',
            'id': '{id}',
        },
        'feed': {
            'endpoint': 'topology-feed',
            'id': '{id}',
        },
        'import': {
            'endpoint': 'topology-import',
            'id': '{id}',
        },
    }

    header['Link-Template'] = utils.header_links(link_templates, template=True)

    return header


def item_headers(item):
    id = item['id']

    header = {}

    links = {
        'self': {
            'endpoint': 'topology',
            'id': id,
        },
        'links': {
            'endpoint': 'topology-links',
            'id': id,
        },
        'entities': {
            'endpoint': 'topology-entities',
            'id': id,
        },
        'feed': {
            'endpoint': 'topology-feed',
            'id': id,
        },
        'import': {
            'endpoint': 'topology-import',
            'id': id,
        },
    }

    header['Link'] = utils.header_links(links)

    return header


def prepare_instance(instance):
    attrs = instance.serialize(unpack=True)

    attrs['link_count'] = manager.link_count(instance.uuid)
    attrs['broken_link_count'] = manager.broken_link_count(instance.uuid)
    attrs['entity_count'] = manager.entity_count(instance.uuid)
    attrs['linked_resources'] = manager.linked_resources(instance.uuid)

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

        items = [prepare_instance(i) for i in instances]

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

        instance = Topology(attrs)
        manager.add(instance)

        item = prepare_instance(instance)

        return item, 201, item_headers(item)


class ItemResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'error': 'data',
                'message': 'not found',
            }, 404

        item = prepare_instance(instance)

        return item, 200, item_headers(item)

    def put(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'error': 'data',
                'message': 'not found',
            }, 404

        attrs = request.json

        # Validate
        try:
            attrs = clean_put(attrs)
        except ValidationError as e:
            return {
                'type': 'validation',
                'message': str(e),
            }, 422

        instance.attrs.update(attrs)
        manager.update(instance)

        return '', 204

    def delete(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'error': 'data',
                'message': 'not found',
            }, 404

        manager.remove(instance)

        return '', 204


class PathsResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        items = []

        for path in manager.paths(instance.uuid):
            item = []

            for link in path:
                item.append(link.serialize(unpack=True))

            items.append(item)

        return items, 202


class LinksResource(restful.Resource):
    def clean_post(self, attrs):
        if not attrs.get('start'):
            raise ValidationError('start required')

        if not attrs.get('end'):
            raise ValidationError('end required')

        return packer.pack({
            'start': attrs['start'],
            'end': attrs['end'],
            'label': attrs.get('label'),
            'description': attrs.get('description'),
            'type': attrs.get('type'),
        })

    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        links = []

        for link, start, end, broken in manager.links(instance.uuid):
            attrs = link.serialize(unpack=True)

            attrs['start'] = start.serialize(unpack=True)
            attrs['end'] = end.serialize(unpack=True)
            attrs['broken'] = broken

            links.append(attrs)

        return links

    def post(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        attrs = request.json

        try:
            attrs = self.clean_post(attrs)
        except ValidationError as e:
            return {
                'type': 'validation',
                'message': str(e),
            }, 422

        link = Link(attrs, topology=instance)
        link_manager = managers.get(Link)
        link_manager.add(link)

        header = {
            'Location': url_for('link', uuid=link.uuid, _external=True)
        }

        return '', 201, header


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


class EntitiesResource(restful.Resource):
    def get(self, id):
        instance = manager.get_by_id(id)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        _entities = manager.entities(instance.uuid)

        items = []

        for e in _entities:
            items.append(entities.prepare_instance(e))

        return items, 200, entities.list_headers()


class ImportResource(restful.Resource):
    def post(self, id):
        pass
