import math
from urllib.parse import urlencode
from flask import request, url_for
from flask.ext import restful
from origins import managers
from origins.models import Entity
from . import utils


manager = managers.get(Entity)

MIN_PAGE = 1
MAX_LIMIT = 100
DEFAULT_LIMIT = 30


def item_headers(item):
    uuid = item['uuid']

    header = {}

    links = {
        'self': {
            'endpoint': 'entity',
            'uuid': uuid,
        },
        'feed': {
            'endpoint': 'entity-feed',
            'uuid': uuid,
        },
        'links': {
            'endpoint': 'entity-links',
            'uuid': uuid,
        },
        'children': {
            'endpoint': 'entity-children',
            'uuid': uuid,
        },
    }

    header['Link'] = utils.header_links(links)

    return header


def pagination_links(page, limit, count):
    links = {}
    base_url = url_for('entities', _external=True)

    args = dict(request.args)
    args['limit'] = limit

    # Current page
    args['page'] = page
    links['self'] = '{}?{}'.format(base_url, urlencode(args, doseq=True))

    # First page
    first_page = MIN_PAGE
    args['page'] = first_page
    links['first'] = '{}?{}'.format(base_url, urlencode(args, doseq=True))

    # Last page
    last_page = math.ceil(count / limit)
    args['page'] = last_page
    links['last'] = '{}?{}'.format(base_url, urlencode(args, doseq=True))

    # Previous
    if page > 1:
        args['page'] = page - 1
        links['prev'] = '{}?{}'.format(base_url, urlencode(args, doseq=True))

    # Next page
    if page < last_page:
        args['page'] = page + 1
        links['next'] = '{}?{}'.format(base_url, urlencode(args, doseq=True))

    return links


def list_headers(page=None, limit=None, count=None):
    header = {}

    links = {
        'self': {
            'endpoint': 'entities',
        },
    }

    # Add pagination links
    if page:
        page_links = pagination_links(page, limit, count)
        links.update(page_links)

    header['Link'] = utils.header_links(links)

    link_templates = {
        'self': {
            'endpoint': 'entity',
            'uuid': '{uuid}',
        },
        'feed': {
            'endpoint': 'entity-feed',
            'uuid': '{uuid}',
        },
        'links': {
            'endpoint': 'entity-links',
            'uuid': '{uuid}',
        },
        'children': {
            'endpoint': 'entity-children',
            'uuid': '{uuid}',
        },
    }

    header['Link-Template'] = utils.header_links(link_templates, template=True)

    return header


def prepare_instance(instance, resource=None):
    attrs = instance.serialize(unpack=True)

    if resource is None:
        resource = manager.resource(instance.uuid)
        resource = resource.serialize(unpack=True)

    attrs['link_count'] = manager.link_count(instance.uuid)
    attrs['resource'] = resource
    attrs['path'] = [e.serialize(unpack=True)
                     for e in manager.path(instance.uuid)]

    return attrs


class ListResource(restful.Resource):
    def get(self):
        query = request.args.getlist('query')

        # Pagination
        limit = None
        page = None
        count = None

        if query:
            try:
                limit = min(int(request.args.get('limit')), MAX_LIMIT)
            except (ValueError, TypeError):
                limit = 30

            try:
                page = max(int(request.args.get('page')), MIN_PAGE)
            except (ValueError, TypeError):
                page = 1

            skip = (page - 1) * limit

            count, instances = manager.search_all(query,
                                                  skip=skip,
                                                  limit=limit)
        else:
            instances = manager.match_all()

        items = [prepare_instance(i) for i in instances]

        return items, 200, list_headers(page=page,
                                        limit=limit,
                                        count=count)


class ItemResource(restful.Resource):
    def get(self, uuid):
        instance = manager.get(uuid)

        if not instance:
            return {
                'error': 'data',
                'message': 'not found',
            }, 404

        item = prepare_instance(instance)

        return item, 200, item_headers(item)


class FeedResource(restful.Resource):
    def get(self, uuid):
        instance = manager.get(uuid)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        events = manager.feed(uuid)

        return events, 200


class LinksResource(restful.Resource):
    def get(self, uuid):
        instance = manager.get(uuid)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        links = []

        for link, start, end in manager.links(uuid):
            attrs = link.serialize(unpack=True)

            attrs['start'] = start.serialize(unpack=True)
            attrs['end'] = end.serialize(unpack=True)

            links.append(attrs)

        return links


class ChildrenResource(restful.Resource):
    def get(self, uuid):
        instance = manager.get(uuid)

        if not instance:
            return {
                'type': 'data',
                'message': 'not found',
            }, 404

        children = manager.children(uuid)
        items = [prepare_instance(e) for e in children]

        return items, 200, list_headers()
