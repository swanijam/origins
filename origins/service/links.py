import traceback
from flask import request
from flask.ext import restful
from origins import log, managers, models
from origins.graph import neo4j
from . import utils, prepare


class BaseLink(restful.Resource):
    # TODO add pagination
    def list_links(self):
        return utils.header_links({
            'self': {
                'endpoint': 'links',
            },
        })

    def list_link_templates(self):
        return utils.header_links({
            'self': {
                'endpoint': 'link',
                'uuid': '{uuid}',
            },
        }, template=True)

    def item_links(self, item):
        uuid = item['uuid']

        return utils.header_links({
            'self': {
                'endpoint': 'link',
                'uuid': uuid,
            },
        })

    def list_headers(self):
        return [
            ('Link', self.list_links()),
            ('Link-Template', self.list_link_templates()),
        ]

    def item_headers(self, item):
        return [
            ('Link', self.item_links(item)),
        ]


def list_headers():
    header = {}

    link_header = utils.header_links({
        'self': {
            'endpoint': 'links'
        }
    })

    header['Link'] = link_header

    return header


manager = managers.get(models.Link)


class ListResource(BaseLink):
    def get(self):
        results = manager.match_all()

        return results, 200, list_headers()

    def post(self):
        attrs = request.json

        stmt = log.Statement('add', 'Link', params={
            'attrs': attrs,
        })

        try:
            log.process([stmt])
        except Exception:
            return {'message': traceback.format_exc()}, 422

        item = stmt.instance.serialize()
        attrs = prepare.prepare_link(item)

        return attrs, 201, self.item_headers(attrs)


class ItemResource(BaseLink):
    def get(self, uuid):
        result = neo4j.tx.send({
            'statement': LINK_STATEMENT,
            'parameters': {
                'uuid': uuid,
            }
        })

        if not result:
            return {'message': 'link not found'}, 404

        item = prepare.prepare_link(result[0])

        return item, 200, self.item_headers(item)

    def put(self, uuid):
        attrs = request.json()
        attrs['origins:uuid'] = uuid

        stmt = log.Statement('update', 'Link', params={
            'attrs': attrs,
        })

        try:
            log.process([stmt])
        except Exception as e:
            return {'message': str(e)}, 422

        item = stmt.instance

        return '', 204, self.item_headers(item)

    def delete(self, uuid):
        stmt = log.Statement('remove', 'Link', params={
            'attrs': {
                'origins:uuid': uuid,
            }
        })

        try:
            log.process([stmt])
        except Exception as e:
            return {'message': str(e)}, 422

        return '', 204
