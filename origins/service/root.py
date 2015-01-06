from flask.ext import restful
from . import utils


def get_headers():
    header = {}

    links = {
        'self': {
            'endpoint': 'root',
        },
        'resources': {
            'endpoint': 'resources',
        },
        'topologies': {
            'endpoint': 'topologies',
        },
        'entities': {
            'endpoint': 'entities',
        },
        'links': {
            'endpoint': 'links',
        },
    }

    header['Link'] = utils.header_links(links)

    link_templates = {
        'resource': {
            'endpoint': 'resource',
            'id': '{id}',
        },
        'topology': {
            'endpoint': 'topology',
            'id': '{id}',
        },
        'link': {
            'endpoint': 'link',
            'uuid': '{uuid}',
        },
        'entity': {
            'endpoint': 'entity',
            'uuid': '{uuid}',
        },
    }

    header['Link-Template'] = utils.header_links(link_templates, True)

    return header


class RootResource(restful.Resource):
    def get(self):
        body = {
            'version': 1.0,
            'title': 'Origins API',
        }

        return body, 200, get_headers()
