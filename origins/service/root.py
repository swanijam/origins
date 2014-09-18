from urllib.parse import unquote
from flask import url_for
from flask.ext import restful


class RootResource(restful.Resource):
    def get(self):
        return {
            'version': 1.0,
            'title': 'Origins API',
            'links': {
                'nodes': {
                    'href': url_for('nodes', _external=True),
                },
                'edges': {
                    'href': url_for('edges', _external=True),
                },
                'collections': {
                    'href': url_for('collections', _external=True),
                },
                'resources': {
                    'href': url_for('resources', _external=True),
                },
                'components': {
                    'href': url_for('components', _external=True),
                },
                'relationships': {
                    'href': url_for('relationships', _external=True),
                },
                'trends': {
                    'href': url_for('trends', _external=True),
                },

                # Templates for single objects
                'node': {
                    'href': unquote(url_for('node', uuid='{uuid}',
                                            _external=True)),
                },
                'edge': {
                    'href': unquote(url_for('edge', uuid='{uuid}',
                                            _external=True)),
                },
                'collection': {
                    'href': unquote(url_for('collection', uuid='{uuid}',
                                            _external=True)),
                },
                'resource': {
                    'href': unquote(url_for('resource', uuid='{uuid}',
                                            _external=True)),
                },
                'component': {
                    'href': unquote(url_for('component', uuid='{uuid}',
                                            _external=True)),
                },
                'relationship': {
                    'href': unquote(url_for('relationship', uuid='{uuid}',
                                            _external=True)),
                },
            }
        }
