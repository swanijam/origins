import json
from flask import Flask, make_response
from flask.ext import restful
from flask_cors import CORS
from origins import config
from origins.models import Model
from . import root, resources, entities, \
    links, topologies, generator


app = Flask(__name__)

cors_methods = (
    'PUT',
    'POST',
    'PATCH',
    'DELETE',
)

cors_headers = (
    'Content-Type',
)

cors_expose_headers = (
    'Link',
    'Link-Template',
)

cors = CORS(app,
            methods=cors_methods,
            headers=cors_headers,
            expose_headers=cors_expose_headers,
            send_wildcard=False,
            supports_credentials=True)

api = restful.Api(app)


def model_serializer(o):
    if isinstance(o, Model):
        return o.serialize(unpack=True)

    raise TypeError


@api.representation('application/json')
def json_representation(data, code, headers=None):
    if data is None:
        data = ''
    elif not isinstance(data, (str, bytes)):
        indent = 4 if config.options['debug'] else None
        data = json.dumps(data, indent=indent, default=model_serializer)

    response = make_response(data, code)

    if headers:
        for key, value in headers.items():
            response.headers[key] = value

    return response


routes = [
    (root.RootResource,
     '/',
     'root'),

    (resources.ListResource,
     '/resources/',
     'resources'),

    (generator.GeneratorResource,
     '/generator/',
     'generator'),

    (resources.ItemResource,
     '/resources/<id>/',
     'resource'),

    (resources.EntitiesResource,
     '/resources/<id>/entities/',
     'resource-entities'),

    (resources.EntityRootsResource,
     '/resources/<id>/entities/root/',
     'resource-entity-roots'),

    (resources.LinksResource,
     '/resources/<id>/links/',
     'resource-links'),

    (resources.FeedResource,
     '/resources/<id>/feed/',
     'resource-feed'),

    (resources.ImportResource,
     '/resources/<id>/import/',
     'resource-import'),

    (topologies.ListResource,
     '/topologies/',
     'topologies'),

    (topologies.ItemResource,
     '/topologies/<id>/',
     'topology'),

    (topologies.LinksResource,
     '/topologies/<id>/links/',
     'topology-links'),

    (topologies.EntitiesResource,
     '/topologies/<id>/entities/',
     'topology-entities'),

    (topologies.FeedResource,
     '/topologies/<id>/feed/',
     'topology-feed'),

    (topologies.ImportResource,
     '/topologies/<id>/import/',
     'topology-import'),

    (entities.ListResource,
     '/entities/',
     'entities'),

    (entities.ItemResource,
     '/entities/<uuid>/',
     'entity'),

    (entities.FeedResource,
     '/entities/<uuid>/feed/',
     'entity-feed'),

    (entities.LinksResource,
     '/entities/<uuid>/links/',
     'entity-links'),

    (entities.ChildrenResource,
     '/entities/<uuid>/children/',
     'entity-children'),

    (links.ListResource,
     '/links/',
     'links'),

    (links.ItemResource,
     '/links/<uuid>/',
     'link'),
]

for resource, path, name in routes:
    api.add_resource(resource, path, endpoint=name)
