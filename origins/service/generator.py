import os
import json
import requests
from origins import config
from flask import request
from flask.ext import restful


def send_request(method, source=None, data=None, files=None):
    loc = config.options['generator']
    url = 'http://{}:{}'.format(loc['host'], loc['port'])

    if source:
        url = os.path.join(url, source) + '/'

    headers = {'Content-Type': 'application/json'}

    if data is not None:
        data = json.dumps(data)

    resp = requests.request(method,
                            url=url,
                            data=data,
                            files=files,
                            headers=headers)

    # Return unicode content, code, and headers
    return resp.text, resp.status_code, resp.headers


class GeneratorResource(restful.Resource):
    "Endpoint proxy to the Origins generator service."
    def get(self):
        content, code, headers = send_request('get')

        return content, code, headers

    def post(self):
        data = request.json

        content, code, headers = send_request('post',
                                              source=data['source'],
                                              data=data['options'],
                                              files=request.files)

        return content, code, headers
