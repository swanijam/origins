from urllib.parse import unquote
from flask import url_for


def header_links(links, template=False):
    items = []

    for name, url in links.items():
        if isinstance(url, dict):
            url = url_for(_external=True, **url)

        if template:
            url = unquote(url)

        items.append('<{}>; rel="{}"'.format(url, name))

    return ','.join(items)
