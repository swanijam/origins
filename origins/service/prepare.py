from origins import packer


def prepare_resource(item):
    return packer.unpack(item)


def prepare_link(item):
    link, start, start_resource, end, end_resource = item

    attrs = packer.unpack(link)

    attrs['start'] = packer.unpack(start)
    attrs['end'] = packer.unpack(end)

    attrs['start']['resource'] = packer.unpack(start_resource)
    attrs['end']['resource'] = packer.unpack(end_resource)

    return attrs


def prepare_entity(item):
    entity, resource = item

    attrs = packer.unpack(entity)
    attrs['resource'] = packer.unpack(resource)

    return attrs


def prepare_agent(item):
    agent, resource = item

    attrs = packer.unpack(agent)
    attrs['resource'] = packer.unpack(resource)

    return attrs
