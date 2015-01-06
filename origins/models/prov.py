from .model import Model
from .resource import Resource


class ResourceModel(Model):
    def __init__(self, *args, **kwargs):
        self.resource = kwargs.pop('resource', None)
        super(ResourceModel, self).__init__(*args, **kwargs)

    def derive(self, attrs=None):
        instance = super(ResourceModel, self).derive(attrs)
        instance.resource = self.resource

        return instance

    def serialize(self, unpack=False):
        attrs = super(ResourceModel, self).serialize(unpack)

        if unpack:
            if self.resource:
                attrs['resource'] = self.resource.serialize(unpack)

        return attrs

    @classmethod
    def parse(cls, attrs, resource=None):
        if resource:
            resource = Resource.parse(resource)

        return cls(attrs, resource=resource)


class Object(ResourceModel):
    pass


class Entity(Object):
    pass


class Link(Object):
    relations = {
        'origins:start': ('Entity', False),
        'origins:end': ('Entity', False),
    }

    def __init__(self, *args, **kwargs):
        self.topology = kwargs.pop('topology', None)
        super(Link, self).__init__(*args, **kwargs)

    def derive(self, attrs=None):
        instance = super(Link, self).derive(attrs)
        instance.topology = self.topology

        return instance


class Agent(Object):
    pass


class Activity(Object):
    pass


# Influences can link entities, activities, or agents
class Influence(ResourceModel):
    relations = {
        'prov:influencer': ('Object', True),
        'prov:influencee': ('Object', True),
    }


# Custom to Origins, only ties two entities together
class Edge(Influence):
    relations = {
        'prov:influencer': ('Entity', True),
        'prov:influencee': ('Entity', True),
    }


class Generation(ResourceModel):
    relations = {
        'prov:entity': ('Entity', True),
        'prov:activity': ('Activity', False),
    }


class Usage(ResourceModel):
    relations = {
        'prov:entity': ('Entity', True),
        'prov:activity': ('Activity', False),
    }


class Start(ResourceModel):
    relations = {
        'prov:activity': ('Activity', True),
        'prov:trigger': ('Entity', False),
        'prov:starter': ('Activity', False),
    }


class End(ResourceModel):
    relations = {
        'prov:activity': ('Activity', True),
        'prov:trigger': ('Entity', False),
        'prov:ender': ('Activity', False),
    }


class Invalidation(ResourceModel):
    relations = {
        'prov:entity': ('Entity', True),
        'prov:activity': ('Activity', False),
    }


class Communication(ResourceModel):
    relations = {
        'prov:informant': ('Activity', True),
        'prov:informed': ('Activity', True),
    }


class Derivation(ResourceModel):
    relations = {
        'prov:generatedEntity': ('Entity', True),
        'prov:usedEntity': ('Entity', True),
        'prov:activity': ('Activity', False),
        'prov:generation': ('Generation', False),
        'prov:usage': ('Usage', False),
    }


class Attribution(ResourceModel):
    relations = {
        'prov:entity': ('Entity', True),
        'prov:agent': ('Agent', True),
    }


class Association(ResourceModel):
    relations = {
        'prov:activity': ('Activity', True),
        'prov:agent': ('Agent', True),
        'prov:plan': ('Entity', False),
    }


class Delegation(ResourceModel):
    relations = {
        'prov:delegate': ('Agent', True),
        'prov:responsible': ('Agent', True),
        'prov:activity': ('Activity', False),
    }


class Specialization(ResourceModel):
    relations = {
        'prov:generalEntity': ('Entity', True),
        'prov:specificEntity': ('Entity', True),
    }


class Alternate(ResourceModel):
    relations = {
        'prov:alternate1': ('Entity', True),
        'prov:alternate2': ('Entity', True),
    }


class Membership(ResourceModel):
    relations = {
        'prov:collection': ('Entity', True),
        'prov:entity': ('Entity', True),
    }
