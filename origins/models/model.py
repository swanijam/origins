from uuid import uuid4
from origins import packer, utils


OPERATIONAL_ATTRS = {
    'origins:uuid',
    'origins:time',
    'origins:model',
    'origins:search',
}


# Add class name as property for model and instance level access
# TODO: remove
class Metaclass(type):
    def __new__(cls, name, bases, attrs):
        attrs['name'] = name
        return type.__new__(cls, name, bases, attrs)


class Model(metaclass=Metaclass):
    abstract = True

    relations = {}

    def __init__(self, attrs=None, defaults=True):
        if attrs is None:
            attrs = {}

        if defaults:
            attrs = self.defaults(attrs)

        self.attrs = attrs

    def __hash__(self):
        return hash(self.uuid)

    def __repr__(self):
        return '<{}: {} r.{}>'.format(self.__class__.name,
                                      self.label or '-',
                                      self.uuid[:8])

    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return self.uuid == other.uuid

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __copy__(self):
        return self.copy()

    @property
    def rels(self):
        from origins import models

        rels = {}

        for key, (model, required) in self.relations.items():
            if key in self.attrs:
                value = self.attrs[key]

                if not isinstance(value, Model):
                    # Use the first model if multiple are supported
                    if isinstance(model, tuple):
                        model = models.get(model[0])
                    else:
                        model = models.get(model)

                    value = model({'origins:uuid': value}, defaults=False)
            else:
                value = None

            rels[key] = value

        return rels

    @classmethod
    def parse(cls, attrs):
        if isinstance(attrs, Model):
            return attrs

        return cls(attrs)

    def defaults(self, attrs):
        attrs['origins:model'] = self.__class__.name

        if 'origins:time' not in attrs:
            attrs['origins:time'] = utils.timestamp()

        if 'origins:uuid' not in attrs:
            attrs['origins:uuid'] = str(uuid4())

        if 'origins:id' not in attrs:
            attrs['origins:id'] = attrs['origins:uuid']

        return attrs

    def copy(self):
        "Makes a copy of this instance."
        return self.__class__(self.serialize())

    def derive(self, attrs=None):
        "Derives a new instance."
        _attrs = self.serialize()

        # Remove managed attributes
        _attrs.pop('origins:uuid', None)
        _attrs.pop('origins:time', None)
        _attrs.pop('origins:search', None)

        if attrs:
            _attrs.update(attrs)

        return self.__class__(_attrs)

    def serialize(self, unpack=False):
        "Serializes the attributes into a dictionary."
        attrs = dict(self.attrs)

        if unpack:
            attrs = packer.unpack(attrs)

            for key, rel in self.rels.items():
                if rel is not None:
                    attrs[key] = rel.serialize(unpack=unpack)

        return attrs

    def diff(self, other):
        "Takes another instance and produces a diff dict."
        if isinstance(other, Model):
            if not isinstance(other, self.__class__):
                raise TypeError('cannot diff against type {}'
                                .format(other.__class__))

            other = other.serialize()

        # Ignore operational attributes and relations
        ignored = OPERATIONAL_ATTRS | set(self.relations)

        return utils.diff_attrs(self.serialize(), other, ignored=ignored)

    # Commonly accessed attributes
    @property
    def id(self):
        return self.attrs.get('origins:id')

    @property
    def uuid(self):
        return self.attrs.get('origins:uuid')

    @property
    def label(self):
        return self.attrs.get('prov:label')

    @property
    def time(self):
        return self.attrs.get('origins:time')
