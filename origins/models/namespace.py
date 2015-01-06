from origins.exceptions import ValidationError


class Namespace():
    def __init__(self, attrs):
        if 'uri' not in attrs:
            raise ValidationError('namespace uri required')

        if 'prefix' not in attrs:
            raise ValidationError('namespace prefix required')

        self.attrs = attrs

    def __hash__(self):
        return hash(self.uri)

    @classmethod
    def parse(cls, attrs):
        return cls(attrs)

    def serialize(self):
        return dict(self.attrs)

    # Commonly accessed attributes
    @property
    def uri(self):
        return self.attrs['uri']

    def prefix(self):
        return self.attrs['prefix']
