from origins import models


class Command():
    def __init__(self, command, instance, host=None):
        self.command = command
        self.instance = instance
        self.model = instance.__class__
        self.host = host

    def __repr__(self):
        name = self.model.__name__
        return '<Command: {} {}>'.format(self.command, name)

    def serialize(self):
        attrs = {
            'command': self.command,
            'model': self.model.__name__,
            'attrs': self.instance.serialize(),
            'host': self.host,
        }

        return attrs

    @classmethod
    def parse(cls, attrs):
        model = attrs['model']
        instance = models.get(model)(attrs['attrs'])

        return cls(attrs['command'],
                   instance=instance,
                   host=attrs['host'])
