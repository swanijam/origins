import logging
from origins import managers, models
from origins.exceptions import ValidationError, QueryError, StatementError
from .commands import Command


logger = logging.getLogger(__name__)


IDENTIFIERS = {
    'origins:id',
    'origins:uuid',
}


class Statement():
    """Declarative statement to be evaluated against the provenance graph.

    For instance-based statements, the `model` must be supplied with an
    optional `params` set of attributes. Additionally, an `id` may be supplied
    which is mapped to the instance UUID once evaluated.
    """
    def __init__(self, keyword, model=None, id=None, params=None,
                 instance=None):

        if params is None:
            params = {}

        if instance:
            model = instance.name

        self.keyword = keyword
        self.model = model
        self.params = params
        self.id = id
        self.instance = instance

    def __repr__(self):
        return '<Statement: {} {} ({})>' \
               .format(self.keyword, self.model, self.id or '-')

    def serialize(self):
        return {
            'keyword': self.keyword,
            'model': self.model,
            'params': self.params,
            'id': self.id,
            'instance': self.instance.serialize(),
        }


def add(stmt, tx):
    """Evaluates an "add" statement which adds an instance to the graph.

    The model instance must not exists.
    """
    model = models.get(stmt.model)
    manager = managers.get(model)
    instance = stmt.instance

    # Assume the instance has been prepared ahead of time
#    if not instance:
#        try:
#            exists = manager.exists(tx=tx, **stmt.params)
#        except QueryError:
#            exists = False
#
#        if exists:
#            raise ValidationError('already exists')
#
#        instance = model(**stmt.params)
#        stmt.instance = instance

    return [
        Command('add', instance, host=tx.host),
    ]


def update(stmt, tx):
    """Evaluates an "update" statement.

    The model instance must exist and be valid. If there is no difference
    determined, the command is skipped.
    """
    model = models.get(stmt.model)
    manager = managers.get(model)
    instance = stmt.instance

#    if instance:
#        if not set(instance.attrs) - IDENTIFIERS:
#            logger.warn('%s only contains identifiers', instance)
#            return
#    else:
#        # Validate the instance exists
#        instance = manager.get(tx=tx, **stmt.params)
#
#        if not instance:
#            raise ValidationError('does not exist')
#
#        stmt.instance = instance
#        attrs = stmt.params.get('attrs', {})
#
#        # Only identifiers, do nothing
#        if not set(attrs) - IDENTIFIERS:
#            logger.warn('%s only contains identifiers', instance)
#            return
#
#        # Diff the attributes to save a write operation
#        diff = instance.diff(attrs)
#
#        if not diff:
#            logger.debug('%s did not change', instance)
#            return
#
#        # Derive from existing instance and determine diff them to see if
#        instance.attrs.update(attrs)

    return [
        Command('update', instance, host=tx.host),
    ]


def remove(stmt, tx):
    model = models.get(stmt.model)
    manager = managers.get(model)
    instance = stmt.instance

#    if not instance:
#        instance = manager.get(tx=tx, **stmt.params)
#
#        if not instance:
#            raise ValidationError('does not exist')
#
#        stmt.instance = instance
#
    return [
        Command('remove', instance, host=tx.host)
    ]


def merge(stmt, tx):
    "Add or update an instance."
    if stmt.instance:
        raise StatementError('merge do not support bound instances')

    try:
        return add(stmt, tx=tx)
    except ValidationError:
        return update(stmt, tx=tx)


def noop(stmt, tx):
    if not stmt.instance:
        model = models.get(stmt.model)
        stmt.instance = model(**stmt.params)


methods = {
    'add': add,
    'update': update,
    'remove': remove,
    'merge': merge,
    'noop': noop,
}
