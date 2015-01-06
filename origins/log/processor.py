import logging
from origins.exceptions import ValidationError
from origins.graph import neo4j
from origins import models, managers
from . import statements

logger = logging.getLogger(__file__)


INSTANCE_STATEMENTS = {'add', 'merge', 'update', 'remove'}


def _resolve_references(stmt, refs):
    """Validates relation-based attributes are present and maps
    local statement IDs to the reference's UUID.

    If a local reference is not found, the relation value is
    assumed to be a remote identifier (i.e. UUID).
    """
    model = models.get(stmt.model)

    if stmt.instance:
        attrs = stmt.instance.attrs
    else:
        stmt.params.setdefault('attrs', {})
        attrs = stmt.params['attrs']

    for attr, (ref_model, required) in model.relations.items():
        if attr not in attrs:
            continue

        local = attrs[attr]

        # Locally referenced, map local instance
        if local in refs:
            ref = refs[local]
            attrs[attr] = ref

            logger.debug('resolved %s#%s %r -> %r',
                         model.name, attr, local, ref)
        else:
            logger.debug('unresolved %s#%s %r',
                         model.name, attr, local)


def _process_statement(stmt, refs, tx):
    if stmt.keyword in INSTANCE_STATEMENTS:
        # Replace reference IDs with instance UUIDs
        _resolve_references(stmt, refs)

    # Get the statement method
    method = statements.methods[stmt.keyword]
    commands = method(stmt, tx=tx)

    # Map statement ID to the corresponding instance
    if stmt.id is not None:
        refs[stmt.id] = stmt.instance

    if commands:
        for cmd in commands:
            yield cmd


def _execute(cmd, tx):
    "Executes a command."
    manager = managers.get(cmd.model)
    method = getattr(manager, cmd.command)

    logger.debug('executing %r', cmd)
    method(cmd.instance, tx=tx, defer=True)

    # Annotate command with transaction host
    cmd.host = tx.host

    return cmd


def _execute_many(cmds, tx):
    with tx as tx:
        for cmd in cmds:
            _execute(cmd, tx=tx)

    return cmds


def process(stmts, execute=True, tx=neo4j.tx):
    """Validates and evaluates a series of statements.

    The output is a series of statements that are ready to be executed by
    the provenance graph.
    """
    # Set of stmts IDs to check for duplicates
    ids = set()

    # Map of stmt IDs to their remote or generated UUIDs
    refs = {}

    commands = []

    with tx as tx:
        for stmt in stmts:
            # Duplicate IDs are not allowed
            if stmt.id is not None:
                if stmt.id in ids:
                    raise ValidationError('duplicate statement id: {}'
                                          .format(stmt.id))

                ids.add(stmt.id)

            for cmd in _process_statement(stmt, refs, tx=tx):
                commands.append(cmd)

        if execute:
            _execute_many(commands, tx=tx)

    return commands
