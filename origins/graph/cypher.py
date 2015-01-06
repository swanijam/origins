import re
from string import Template as T


def map(keys, parameter):
    "Produce a valid Cypher parametized map given keys and a parameter name."
    if not keys:
        return '{}'

    toks = []

    for k in keys:
        toks.append('`{k}`: {{ {p} }}.`{k}`'.format(k=k, p=parameter))

    return '{' + ', '.join(toks) + '}'


def search(predicate, variable, parameter, operator='OR'):
    "Returns a parametized set of regexp-based expressions."
    toks = []

    for k, v in predicate.items():
        if isinstance(v, str):
            o = '=~'
        else:
            o = '='

        toks.append('`{v}`.`{k}` {o} {{ {p} }}.`{k}`'
                    .format(o=o, k=k, v=variable, p=parameter))

    # Add spaces around operator
    operator = ' ' + operator + ' '

    return operator.join(toks)


def labels(labels):
    "Converts and array of labels into the Cypher labels format for a node."
    if not labels:
        return ''

    if isinstance(labels, str):
        labels = [labels]

    return ':' + ':'.join(['`{}`'.format(l) for l in labels])


# Duplicate whitespace
ns = re.compile(r'[\n\s]+')

# Comments
cm = re.compile(r'//.*$', re.M)


def _(s):
    return ns.sub(' ', cm.sub('', s).strip())


def prepare(statement, model=None, start_model=None, end_model=None,
            **mapping):
    "Prepares a Cypher statement."
    statement = T(statement)

    mapping['model'] = labels(model)
    mapping['start_model'] = labels(start_model)
    mapping['end_model'] = labels(end_model)

    return _(statement.safe_substitute(mapping))
