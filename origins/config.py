import os
import sys
import json
import logging
import logging.config
from copy import deepcopy

ENV_PREFIX = 'ORIGINS'

ENV_ELIGIBLE = (
    'debug',
    'host',
    'port',
    'generator-service',
    'dispatch-service',
    'neo4j',
    'redis',
)

# Default configuration options
default_options = {
    'debug': False,

    'host': 'localhost',
    'port': 5000,

    'generator-service': {
        'host': 'localhost',
        'port': 5001,
    },

    'dispatch-service': {
        'host': 'localhost',
        'port': 5002,
    },

    'neo4j': {
        'host': 'localhost',
        'port': 7474,
    },

    'redis': {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
    },

    'logging': {
        'version': 1,
        'disable_existing_loggers': True,
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
            },
        },
        'loggers': {
            'origins': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': True,
            },
            'origins.log.processor': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': True,
            },
            'origins.events': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': True,
            },
            'origins.graph.neo4j': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': True,
            },
        },
    },
}


def _defaults(a, b):
    "Recursively apply default values from b into a."
    o = {}

    for ak, av in a.items():
        if isinstance(av, dict):
            o[ak] = _defaults(av, b[ak])
        else:
            o[ak] = deepcopy(av)

    # Fill in the remaining defaults
    for bk, bv in b.items():
        # Already handled
        if bk not in o:
            o[bk] = deepcopy(bv)

    return o


def load_options(path):
    "Loads configuration options from path."
    try:
        with open(path, 'rU') as f:
            return set_options(json.load(f))
    except (ValueError, IOError):
        sys.stderr.write('Error loading configuration file {}\n'.format(path))
        raise


def set_options(opts=None):
    global options

    options = make_options(opts)

    setup_logging(options)
    return options


def make_options(options=None):
    if not options:
        options = {}

    options = _defaults(options, default_options)
    setup_logging(options)

    return options


def setup_logging(options):
    if 'logging' in options:
        logging.config.dictConfig(options['logging'])
    else:
        logging.basicConfig(level=logging.DEBUG)


def set_loglevel(level):
    if 'handlers' not in options['logging']:
        return

    for opts in options['logging']['handlers'].values():
        opts['level'] = level

    setup_logging(options)


def get_environ(keys=None, config=None, options=None, path=None):
    "Extracts configuration options from the environment."
    if keys is None:
        keys = ENV_ELIGIBLE

    if config is None:
        config = default_options

    if options is None:
        options = {}

    if path is None:
        path = []

    for key in keys:
        if key not in config:
            continue

        value = config[key]

        # Recurse
        if isinstance(value, dict):
            _options = options[key] = {}

            get_environ(value.keys(),
                        config=value,
                        options=_options,
                        path=path + [key])
        else:
            env_key = '_'.join(path + [key])
            env_key = ENV_PREFIX + '_' + env_key.upper().replace('-', '_')

            if env_key not in os.environ:
                continue

            env_value = os.environ[env_key]

            # Get type of option to attempt to coerce env value
            _type = type(value)

            if _type is bool:
                if env_value.lower() in ('true', '1'):
                    env_value = True
                elif env_value.lower() in ('false', '0'):
                    env_value = False
            elif _type is int:
                try:
                    env_value = int(env_value)
                except ValueError:
                    env_value = None

            if env_value is not None:
                options[key] = env_value

    return options


# Load config options from environment
if os.environ.get('ORIGINS_CONFIG'):
    options = load_options(os.environ['ORIGINS_CONFIG'])
else:
    options = make_options()

# Load individual configuration options from the environment
set_options(get_environ())
