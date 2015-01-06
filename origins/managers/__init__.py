from origins import models
from .resource import ResourceManager, ResourceManagedManager
from .topology import TopologyManager
from .edge import EdgeManager
from .link import LinkManager
from .entity import EntityManager
from .namespace import NamespaceManager
from .derivation import DerivationManager

DEFAULT_MANAGER = ResourceManagedManager

MODEL_MANAGERS = {
    models.Entity: EntityManager,
    models.Link: LinkManager,
    models.Edge: EdgeManager,
    models.Namespace: NamespaceManager,
    models.Resource: ResourceManager,
    models.Topology: TopologyManager,
    models.Derivation: DerivationManager,
}

_cache = {}


def get(model):
    "Returns a manager instance for the model."
    if isinstance(model, str):
        model = models.get(model)

    if model not in _cache:
        if model in MODEL_MANAGERS:
            manager = MODEL_MANAGERS[model](model)
        else:
            manager = DEFAULT_MANAGER(model)

        _cache[model] = manager

    return _cache[model]
