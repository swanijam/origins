from .model import Model  # noqa
from .resource import Resource
from .topology import Topology
from .namespace import Namespace
from .prov import (Entity, Activity, Agent, Generation, Usage, Start, End,
                   Invalidation, Communication, Derivation, Attribution,
                   Association, Delegation, Influence, Alternate, Membership,
                   Specialization, Link, Edge)


MODELS = {
    # Model names
    'Resource': Resource,
    'Topology': Topology,
    'Namespace': Namespace,
    'Entity': Entity,
    'Edge': Edge,
    'Link': Link,
    'Activity': Activity,
    'Agent': Agent,
    'Generation': Generation,
    'Usage': Usage,
    'Start': Start,
    'End': End,
    'Invalidation': Invalidation,
    'Communication': Communication,
    'Derivation': Derivation,
    'Attribution': Attribution,
    'Association': Association,
    'Delegation': Delegation,
    'Influence': Influence,
    'Alternate': Alternate,
    'Membership': Membership,
    'Specialization': Specialization,

    # PROV concepts
    'entity': Entity,
    'agent': Agent,
    'activity': Activity,
    'wasGeneratedBy': Generation,
    'used': Usage,
    'wasInformedBy': Communication,
    'wasStartedBy': Start,
    'wasEndedBy': End,
    'wasInvalidatedBy': Invalidation,
    'wasDerivedFrom': Derivation,
    'wasAttributedTo': Attribution,
    'wasAssociatedWith': Association,
    'actedOnBehalfOf': Delegation,
    'wasInfluencedBy': Influence,
    'alternateOf': Alternate,
    'specializationOf': Specialization,
    'hadMember': Membership,
}


# Sub-types of existing models based on the prov:type attribute
TYPE_OVERRIDES = {
    ('entity', 'origins:Link'): Link,
    ('wasInfluencedBy', 'origins:Edge'): Edge,
}


def get(model, attrs=None):
    if model is None:
        return

    # Shift arguments
    if isinstance(model, dict):
        attrs = model
        name = attrs['origins:model']
    else:
        if attrs and 'origins:model' in attrs:
            name = attrs['origins:model']
        else:
            name = model

    if attrs:
        type = attrs.get('prov:type')

        if type:
            if isinstance(type, (list, tuple)):
                for _type in type:
                    if (name, _type) in TYPE_OVERRIDES:
                        return TYPE_OVERRIDES[(name, _type)]

            elif (name, type) in TYPE_OVERRIDES:
                return TYPE_OVERRIDES[(name, type)]

    return MODELS[name]


def parse(attrs, *args, **kwargs):
    if attrs is None:
        return

    model = get(attrs)

    return model.parse(attrs, *args, **kwargs)
