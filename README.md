# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

Origins is a service that enables:

1. Identifying, importing, and managing all the "things"
2. Organizing and creating links between these "things"
3. Getting notified when anything changes

## How are changes tracked?

Origins is built on the assumption that things change all the time. A "thing" in Origins must declare one or more properties that it may have values for. Properties may represent the data itself or metadata about the thing being represented (more common for imported sources). If that thing is updated (through a subsequent import or via the API), the old and new properties are compared and if there is a difference, a new revision of the thing is created with the new properties. The previous revision is *invalidated* since its properties are no longer representative of the current state of the thing.

## How is data provenance used?

Data provenance is a description of how a thing came to be. This includes concepts such as *who* was responsible, *what* was involved, and *how* did it happen. Equally important is *when* these influences occurred. For example, an article may be written by Joe, edited by Sue, then published by Jane. At each stage the document has one more annotations that can be made on it:

- Joe created the article through the act of writing
- In writing the article he likely used some information sources such as websites, books, articles, etc.
- Sue's *edit* resulted in a new revision of Joe's article
- Like Joe, Sue may have used a few resources to validate Joe's content
- Jane published the article which may have involved adding it to a public website

Origins supports importing externally generated provenance (for the systems that support it), but also captures provenance on the things changed directly in the system. Collectively, these annotations make up the history and lineage of a thing.

### Provenance Descriptions

- Generation
    - time - when did it occur
    - entity - what thing was generated
    - activity - what caused the entity to be generated
    - association - who was responsible for generating it
    - derivation - what was the entity derived from
    - usage (via derivation) - what was used in deriving the entity
- Invalidation
    - time - when did it occur
    - entity - what thing was invalidated
    - activity - what caused the entity to be invalidated
    - associated - who was responsible for invalidating it

## Documentation

Please refer to the Origins wiki for getting started and deployment documentation: https://github.com/cbmi/origins/wiki/

## Development

Requirements:

- Python 3.2+
- Redis 2.8+
- Neo4j 2.1+

## Types

- There are two type classes: **occurrent** and **continuant**
- An occurrent is "actually occurring or observable, not potential or hypothetical."
- A continuant is "a thing that retains its identity even though its state and relations may change."
    - The *identity* is evaluated using the `origins:id` attribute and stored using the `origins:next` edge
- A continuant is composed of a series of one or more occurrents
- All changes to a continuant result in a new occurrent with one or more provenance descriptions
    - This distinction is made by the presence of the `origins:id` attribute
- Individual occurrents may be updated in-place

### Models

- All models are based on a type class
- Occurrent-based models:
    - `namespace`
    - PROV concepts: `entity`, `generation`, `bundle`, etc.
- Continuant-based models:
    - `node`
    - `edge`
    - `resource`
    - `component`
    - `relationship`

### Attributes

- At a minimum an instance is guaranteed to have three attributes:
    - `origins:model` - The [model](#models) of the instance
    - `origins:uuid` - An unique identifier of the instance
    - `origins:time` - Timestamp when the instance was evaluated
- Continuants also have a `origins:id` which is auto-generated if not supplied
- Origins defines a set of *common attributes* which provides a consistent way for consumers to identify and describe instances:
    - `origins:label` - The label of the instance
    - `origins:description` - A description of the instance
    - `origins:type` - The client-defined type of the instance
- Edge-based models require two additional attributes for representing a directed edge:
    - `origins:start` - The instance the edge is coming from
    - `origins:end` - The instance the edge is going to
- Additional attributes that the instance has are generally referred to it's `properties` to distinguish them from the Origins specific attributes. Consequently, property keys may not contain the `origins:` prefix.

## Commands

- A command is an abstraction on the primitive operations such as create, update, delete
- Commands are first evaluated and prepared against the provenance graph
    - Evaluation may result in an error, a "no op" (skipped command), or one or more new commands
    - The result is a new set of commands
- The commands in the transaction are then executed against a [service](#services)
    - A command will invoke one or more operations
- If the transaction is successful, the commands are appended to the command log
- A transaction is a series of commands that must all successfully execute in order for the commands to be appended to the log
- The command log is used by secondary services or slaves to update their state to match that of the host
    - For example, write-based commands will be used to update documents in the Elasticsearch instance 

### Ideas 

- Command should include enough information to undo a change
    - add - remove the node
    - remove - add the node
    - update - update with previous attributes

## Services

- Provenance graph - The primary store of the data. Commands are evaluated against this graph.
- Literal graph - A representation of the *visible* state of the provenance graph. This makes it possible to query nodes and edges natively.
- Search index - A full-text search index of the nodes and edges. Identifiers and the relevant text are returned.
- Events system - Commands correspond to one or more events that are triggered by the events system. Clients can be subscribe to these events to receive them when they are triggered.

### Execution

- Evaluated [commands](#commands) are processed, executed, and committed to provenance graph first. If this step fails, the commands are not written to the command log.
- Secondary services process commands read from the command log to update their state and reflect the changes made to the provenance graph.
    - 

### Statements

- High-level declarative statements that get evaluated against the provenance graph
- Evaluated statements are translated into backend-specific commands that get executed
    - Provenance graph
    - Literal graph
    - Search index
    - Events/webhooks

#### Add

- Adds an instance to the graph
- Requires a `model` and optional `attrs` which represent the attributes of the instance.
- Validates the instance does not exist if `origins:id` or `origins:uuid` is contained in `attrs`
- Prepares the instance

#### Update

- Updates an instance in the graph
- Requires `model` and `attrs` to be defined with an `origins:id` or `origins:uuid`
- Validates the instance exists in the graph and is valid
- Diffs the `attrs` against the current instance
- Prepares a new instance
- Finds all dependent edges and prepares statements to alter them

#### Remove

- Removes and instance in the graph
- Requires `model` and `attrs` to be defined with an `origins:id` or `origins:uuid`
- Validates the instance exists in the graph and is valid
- Finds all dependent nodes and edges and prepares statements to remove them

### Dependence

- Dependency path - the set of nodes and edges affected by a node change
    - An update may require re-evaluation of the affected nodes
        - Triggers a dependency event
        - Direction
            - Binary, the update affects the node or it doesn't
            - Create separate edge for mutual behavior?
    - A remove may require re-evaluation of affected nodes
        - Hard dependencies will be invalidated
            - `origins:dependent = true`
        - Everything else is a soft dependency
            - Dependency event triggered on node
- Dependency event
    - Includes all path of affected nodes given some starter node

- Examples
    - schema <= SQL => inclusion criteria
        - SQL statement depends on the schema and inclusion criteria
        - either change, the SQL statement has to be evaluated

### Models

- how does PROV play a role in Origins?
    - specification used 

### Purpose

- In Origins, first-class objects have an ID
    - An object is either a node or edge
    - Objects are versioned
        - UUIDs are used to differentiate revisions
        - Changes in the attributes result in a new version
    - A node revision will result in revisions (copies) for all outbound edges
        - A  -  B to A' -' B
        - This ensures the state *declared* for or by the node is maintained
            - If Joe likes kale and changes his hair color from brown to black, it does not imply he no longer likes kale
    - Edges connect two nodes and defines a relationship between them
        - Edges are directed
            - The end node must have been declared *before* the start node
            - It implies independence of the end node from the start node
        - It should also be assumed the end node knows nothing about the start node
            - There is no confirmation of the end node that the start node can form an edge to it
        - An edge implies a *subscription* to events on the end node by the start node
            - If the end node changes (update or remove), the start node will be notified
        - When an end node changes, the incoming edges to the previous revision are not copied
            - This results in "broken edge" between the latest start node and the (now) previous end node
            - A broken edge is defined by an edge that is *valid* but points to a broken end node
            - An invalid edge *could* imply the relationship was explicitly removed
            - A broken relationship can be fixed by updating the edge 
- PROV is a set of relation annotations to "objects of interest" for representing "past" state
    - PROV is an implementation detail, but not the implementation

### Why?

- See who was involved and the influence in producing some data and when those events occurred
- Browse


### Features

"What data was combined to produce the result."

- Standard CRUD operations translate to provenance concepts
- Versioning of nodes and edges
    - Previous revisions are no longer valid
- High-level data model for common modeling of resources, components, and relationships
    - Components and relationships are nodes and edges that are "managed" by a resource
- Declaration of lineage/descent
    - Which nodes/edges were predecessors
    - The "node" likely could not exist without the predecessors
- Support for PROV concepts
    - Nodes and edges can be augmented with PROV data
- Mapping of PROV to Origins
    - prov:Derivation(prov:Revision) => Origins revision
    - prov:Derivation(origins:
- Client definitions with access control layer (ACL) support

### Generators

- Interfaces to common systems for extracting components such as database systems, structured files, and web services

### Technical Features

- Transactions
    - Provenance is not partially applied to ensure consistency of the data being annotated
- Command logging
    - Write operations are evaluated and written to the log prior to executing the command
    - If a transaction fails the log will be read on startup to execute the queued commands

### Statements

Statements consist of a keyword and zero or more parameters.

#### `add` | `update` | `remove`

**Parameters**

General.

- required: `model`
- optional: `id`, `label`, `description`, `type`, `properties`

Component.

- required: `resource`

Relationship.

- required: `resource`, `start`, `end`
- optional: `dependence`

### Questions

*From: https://cs.brown.edu/research/pubs/theses/masters/2010/islam.pdf*

1. Efficiency of storing and retriving provenance
    - Recursive queries are not acceptable
2. Track changes to the systems that contain, process, or produce the data
3. Systems that produce the same output may not result in the same provenance
    - Can/should this be reconciled?
4. Provenance-aware security model
5. User interfaces for performing actions should produce provenance
6. Organizations do not see the value in capturing provenance
    - Time consuming and little short term benefit
7. Provenance should be based on a systems schema and/or constraints
8. Provenance for heterogeneous data and managing the differences
9. Making use of the provenance data itself with summaries and visualizations
10. More general way to integrate provenance techniques across systems
    - Cannot embed in all systems

### How does Origins solve these issues

1. Provenance data is highly connected and fits well in a graph model. Origins uses a native graph database and algorithms to store and retrieve this data. Edges are traversed by *hopping* across native linked lists rather than recursively joining at runtime.
2. A node in Origins is the *thing* provenance is being captured for. However it can also be part of the provenance of other nodes. Changes to a node results in an independent node with a separate set of provenance. This ensures provenance is specific to the state of the node at each revision. However, an edge is formed between revisions so provenance data can be queried over time and aggregated for display or analysis.
3. Origins is designed for storing and retrieving provenance data. Reconciling redundant provenance would require changes in the provenance capture/extraction phase. Origins may build in detection of redundant provenance data or entity reconciliation, but this is secondary.
4. At this time, Origins does not have support for users or permissions. However due to the graph model, adding an access control layer (ACL) would be straightforward.
5. Origins is a service for storing, retrieving, and managing *representations* of external data and the provenance associated with it. The representation should store enough metadata to convey what has changed either as values or links to stable rsources.
6. Origins has powerful API endpoints for browsing and searching components across resources. The Web client is the user interface for reporting on and visualizing the provenance data.
7. Origins does not assume or infer how or where provenance was generated, but only stores it and makes it accessible.
8. Origins defines a high-level named container for encapsulating provenance data called a *resource*. The provenance contained in a resource could represent anything such as a database system, queries, specimens, etc. Resoures can be provided a name, type, and description to differentiate the contents at a high level. Furthermore Origins supports creating *collections* which resources can be apart of. Addressing Q4, ACL rules can be applied to the resource.
9. See A6.
10. As mentioned before, Origins is a service that enables provenance data to be stored and retrieved. It is complementary to existing systems that generate PROV data.

#### Components: Entities and agents

Entities and agents are extracted as the "things of interest", they are generically referred to as *components*.

#### Relationship Entity Type

Origins defines an entity [prov:type](http://www.w3.org/TR/2013/REC-prov-dm-20130430/#term-attribute-type) for modeling relationships between entities, `origins:Relationship`. This entity requires the `origins:start` and `origins:end` attributes to be defined which must be two entities that are not relationships themselves.

#### Persisted Identifiers

Provenance reconcilation is one of the harder and time consuming problems when working with provenance data. Existing nodes can be referenced in the graph by supplying the `origins:id`. For example, given these two PROV-N snippets:

A blood draw produced a specimen.

```
activity(bd, [prov:type="blood draw"])
entity(sp, [prov:type="blood specimen"])
wasGeneratedBy(sp, bd)
```

The specimen used in an analysis that produced some result.

```
entity(sp)
activity(an, [prov:type="analysis"])
used(u; an, sp)
entity(re, [prov:type="result"])
wasGeneratedBy(g; re, an)
wasDerivedFrom(re, sp, an, g, u, [prov:type="PrimarySource"])
```

The `sp` entity definitions in the two snippets are not linked in any way. There is also not enough unique content (attributes) defined on the entities that would provide an confidence for attempting to reconcile them.

The simple solution is supply a persistent identifier that can be used to reference the node across documents (loads). The above snippets would now look like this:

```
activity(bd, [prov:type="blood draw"])
entity(sp, [prov:type="blood specimen", origins:id="bs1"])
wasGeneratedBy(sp, bd)
```
and

```
entity(sp, [origins:id="bs1"])
activity(an, [prov:type="analysis"])
used(u; an, sp)
entity(re, [prov:type="result"])
wasGeneratedBy(g; re, an)
wasDerivedFrom(re, sp, an, g, u, [prov:type="PrimarySource"])
```

The `bs1` identifier prevents storing the `sp` entity in the second snippet as a separate node in the graph. Instead it reuse the node created in the first snippet.

Another special behavior of the `origins:id` is that it can be used to identify revision sets. It can be shared across entities that are revisions of one another. The *latest* revision will be returned on subsequent lookups. For example:

An article was written.

```
activity(a, [prov:type="write"])
entity(r, [origins:id="article", ex:title="PROV 101"])
wasGeneratedBy(r, a)
```

Article was edited and produced a new version.

```
entity(r0, [origins:id="article"])
entity(r1, [origins:id="article", ex:title="Intro to PROV"])
activity(a, [prov:type="edit"])
wasGeneratedBy(g; r1, a)
wasDerivedFrom(r1, r0, a, g, [prov:type="prov:Revision"])
```

`r0` will match due to the behavior described above, however `r1` now has the same identifier. If a second edit was made to the article:

```
entity(r0, [origins:id="article"])
entity(r1, [origins:id="article", ex:title="PROV in Practice"])
activity(a, [prov:type="edit"])
wasGeneratedBy(g; r1, a)
wasDerivedFrom(r1, r0, a, g, [prov:type="prov:Revision"])
```

`r0` will match the second version ("Intro to PROV") and `r1` will be derived from that. This behavior supports the common case of a linear revision history. It does this by determining which entity is being used in the derivation and then using that as the node to link to. Subsequent derivations can be included in the same document.

##### UUIDs

Of course it becomes ambiguious when wanting to create different edits from the same revision. This is why Origins also generates a UUID for every node produced in the graph. This means an explicit node can be referenced by specifying the `origins:uuid` instead of `origins:id`. In the last example, if we wanted to treat the *PROV in Practice* as a separate edit to *PROV 101* (first version), we could specify it's UUID.

```
entity(r0, [origins:uuid="abc123-..."])
entity(r1, [origins:id="article", ex:title="PROV in Practice"])
activity(a, [prov:type="edit"])
wasGeneratedBy(g; r1, a)
wasDerivedFrom(r1, r0, a, g, [prov:type="prov:Revision"])
```

#### Updates

Since the entities in the above example are being referenced and are not new, the only attribute required is the ID or UUID. However to further simplify reconciliation of redundant entities, referenced nodes that contain attributes other than `origins:id` or `origins:uuid` will be merged into the existing attributes in the graph. For the previous example defined this entity.

```
entity(r, [origins:id="article", ex:title="PROV in Practice"])
```

If new information is available about the entity (more specifically that particular revision), the entity can be declared again with more attributes:

```
entity([origins:id="article", ex:author="Byron", ex:date="2014-11-08"])
```

And the attributes will be merged into the existing attributes.


#### Influences

In the information age, we are overloaded with notifications from apps and services, however we have equally come to expect them for most things. Notifications are sent when some *event* occurs. For most applications these events are well-defined such as a "friend request" or "comment left on your post". Since Origins deals with heterogenous data the events and the interactions between the data need to be declared.

PROV defines an *influence* concept which serves as a generic relation between two objects. This can be used when one of the [pre-defined influences](http://www.w3.org/TR/2013/REC-prov-dm-20130430/#mapping-relations-to-influence-table) are not appropriate. Origins extends the utility of this relation for declaring events.

```
entity(e1)
entity(e2)
wasInfluencedBy(e1, e2, [origins:=true])
```


#### Edges

Often, resources overlap in one or more ways. For example, a new project may involve a series of meetings which result in governance and specification documents. These documents are then implemented by some process whether it be carried out by human, machine, or both. This process may involve multiple stages and produces various outputs. The implicit links between these various resources are commonly overlooked as significant and are not kept track of.

An edge (or link, reference, relationship) connects two (non-edge) entities or agents. It is a sub-type of entity and therefore it's provenance can be tracked as well. The PROV-compatible way is:

```
entity(e1)
entity(e2)
entity([prov:type=origins:Edge, origins:start=e1, origins:end=e2])
```

Alternately, it can be declared with a PROV-N extension:

```
origins:edge(e1, e2)
```

A link implicitly declares

### Inferring provenance

Most systems have limited support for capturing and recording provenance as events take place in the system. For example, a data dictionary flat file does not generally have any metadata about the changes that are made across versions. The entities are extracted from the file and need to be compared against the existing entities in order to infer what happened between versions.

There are three provenance concepts that can be detected from a simple diff:

- Generation - when the local entity is not present in the graph
- Derivation - when the local entity differs from the entity in the graph
- Invalidation - when the remote entity is present, but not locally

Generation and Invalidation are antonyms, but share the same attributes:

- `entity` - The entity the event applies to
- `activity` - The activity which caused the event to occur
- `time` - The time the event occurred

A Derivation events supports the following attributes:

- `usedEntity` - The previous revision of the entity (remote)
- `generatedEntity` - The new revision of the entity (local)
- `activity` - The activity which caused the derivation
- `generation` - The generation event of the new revision
- `usage` - The usage event between the activity and the previous revision

Since all of the provenance can be produced based on the evaluation of the change in state, the only two attributes that need to be provided by the system are the `time` and `activity`.

```
{
    "keyword": "merge",
    "model": "entity",
    "attribution": { ... },
    "params": {
        "time": "....",
        "activity": { ... },
        "attrs": { ... }
    },
}
```

### Persistent PROV

The PROV specification defines document-based formats including [PROV-XML](http://www.w3.org/TR/2013/NOTE-prov-xml-20130430/), [PROV-O](http://www.w3.org/TR/2013/REC-prov-o-20130430/) (OWL2), [PROV-JSON](http://www.w3.org/Submission/2013/SUBM-prov-json-20130424/), and a human-readable notation called [PROV-N](http://www.w3.org/TR/2013/REC-prov-n-20130430/). An implicit constraint with document-based formats is that identifiers cannot be assumed to be unique across documents.

Through the use of `origins:id` and `origins:uuid`, Origins can automatically reconcile provenance.


### Refactors

- Structures for each type
    - UUID, ID, and timestamp auto-generated
- Queries for each type
