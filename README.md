# GraphSail

RDF storage and inference layer (SAIL) for Apache TinkerPop 3

## Namespaces

Namespace definitions (prefix - IRI pairs) are a convenience provided in GraphSail for compatibility with RDF4j.
However, they are not stored "in the graph" like RDF statements (which are represented as vertices and edges).
Whereas Blueprints GraphSail attached namespaces to a special reference vertex, GraphSail v3 stores them in-memory.
Read-write access to namespaces is very fast, but the namespaces persist only for the liftime of a session.
In addition, changes to namespaces are not bound to TinkerPop transactions, so they are not rolled back if a write operation fails.
