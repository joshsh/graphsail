package net.fortytwo.tpop.sail;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.ContextStatement;

class GraphSailStatement extends ContextStatement {
    private final Edge edge;

    GraphSailStatement(
            final Edge edge, final Resource subject, final IRI predicate, final Value object, final Resource context) {
        super(subject, predicate, object, context);
        this.edge = edge;
    }

    Edge getEdge() {
        return edge;
    }
}
