package net.fortytwo.tpop.sail;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;

/**
 * A transactional connection to a GraphSail RDF store
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class GraphSailConnection extends SailSourceConnection {

    private static final Resource[] DEFAULT_CONTEXT = {null};

    private final DataStore dataStore;

    GraphSailConnection(GraphSail sail) {
        super(sail, sail.getSailStore(), sail.getEvaluationStrategyFactory());
        this.dataStore = sail.getDataStore();
    }

    @Override
    public void addStatementInternal(
            final Resource subject,
            final IRI predicate,
            final Value object,
            final Resource... contexts) throws SailException {
        Vertex subjectVertex = dataStore.getOrCreateVertexByValue(subject);
        Vertex objectVertex = dataStore.getOrCreateVertexByValue(object);
        Resource[] addContexts = 0 == contexts.length
                ? DEFAULT_CONTEXT
                : contexts;
        String label = predicate.stringValue();
        for (Resource context : addContexts) {
            String contextValue = null == context ? null : context.stringValue();
            if (dataStore.getUniqueStatements()
                    && dataStore.edgeExists(subjectVertex, objectVertex, label, contextValue)) {
                continue;
            }

            dataStore.addStatementInternal(subjectVertex, objectVertex, label, contextValue);
         }
    }

    @Override
    public void removeStatementsInternal(final Resource subject,
                                 final IRI predicate,
                                 final Value object,
                                 final Resource... contexts) throws SailException {
        dataStore.removeIteratorStatements(dataStore.buildIterator(subject, predicate, object, contexts));
    }
}
