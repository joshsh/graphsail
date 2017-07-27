package net.fortytwo.tpop.sail;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailDataset;

import java.util.Set;

class GraphSailDataset implements SailDataset {
    private final DataStore dataStore;

    GraphSailDataset(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void close() throws SailException {
        // TODO: graph transaction handling
    }

    @Override
    public CloseableIteration<? extends Namespace, SailException> getNamespaces() throws SailException {
        return dataStore.getNamespaces().getAll();
    }

    @Override
    public String getNamespace(final String prefix) throws SailException {
        return dataStore.getNamespaces().get(prefix);
    }

    @Override
    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {
            Set<Resource> contexts = DataStore.findContextsIn(dataStore.getAllStatements());
            return IterUtils.toCloseableIteration(contexts.iterator(), s -> s);
    }

    @Override
    public CloseableIteration<? extends Statement, SailException> getStatements(
            Resource subject, IRI predicate, Value object, Resource... contexts) throws SailException {
        return dataStore.buildIterator(subject, predicate, object, contexts);
    }
}
