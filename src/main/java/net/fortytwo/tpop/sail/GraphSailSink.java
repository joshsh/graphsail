package net.fortytwo.tpop.sail;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailSink;

import java.util.Set;

class GraphSailSink implements SailSink {
    private final DataStore dataStore;

    GraphSailSink(final DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void prepare() throws SailException {
        // TODO: graph transaction handling
    }

    @Override
    public void flush() throws SailException {
        dataStore.getSailChangedHelper().flush();
        // TODO: graph transaction handling
    }

    @Override
    public void setNamespace(final String prefix, final String name) throws SailException {
        dataStore.getNamespaces().set(prefix, name);
    }

    @Override
    public void removeNamespace(final String prefix) throws SailException {
        dataStore.getNamespaces().remove(prefix);
    }

    @Override
    public void clearNamespaces() throws SailException {
        dataStore.getNamespaces().clear();
    }

    @Override
    public void clear(Resource... contexts) throws SailException {
        Set<Resource> contextSet = contexts.length == 0 ? null : dataStore.createNullSafeSetOfContexts(contexts);

        CloseableIteration<? extends Statement, SailException> edges = dataStore.getAllStatements();
        if (0 < contexts.length) {
            edges = IterUtils.filter(edges,
                    st -> contextSet.contains(st.getContext()));
        }

        dataStore.removeIteratorStatements(edges);
    }

    @Override
    public void observe(Resource subject, IRI predicate, Value object, Resource... contexts) throws SailException {
        // TODO: no-op?
    }

    @Override
    public void approve(Resource subject, IRI predicate, Value object, Resource context) throws SailException {
        // TODO: no-op?
    }

    @Override
    public void deprecate(Resource subject, IRI predicate, Value object, Resource context) throws SailException {
        // TODO: no-op?
    }

    @Override
    public void close() throws SailException {
        // TODO: graph transaction handling
    }
}
