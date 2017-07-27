package net.fortytwo.tpop.sail;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategyFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverImpl;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailChangedEvent;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;

import java.util.function.Function;

/**
 * An RDF storage and inference layer (SAIL) for Apache TinkerPop 3
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSail extends AbstractNotifyingSail {
    private final DataStore dataStore;
    private final SailStore sailStore;

    private EvaluationStrategyFactory evalStratFactory;
    private FederatedServiceResolver serviceResolver;
    private FederatedServiceResolverImpl dependentServiceResolver;

    public GraphSail(final Graph graph, final Function<String, GraphIndex> indexFactory) {
        this(graph, indexFactory, false);
    }

    public GraphSail(final Graph graph, final Function<String, GraphIndex> indexFactory, final boolean readOnly) {
        this.dataStore = new DataStore(graph, readOnly, indexFactory, new DataStore.SailChangedHelper() {
            @Override
            public void notifyOfChanges(boolean statementsAdded, boolean statementsRemoved) {
                if (statementsAdded || statementsRemoved) {
                    SailChangedEvent event = new SailChangedEvent() {
                        @Override
                        public Sail getSail() {
                            return GraphSail.this;
                        }

                        @Override
                        public boolean statementsAdded() {
                            return statementsAdded;
                        }

                        @Override
                        public boolean statementsRemoved() {
                            return statementsRemoved;
                        }
                    };
                    notifySailChanged(event);
                }
            }
        });
        this.sailStore = new GraphSailStore(dataStore);
    }

    DataStore getDataStore() {
        return dataStore;
    }

    SailStore getSailStore() {
        return sailStore;
    }

    synchronized EvaluationStrategyFactory getEvaluationStrategyFactory() {
        if (evalStratFactory == null) {
            evalStratFactory = new StrictEvaluationStrategyFactory(getFederatedServiceResolver());
        }
        evalStratFactory.setQuerySolutionCacheThreshold(getIterationCacheSyncThreshold());
        return evalStratFactory;
    }

    private synchronized FederatedServiceResolver getFederatedServiceResolver() {
        if (serviceResolver == null) {
            if (dependentServiceResolver == null) {
                dependentServiceResolver = new FederatedServiceResolverImpl();
            }
            setFederatedServiceResolver(dependentServiceResolver);
        }
        return serviceResolver;
    }

    private synchronized void setFederatedServiceResolver(FederatedServiceResolver resolver) {
        this.serviceResolver = resolver;
        if (resolver != null && evalStratFactory instanceof FederatedServiceResolverClient) {
            ((FederatedServiceResolverClient) evalStratFactory).setFederatedServiceResolver(resolver);
        }
    }

    /**
     * Enables or disables enforcement of a unique statements policy (disabled by default),
     * which ensures that no new statement will be added which is identical
     * (in all of its subject, predicate, object and context) to an existing statement.
     * If enabled, this policy will first remove any existing statements identical to the to-be-added statement,
     * before adding the latter statement.
     * This comes at the cost of higher write latency.
     *
     * @param flag whether this policy should be enforced
     */
    public void enforceUniqueStatements(final boolean flag) {
        dataStore.setUniqueStatements(flag);
    }

    @Override
    protected void shutDownInternal() throws SailException {
        wrapForSail(sailStore::close);
    }

    @Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new GraphSailConnection(this);
    }

    @Override
    public boolean isWritable() throws SailException {
        return !dataStore.isReadOnly();
    }

    @Override
    public ValueFactory getValueFactory() {
        return dataStore.getValueFactory();
    }

    private void wrapForSail(final NoargMethod method) {
        try {
            method.call();
        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @FunctionalInterface
    private interface NoargMethod {
        void call() throws Exception;
    }
}