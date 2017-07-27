package net.fortytwo.tpop.sail;

import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.BackingSailSource;
import org.eclipse.rdf4j.sail.base.SailDataset;
import org.eclipse.rdf4j.sail.base.SailSink;
import org.eclipse.rdf4j.sail.base.SailSource;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GraphSailStore implements SailStore {
    private final Logger logger = LoggerFactory.getLogger(GraphSailStore.class);

    private final DataStore dataStore;
    private final SailSource sailSource;

    GraphSailStore(DataStore dataStore) {
        this.dataStore = dataStore;
        sailSource = new BackingSailSource() {
            @Override
            public SailSink sink(IsolationLevel isolationLevel) throws SailException {
                return new GraphSailSink(dataStore);
            }

            @Override
            public SailDataset dataset(IsolationLevel isolationLevel) throws SailException {
                return new GraphSailDataset(dataStore);
            }
        };
    }

    @Override
    public ValueFactory getValueFactory() {
        return null;
    }

    @Override
    public EvaluationStatistics getEvaluationStatistics() {
        // TODO
        return new EvaluationStatistics();
    }

    @Override
    public SailSource getExplicitSailSource() {
        return sailSource;
    }

    @Override
    public SailSource getInferredSailSource() {
        return sailSource;
    }

    @Override
    public void close() {
        try {
            dataStore.getGraph().close();
        } catch (Exception e) {
            logger.error("failed to close graph", e);
        }
    }
}
