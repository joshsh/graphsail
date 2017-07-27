package net.fortytwo.tpop.sail;

import net.fortytwo.tpop.sail.tg.TinkerGraphIndex;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.sail.SailConnection;
import org.junit.After;
import org.junit.Before;

import java.util.function.Function;

public class GraphSailTestBase {

    protected GraphSail graphSail;
    protected SailConnection connection;

    @Before
    public void setUp() {
        graphSail = createSail();
        graphSail.initialize();
    }

    @After
    public void tearDown() {
        graphSail.shutDown();
    }

    protected GraphSail createSail() {
        GraphWrapper wrapper = createGraphWrapper();
        return new GraphSail(wrapper.getGraph(), wrapper.getIndexFactory());
    }

    // override me for alternative graph back-ends
    protected GraphWrapper createGraphWrapper() {
        return createTinkerGraphWrapper();
    }

    protected void createConnection() {
        connection = graphSail.getConnection();
    }

    protected int countStatements() {
        return countIterator(connection.getStatements(null, null, null, false));
    }

    private GraphWrapper createTinkerGraphWrapper() {
        TinkerGraph graph = TinkerGraph.open();
        return new GraphWrapper(graph, key -> new TinkerGraphIndex(key, graph));
    }

    protected static <T, X extends Exception> int countIterator(final CloseableIteration<T, X> iter) throws X {
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        return count;
    }

    private static class GraphWrapper {
        private final Graph graph;
        private final Function<String, GraphIndex> indexFactory;

        private GraphWrapper(final Graph graph, final Function<String, GraphIndex> indexFactory) {
            this.graph = graph;
            this.indexFactory = indexFactory;
        }

        public Graph getGraph() {
            return graph;
        }

        public Function<String, GraphIndex> getIndexFactory() {
            return indexFactory;
        }
    }
}
