package net.fortytwo.tpop.sail;

import net.fortytwo.tpop.sail.tg.TinkerGraphIndex;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.sail.Sail;

import java.util.function.Function;

public class GraphSailTest extends SailTest {

    @Override
    protected void before() throws Exception {
    }

    @Override
    protected void after() throws Exception {
    }

    @Override
    protected Sail createSail() throws Exception {
        GraphWrapper wrapper = createGraphWrapper();
        GraphSail sail = new GraphSail(wrapper.getGraph(), wrapper.getIndexFactory());
        sail.enforceUniqueStatements(uniqueStatements);
        return sail;
    }

    // override me for alternative graph back-ends
    protected GraphWrapper createGraphWrapper() {
        return createTinkerGraphWrapper();
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
