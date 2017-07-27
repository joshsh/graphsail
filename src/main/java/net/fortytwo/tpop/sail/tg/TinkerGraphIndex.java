package net.fortytwo.tpop.sail.tg;

import com.google.common.base.Preconditions;
import net.fortytwo.tpop.sail.GraphIndex;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class TinkerGraphIndex extends GraphIndex {
    private final TinkerGraph graph;

    public TinkerGraphIndex(final String key, final TinkerGraph graph) {
        super(key);
        this.graph = graph;
    }

    @Override
    protected boolean isAutomatic() {
        return true;
    }

    @Override
    public void initialize() {
        checkIsProperIndexKey(key);

        if (!keyExists(key)) {
            graph.createIndex(key, Vertex.class);
        }
    }

    @Override
    public void addInternal(Vertex vertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeInternal(final Vertex vertex) {
        throw new UnsupportedOperationException();
    }

    private boolean keyExists(final String key) {
        return graph.getIndexedKeys(Vertex.class).contains(key);
    }

    private void checkIsProperIndexKey(final String key) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key.length() > 0);
    }
}
