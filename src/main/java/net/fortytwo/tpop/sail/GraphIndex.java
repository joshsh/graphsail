package net.fortytwo.tpop.sail;

import org.apache.tinkerpop.gremlin.structure.Vertex;

public abstract class GraphIndex {
    protected final String key;

    protected GraphIndex(final String key) {
        this.key = key;
    }

    protected abstract boolean isAutomatic();

    public abstract void initialize();

    protected abstract void addInternal(Vertex vertex);

    public abstract void removeInternal(Vertex vertex);

    void add(Vertex vertex) {
        if (!isAutomatic()) {
            addInternal(vertex);
        }
    }

    void remove(Vertex vertex) {
        if (!isAutomatic()) {
            removeInternal(vertex);
        }
    }
}
