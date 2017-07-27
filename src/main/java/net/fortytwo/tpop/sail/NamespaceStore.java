package net.fortytwo.tpop.sail;

import com.google.common.base.Preconditions;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.sail.SailException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory store for IRI namespace definitions.
 * This store is not persisted between sessions.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class NamespaceStore {
    private final Map<String, String> namesByPrefix = new ConcurrentHashMap<>();

    String get(final String prefix) {
        Preconditions.checkNotNull(prefix);

        return namesByPrefix.get(trim(prefix));
    }

    void set(final String prefix, final String name) {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkNotNull(name);

        namesByPrefix.put(trim(prefix), trim(name));
    }

    void remove(final String prefix) {
        Preconditions.checkNotNull(trim(prefix));

        namesByPrefix.remove(trim(prefix));
    }

    void clear() {
        namesByPrefix.clear();
    }

    CloseableIteration<? extends Namespace, SailException> getAll() {
        return IterUtils.toCloseableIteration(namesByPrefix.entrySet().iterator(),
                e -> new SimpleNamespace(e.getKey(), e.getValue()));
    }

    private String trim(final String untrimmed) {
        return untrimmed.trim();
    }
}
