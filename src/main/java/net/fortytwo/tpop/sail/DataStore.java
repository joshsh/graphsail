package net.fortytwo.tpop.sail;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.sail.SailException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

/**
 * A context object which is shared between the Blueprints Sail and its connections.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class DataStore {

    private final boolean readOnly;
    private final Graph graph;
    private final GraphTraversalSource traversal;
    private final NamespaceStore namespaces = new NamespaceStore();
    private final ValueFactory valueFactory = SimpleValueFactory.getInstance();

    private final GraphIndex valueIndex;

    private boolean uniqueStatements;

    private final SailChangedHelper sailChangedHelper;

    DataStore(final Graph graph,
              final boolean readOnly,
              final Function<String, GraphIndex> indexFactory,
              final SailChangedHelper sailChangedHelper) {
        this.graph = graph;
        this.traversal = graph.traversal();
        this.readOnly = readOnly;
        this.sailChangedHelper = sailChangedHelper;

        valueIndex = indexFactory.apply(Schema.VertexProperties.VALUE);
        valueIndex.initialize();
    }

    public Graph getGraph() {
        return graph;
    }

    boolean isReadOnly() {
        return readOnly;
    }

    NamespaceStore getNamespaces() {
        return namespaces;
    }

    private CloseableIteration<? extends Statement, SailException> getSubjectStatements(final Resource subject) {
        // assuming all edges are statements
        Vertex vertex = getVertexByValue(subject);
        Iterator<Edge> edges = null == vertex
                ? Collections.emptyIterator()
                : vertex.edges(Direction.OUT);
        return toStatements(edges);
    }

    private CloseableIteration<? extends Statement, SailException> getObjectStatements(final Value object) {
        // assuming all edges are statements
        Vertex vertex = getVertexByValue(object);
        Iterator<Edge> edges = null == vertex
                ? Collections.emptyIterator()
                : vertex.edges(Direction.IN);
        return toStatements(edges);
    }

    CloseableIteration<? extends Statement, SailException> getAllStatements() {
        return toStatements(getAllStatementEdges());
    }

    private Vertex getVertexByValue(final Value value) {
        return getVertexByValue(value, findLabel(value));
    }

    private Vertex getVertexByValue(final Value value, final Schema.VertexLabel vertexLabel) {
        Iterator<Vertex> hits = traversal.V()
                .has(T.label, vertexLabel.name()).has(Schema.VertexProperties.VALUE, value.stringValue());
        while (hits.hasNext()) {
            Vertex next = hits.next();

            // literals, additionally, may differ in datatype or language
            if (vertexLabel.equals(Schema.VertexLabel.Literal)
                    && !datatypeAndLanguageEquals((Literal) value, next)) {
                continue;
            }

            return next;
        }

        return null;
    }

    boolean edgeExists(final Vertex outV, final Vertex inV, final String label, final String context) {
        Iterator<Edge> edges = outV.edges(Direction.OUT, label);
        while (edges.hasNext()) {
            Edge next = edges.next();
            if (!next.inVertex().equals(inV)) continue;
            if (contextEquals(context, next)) {
                return true;
            }
        }
        return false;
    }

    Statement addStatementInternal(final Vertex outV, final Vertex inV, final String label, final String context) {
        Edge edge = outV.addEdge(label, inV);
        registerStatementAdded();
        if (null != context) {
            edge.property(Schema.EdgeProperties.CONTEXT, context);
        }
        return toStatement(edge);
    }

    private void registerStatementAdded() {
        sailChangedHelper.statementsAdded = true;
    }

    private void registerStatementRemoved() {
        sailChangedHelper.statementsRemoved = true;
    }

    private boolean contextEquals(final String expected, final Edge edge) {
        Property<String> actual = edge.property(Schema.EdgeProperties.CONTEXT);
        if (actual.isPresent()) {
            return null != expected && actual.value().equals(expected);
        } else {
            return null == expected;
        }
    }

    private CloseableIteration<? extends Statement, SailException> toStatements(final Iterator<Edge> edges) {
        return IterUtils.toCloseableIteration(edges, this::toStatement);
    }

    private boolean datatypeAndLanguageEquals(final Literal expected, final Vertex vertex) {
        String datatype = getDatatype(vertex);
        if (null == datatype || !datatype.equals(expected.getDatatype().stringValue())) {
            return false;
        }
        if (datatype.equals(RDF.LANGSTRING.stringValue())) {
            String language = getLanguage(vertex);
            if (null == language || !language.equals(expected.getLanguage().get())) {
                return false;
            }
        }

        return true;
    }

    Vertex getOrCreateVertexByValue(final Value value) {
        return getOrCreateVertexByValue(value, findLabel(value));
    }

    private Vertex getOrCreateVertexByValue(final Value value, final Schema.VertexLabel vertexLabel) {
        Vertex vertex = getVertexByValue(value, vertexLabel);
        if (null == vertex) {
            vertex = createNewVertex(value, vertexLabel);
        }
        return vertex;
    }

    private Vertex createNewVertex(final Value value, final Schema.VertexLabel vertexLabel) {
        switch (vertexLabel) {
            case IRI:
                return createNewIRIVertex(value);
            case BNode:
                return createNewBNodeVertex(value);
            case Literal:
                return createNewLiteralVertex(value);
            default:
                throw new IllegalStateException();
        }
    }

    private Vertex createVertex(final String label, final Value value) {
        Vertex vertex = graph.addVertex(label);
        vertex.property(Schema.VertexProperties.VALUE, value.stringValue());
        valueIndex.add(vertex);
        return vertex;
    }

    private Vertex createNewIRIVertex(final Value value) {
        return createVertex(Schema.VertexLabel.IRI.name(), value);
    }

    private Vertex createNewBNodeVertex(final Value value) {
        return createVertex(Schema.VertexLabel.BNode.name(), value);
    }

    private Vertex createNewLiteralVertex(final Value value) {
        Vertex vertex = createVertex(Schema.VertexLabel.Literal.name(), value);
        IRI datatype = ((Literal) value).getDatatype();
        vertex.property(Schema.VertexProperties.DATATYPE, datatype.stringValue());
        if (datatype.equals(RDF.LANGSTRING)) {
            vertex.property(Schema.VertexProperties.LANGUAGE, ((Literal) value).getLanguage().get());
        }
        return vertex;
    }

    private Schema.VertexLabel findLabel(final Value value) {
        if (value instanceof IRI) {
            return Schema.VertexLabel.IRI;
        } else if (value instanceof BNode) {
            return Schema.VertexLabel.BNode;
        } else if (value instanceof Literal) {
            return Schema.VertexLabel.Literal;
        } else {
            throw new IllegalArgumentException();
        }
    }

    // note: for now, every edge in the graph is assumed to be a statement edge
    private Iterator<Edge> getAllStatementEdges() {
        return graph.edges();
    }

    private void removeEdgeCleanly(final Edge edge) {
        Vertex outV = edge.outVertex();
        Vertex inV = edge.inVertex();
        edge.remove();
        removeIfIsolated(outV);
        removeIfIsolated(inV);
    }

    void removeIteratorStatements(final CloseableIteration<? extends Statement, SailException> statements) {
        while (statements.hasNext()) {
            Statement next = statements.next();

            // beware of ConcurrentModificationExceptions
            removeEdgeCleanly(((GraphSailStatement) next).getEdge());
            registerStatementRemoved();
        }
    }

    private void removeIfIsolated(final Vertex toTest) {
        if (isIsolated(toTest)) {
            deleteVertex(toTest);
        }
    }

    private void deleteVertex(final Vertex toDelete) {
        valueIndex.remove(toDelete);
        toDelete.remove();
    }

    private boolean isIsolated(final Vertex toTest) {
        return isEmpty(toTest.edges(Direction.BOTH));
    }

    private <D> boolean isEmpty(final Iterator<D> iter) {
        return !iter.hasNext();
    }

    ValueFactory getValueFactory() {
        return valueFactory;
    }

    boolean getUniqueStatements() {
        return uniqueStatements;
    }

    void setUniqueStatements(boolean uniqueStatements) {
        this.uniqueStatements = uniqueStatements;
    }

    private Statement toStatement(final Edge edge) {
        return new GraphSailStatement(edge,
                getSubject(edge), getPredicate(edge), getObject(edge), getContext(edge));
    }

    private Resource getSubject(final Edge edge) {
        return toResource(edge.outVertex());
    }

    private IRI getPredicate(final Edge edge) {
        return toIRI(edge.label());
    }

    private Value getObject(final Edge edge) {
        return toValue(edge.inVertex());
    }

    private Resource getContext(final Edge edge) {
        Property<String> prop = edge.property(Schema.EdgeProperties.CONTEXT);
        return prop.isPresent() ? toResource(prop.value()) : null;
    }

    private Resource toResource(final Vertex vertex) {
        return (Resource) toValue(vertex);
    }

    private IRI toIRI(final String iriValue) {
        return valueFactory.createIRI(iriValue);
    }

    private BNode toBNode(final String bNodeValue) {
        return valueFactory.createBNode(bNodeValue.substring(2));
    }

    private Resource toResource(final String iriOrBNodeValue) {
        // note: an IRI beginning with "_:" is not allowed
        if (iriOrBNodeValue.startsWith("_:")) {
            return toBNode(iriOrBNodeValue);
        } else {
            return toIRI(iriOrBNodeValue);
        }
    }

    private Value toValue(final Vertex vertex) {
        Schema.VertexLabel kind = Schema.VertexLabel.valueOf(vertex.label());
        switch (kind) {
            case IRI:
                return toIRI(vertex);
            case BNode:
                return toBNode(vertex);
            case Literal:
                return toLiteral(vertex);
            default:
                throw new IllegalStateException();
        }
    }

    private IRI toIRI(final Vertex vertex) {
        return toIRI(getValue(vertex));
    }

    private BNode toBNode(final Vertex vertex) {
        return toBNode(getValue(vertex));
    }

    private Literal toLiteral(final Vertex vertex) {
        String value = getValue(vertex);
        String datatype = getDatatype(vertex);
        if (datatype.equals(RDF.LANGSTRING.stringValue())) {
            String language = getLanguage(vertex);
            return valueFactory.createLiteral(value, language);
        } else {
            IRI dt = valueFactory.createIRI(datatype);
            return valueFactory.createLiteral(value, dt);
        }
    }

    private String getValue(final Vertex vertex) {
        return vertex.value(Schema.VertexProperties.VALUE);
    }

    private String getDatatype(final Vertex vertex) {
        return vertex.value(Schema.VertexProperties.DATATYPE);
    }

    private String getLanguage(final Vertex vertex) {
        Property<String> prop = vertex.property(Schema.VertexProperties.LANGUAGE);
        return prop.isPresent() ? prop.value() : null;
    }

    CloseableIteration<? extends Statement, SailException> buildIterator(final Resource subject,
                                                                         final IRI predicate,
                                                                         final Value object,
                                                                         final Resource... contexts) {
        if (null != subject) {
            return getStatementsBySubject(subject, predicate, object, contexts);
        } else if (null != object) {
            return getStatementsByObject(subject, predicate, object, contexts);
        } else {
            return getStatementsWithFullScan(subject, predicate, object, contexts);
        }
    }

    private CloseableIteration<? extends Statement, SailException> getStatementsBySubject(
            final Resource subject,
            final IRI predicate,
            final Value object,
            final Resource... contexts) {

        CloseableIteration<? extends Statement, SailException> iter = getSubjectStatements(subject);
        if (null != object) {
            iter = addObjectFilter(iter, object);
        }
        if (null != predicate) {
            iter = addPredicateFilter(iter, predicate);
        }
        if (contexts.length > 0) {
            iter = addContextFilter(iter, contexts);
        }
        return iter;
    }

    private CloseableIteration<? extends Statement, SailException> getStatementsByObject(
            final Resource subject,
            final IRI predicate,
            final Value object,
            final Resource... contexts) {
        CloseableIteration<? extends Statement, SailException> iter = getObjectStatements(object);
        if (null != subject) {
            iter = addSubjectFilter(iter, subject);
        }
        if (null != predicate) {
            iter = addPredicateFilter(iter, predicate);
        }
        if (contexts.length > 0) {
            iter = addContextFilter(iter, contexts);
        }
        return iter;
    }

    private CloseableIteration<? extends Statement, SailException> getStatementsWithFullScan(
            final Resource subject,
            final IRI predicate,
            final Value object,
            final Resource... contexts) {
        CloseableIteration<? extends Statement, SailException> iter = getAllStatements();
        if (null != subject) {
            iter = addSubjectFilter(iter, subject);
        }
        if (null != object) {
            iter = addObjectFilter(iter, object);
        }
        if (null != predicate) {
            iter = addPredicateFilter(iter, predicate);
        }
        if (contexts.length > 0) {
            iter = addContextFilter(iter, contexts);
        }
        return iter;
    }

    private <S extends Statement> CloseableIteration<S, SailException> addSubjectFilter(
            final CloseableIteration<S, SailException> base, final Resource equalSubject) {
        return IterUtils.filter(base, statement -> statement.getSubject().equals(equalSubject));
    }

    private <S extends Statement> CloseableIteration<S, SailException> addObjectFilter(
            final CloseableIteration<S, SailException> base, final Value equalObject) {
        return IterUtils.filter(base, statement -> statement.getObject().equals(equalObject));
    }

    private <S extends Statement> CloseableIteration<S, SailException> addPredicateFilter(
            final CloseableIteration<S, SailException> base, final IRI equalPredicate) {
        return IterUtils.filter(base, statement -> statement.getPredicate().equals(equalPredicate));
    }

    private <S extends Statement> CloseableIteration<S, SailException> addContextFilter(
            final CloseableIteration<S, SailException> base, final Resource[] contexts) {
        Set<Resource> contextSet = createNullSafeSetOfContexts(contexts);
        return IterUtils.filter(base,
                statement -> contextSet.contains(statement.getContext()));
    }

    static Set<Resource> findContextsIn(final CloseableIteration<? extends Statement, SailException> iter) {
        Set<Resource> contexts = new HashSet<>(); // may contain null
        while (iter.hasNext()) {
            contexts.add(iter.next().getContext());
        }
        return contexts;
    }

    Set<Resource> createNullSafeSetOfContexts(final Resource[] contexts) {
        // HashSet explicitly allows null as an element
        Set<Resource> set = new HashSet<>();
        Collections.addAll(set, contexts);
        return set;
    }

    SailChangedHelper getSailChangedHelper() {
        return sailChangedHelper;
    }

    abstract static class SailChangedHelper {
        private boolean statementsAdded;
        private boolean statementsRemoved;

        public abstract void notifyOfChanges(boolean statementsAdded, boolean statementsRemoved);

        synchronized void flush() {
            boolean addedTmp = statementsAdded;
            boolean removedTmp = statementsRemoved;
            statementsAdded = false;
            statementsRemoved = false;
            notifyOfChanges(addedTmp, removedTmp);
        }
    }
}
