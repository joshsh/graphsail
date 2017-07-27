package net.fortytwo.tpop.sail;


import com.google.common.base.Preconditions;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailChangedEvent;
import org.eclipse.rdf4j.sail.SailChangedListener;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.inferencer.InferencerConnection;
import org.eclipse.rdf4j.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.xml.datatype.XMLGregorianCalendar;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class SailTest {
    protected Sail sail;
    protected ForwardChainingRDFSInferencer inferencer;

    protected boolean uniqueStatements = false;

    @Before
    public final void setUp() throws Exception {
        before();
        this.sail = createSail();
        sail.initialize();

        if (sail instanceof NotifyingSail) {
            try (SailConnection sc = getConnection()) {
                if (sc instanceof InferencerConnection) {
                    inferencer = new ForwardChainingRDFSInferencer((NotifyingSail) sail);
                }
            }
        }
    }

    @After
    public final void tearDown() throws Exception {
        sail.shutDown();
        after();
    }

    protected abstract void before() throws Exception;

    protected abstract void after() throws Exception;

    protected abstract Sail createSail() throws Exception;

    // statement manipulation //////////////////////////////////////////////////

    @Test
    public void testGetStatementsS_POG() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/S_POG#a");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/S_POG#b");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/test/S_POG#c");
            IRI uriD = sail.getValueFactory().createIRI("http://example.org/test/S_POG#d");
            long before, after;

            // default context, different S,P,O
            sc.removeStatements(uriA, null, null);
            commit(sc);
            before = countStatements(sc, uriA, null, null, false);
            sc.addStatement(uriA, uriB, uriC);
            commit(sc);
            after = countStatements(sc, uriA, null, null, false);
            assertEquals(0, before);
            System.out.flush();
            assertEquals(1, after);

            // one specific context, different S,P,O
            sc.removeStatements(uriA, null, null, uriD);
            commit(sc);
            before = countStatements(sc, uriA, null, null, false, uriD);
            sc.addStatement(uriA, uriB, uriC, uriD);
            commit(sc);
            after = countStatements(sc, uriA, null, null, false, uriD);
            assertEquals(0, before);
            assertEquals(1, after);

            // one specific context, same S,P,O,G
            sc.removeStatements(uriA, null, null, uriA);
            commit(sc);
            before = countStatements(sc, uriA, null, null, false, uriA);
            sc.addStatement(uriA, uriB, uriC, uriA);
            commit(sc);
            after = countStatements(sc, uriA, null, null, false, uriA);
            assertEquals(0, before);
            assertEquals(1, after);

            // default context, same S,P,O
            sc.removeStatements(uriA, null, null);
            commit(sc);
            before = countStatements(sc, uriA, null, null, false);
            sc.addStatement(uriA, uriB, uriC);
            commit(sc);
            after = countStatements(sc, uriA, null, null, false);
            assertEquals(0, before);
            assertEquals(1, after);
        }
    }

    @Test
    public void testGetStatementsSP_OG() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/SP_OG#a");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/SP_OG#b");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/test/SP_OG#c");
            long before, after;

            // Add statement to the implicit null context.
            sc.removeStatements(null, null, null);
            before = countStatements(sc, uriA, uriB, null, false);
            sc.addStatement(uriA, uriB, uriC);
            commit(sc);
            after = countStatements(sc, uriA, uriB, null, false);
            assertEquals(0, before);
            assertEquals(1, after);
        }
    }

    @Test
    public void testGetStatementsO_SPG() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/O_SPG#a");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/O_SPG#b");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/test/O_SPG#c");
            Literal plainLitA = sail.getValueFactory().createLiteral("arbitrary plain literal 9548734867");
            Literal stringLitA = sail.getValueFactory().createLiteral("arbitrary string literal 8765", XMLSchema.STRING);
            long before, after;

            // Add statement to a specific context.
            sc.removeStatements(null, null, uriA, uriA);
            commit(sc);
            before = countStatements(sc, null, null, uriA, false);
            sc.addStatement(uriB, uriC, uriA);
            commit(sc);
            after = countStatements(sc, null, null, uriA, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // Add plain literal statement to the default context.
            sc.removeStatements(null, null, plainLitA);
            commit(sc);
            before = countStatements(sc, null, null, plainLitA, false);
            sc.addStatement(uriA, uriA, plainLitA);
            commit(sc);
            after = countStatements(sc, null, null, plainLitA, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // Add string-typed literal statement to the default context.
            sc.removeStatements(null, null, plainLitA);
            commit(sc);
            before = countStatements(sc, null, null, stringLitA, false);
            sc.addStatement(uriA, uriA, stringLitA);
            commit(sc);
            after = countStatements(sc, null, null, stringLitA, false);
            assertEquals(0, before);
            assertEquals(1, after);
        }
    }

    @Test
    public void testGetStatementsPO_SG() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/PO_SG#a");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/PO_SG#b");
            IRI foo = sail.getValueFactory().createIRI("http://example.org/ns#foo");
            IRI firstName = sail.getValueFactory().createIRI("http://example.org/ns#firstName");
            Literal plainLitA = sail.getValueFactory().createLiteral("arbitrary plain literal 8765675");
            Literal fooLabel = sail.getValueFactory().createLiteral("foo", XMLSchema.STRING);
            long before, after;

            // Add statement to the implicit null context.
            sc.removeStatements(null, null, null, uriA);
            commit(sc);
            before = countStatements(sc, null, uriA, uriB, false);
            sc.addStatement(uriA, uriA, uriB);
            commit(sc);
            after = countStatements(sc, null, uriA, uriB, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // Add plain literal statement to the default context.
            sc.removeStatements(null, null, plainLitA);
            commit(sc);
            before = countStatements(sc, null, uriA, plainLitA, false);
            sc.addStatement(uriA, uriA, plainLitA);
            sc.addStatement(uriA, uriB, plainLitA);
            sc.addStatement(uriB, uriB, plainLitA);
            commit(sc);
            after = countStatements(sc, null, uriA, plainLitA, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // Add string-typed literal statement to the default context.
            sc.removeStatements(null, null, fooLabel);
            commit(sc);
            before = countStatements(sc, null, firstName, fooLabel, false);
            sc.addStatement(foo, firstName, fooLabel);
            commit(sc);
            after = countStatements(sc, null, firstName, fooLabel, false);
            assertEquals(0, before);
            assertEquals(1, after);
            assertEquals(foo, toSet(sc.getStatements(null, firstName, fooLabel, false)).iterator().next().getSubject());

        }
    }

    @Test
    public void testGetStatementsSPO_G() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/S_POG#a");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/S_POG#b");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/test/S_POG#c");
            IRI uriD = sail.getValueFactory().createIRI("http://example.org/test/S_POG#d");
            long before, after;

            // default context, different S,P,O
            sc.removeStatements(uriA, null, null);
            commit(sc);
            before = countStatements(sc, uriA, uriB, uriC, false);
            sc.addStatement(uriA, uriB, uriC);
            commit(sc);
            after = countStatements(sc, uriA, uriB, uriC, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // default context, same S,P,O
            sc.removeStatements(uriA, null, null);
            commit(sc);
            before = countStatements(sc, uriA, uriB, uriC, false);
            sc.addStatement(uriA, uriB, uriC);
            commit(sc);
            after = countStatements(sc, uriA, uriB, uriC, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // one specific context, different S,P,O
            sc.removeStatements(uriA, null, null, uriD);
            commit(sc);
            before = countStatements(sc, uriA, uriB, uriC, false, uriD);
            sc.addStatement(uriA, uriB, uriC, uriD);
            commit(sc);
            after = countStatements(sc, uriA, uriB, uriC, false, uriD);
            assertEquals(0, before);
            assertEquals(1, after);

            // one specific context, same S,P,O,G
            sc.removeStatements(uriA, null, null, uriA);
            commit(sc);
            before = countStatements(sc, uriA, uriB, uriC, false, uriA);
            sc.addStatement(uriA, uriB, uriC, uriA);
            commit(sc);
            after = countStatements(sc, uriA, uriB, uriC, false, uriA);
            assertEquals(0, before);
            assertEquals(1, after);
        }
    }

    @Test
    public void testGetStatementsP_SOG() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/P_SOG#a");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/P_SOG#b");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/test/P_SOG#c");
            IRI foo = sail.getValueFactory().createIRI("http://example.org/ns#foo");
            IRI firstName = sail.getValueFactory().createIRI("http://example.org/ns#firstName");
            Literal plainLitA = sail.getValueFactory().createLiteral("arbitrary plain literal 238445");
            Literal fooLabel = sail.getValueFactory().createLiteral("foo", XMLSchema.STRING);
            long before, after;

            // Add statement to the implicit null context.
            sc.removeStatements(null, uriA, null);
            commit(sc);
            before = countStatements(sc, null, uriA, null, false);
            sc.addStatement(uriB, uriA, uriC);
            commit(sc);
            after = countStatements(sc, null, uriA, null, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // Add plain literal statement to the default context.
            sc.removeStatements(null, uriA, null);
            commit(sc);
            before = countStatements(sc, null, uriA, null, false);
            sc.addStatement(uriA, uriA, plainLitA);
            sc.addStatement(uriA, uriB, plainLitA);
            sc.addStatement(uriB, uriB, plainLitA);
            commit(sc);
            after = countStatements(sc, null, uriA, null, false);
            assertEquals(0, before);
            assertEquals(1, after);

            // Add string-typed literal statement to the default context.
            sc.removeStatements(null, firstName, null);
            commit(sc);
            before = countStatements(sc, null, firstName, null, false);
            sc.addStatement(foo, firstName, fooLabel);
            commit(sc);
            after = countStatements(sc, null, firstName, null, false);
            assertEquals(0, before);
            assertEquals(1, after);
            assertEquals(foo, toSet(sc.getStatements(null, firstName, null, false)).iterator().next().getSubject());

            // Add statement to a non-null context.
            sc.removeStatements(null, uriA, null);
            commit(sc);
            before = countStatements(sc, null, uriA, null, false);
            sc.addStatement(uriB, uriA, uriC, uriA);
            commit(sc);
            after = countStatements(sc, null, uriA, null, false);
            assertEquals(0, before);
            assertEquals(1, after);

            sc.removeStatements(null, uriA, null);
            commit(sc);
            before = countStatements(sc, null, uriA, null, false);
            sc.addStatement(uriB, uriA, uriC, uriC);
            sc.addStatement(uriC, uriA, uriA, uriA);
            commit(sc);
            sc.addStatement(uriA, uriA, uriB, uriB);
            commit(sc);
            after = countStatements(sc, null, uriA, null, false);
            assertEquals(0, before);
            assertEquals(3, after);
        }
    }

    @Test
    public void testGetStatementsWithVariableContexts() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");
            long count;
            sc.clear();
            //sc.removeStatements(uriA, uriA, uriA);
            commit(sc);
            Resource[] contexts = {uriA, null};
            sc.addStatement(uriA, uriB, uriC, contexts);
            commit(sc);

            // Get statements from all contexts.
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(2, count);

            // Get statements from a specific partition context.
            count = countStatements(sc, null, null, null, false, uriA);
            assertEquals(1, count);

            // Get statements from the null context.
            Resource[] c = {null};
            count = countStatements(sc, null, null, null, false, c);
            //assertTrue(count > 0);
            assertEquals(1, count);
            long countLast = count;

            // Get statements from more than one context.
            count = countStatements(sc, null, null, null, false, contexts);
            assertEquals(1 + countLast, count);

        }
    }

    @Test
    public void testRemoveStatements() throws Exception {
        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");
            Resource[] contexts = {uriA, null};
            long count;

            // Remove from all contexts.
            sc.removeStatements(uriA, null, null);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
            sc.addStatement(uriA, uriB, uriC, contexts);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            // two, because the statement was added to two contexts
            assertEquals(2, count);
            sc.removeStatements(uriA, null, null);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);

            // Remove from one partition context.
            sc.removeStatements(uriA, null, null);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
            sc.addStatement(uriA, uriB, uriC, contexts);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(2, count);
            Resource[] oneContext = {uriA};
            sc.removeStatements(uriA, null, null, oneContext);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(1, count);

            // Remove from the null context.
            sc.removeStatements(uriA, null, null);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
            sc.addStatement(uriA, uriB, uriC, contexts);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(2, count);
            Resource[] nullContext = {null};
            sc.removeStatements(uriA, null, null, nullContext);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(1, count);

            // Remove from more than one context.
            sc.removeStatements(uriA, null, null);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
            sc.addStatement(uriA, uriB, uriC, contexts);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(2, count);
            sc.removeStatements(uriA, null, null);
            commit(sc);
            count = countStatements(sc, uriA, null, null, false, contexts);
            assertEquals(0, count);
        }
    }

    @Test
    public void testClear() throws Exception {
        IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
        IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
        IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");

        try (SailConnection sc = getConnection()) {
            sc.clear();
            assertEquals(0L, sc.size());
            sc.addStatement(uriA, uriB, uriC, uriA);
            sc.addStatement(uriC, uriA, uriB, uriA);
            sc.addStatement(uriB, uriC, uriA, uriA);
            assertEquals(3L, sc.size(uriA));
            sc.addStatement(uriA, uriB, uriC, uriB);
            sc.addStatement(uriB, uriC, uriA, uriB);
            assertEquals(2L, sc.size(uriB));
            sc.addStatement(uriA, uriB, uriC);
            assertEquals(1L, sc.size((Resource) null));
            sc.addStatement(uriA, uriB, uriC, uriC);
            sc.addStatement(uriB, uriC, uriA, uriC);
            sc.addStatement(uriC, uriA, uriB, uriC);
            sc.addStatement(uriA, uriB, uriB, uriC);
            assertEquals(4L, sc.size(uriC));
            assertEquals(10L, sc.size());
            sc.clear(uriA, uriC);
            assertEquals(1L, sc.size((Resource) null));
            assertEquals(0L, sc.size(uriA));
            assertEquals(2L, sc.size(uriB));
            assertEquals(0L, sc.size(uriC));
            assertEquals(3L, sc.size());
            sc.clear();
            assertEquals(0L, sc.size());
            commit(sc);
        }
    }

    @Test
    public void testGetContextIDs() throws Exception {
        // TODO
    }

    @Test
    public void testSize() throws Exception {
        IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
        IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
        IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");

        try (SailConnection sc = getConnection()) {
            sc.removeStatements(null, null, null);

            assertEquals(0L, sc.size());
            sc.addStatement(uriA, uriB, uriC, uriA);
            // commit(sc);
            for (Statement st : IterUtils.collect(sc.getStatements(null, null, null, false))) {
                System.out.println("st: " + st);
            }
            assertEquals(1L, sc.size());
            sc.addStatement(uriA, uriB, uriC, uriB);
            // commit(sc);
            assertEquals(2L, sc.size());
            sc.addStatement(uriB, uriB, uriC, uriB);
            // commit(sc);
            assertEquals(3L, sc.size());
            sc.addStatement(uriC, uriB, uriA);
            // commit(sc);
            assertEquals(4L, sc.size());
            assertEquals(1L, sc.size(uriA));
            assertEquals(2L, sc.size(uriB));
            assertEquals(0L, sc.size(uriC));
            assertEquals(1L, sc.size((IRI) null));
            assertEquals(3L, sc.size(uriB, null));
            assertEquals(3L, sc.size(uriB, uriC, null));
            assertEquals(4L, sc.size(uriA, uriB, null));
            assertEquals(4L, sc.size(uriA, uriB, uriC, null));
            assertEquals(3L, sc.size(uriA, uriB));
            commit(sc);
        }
    }

    @Test
    public void testDuplicateStatements() throws Exception {
        if (uniqueStatements) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");
            try (SailConnection sc = getConnection()) {
                sc.clear();
                assertEquals(0, countStatements(sc, uriA, uriB, uriC, false));
                sc.addStatement(uriA, uriB, uriC);
                assertEquals(1, countStatements(sc, uriA, uriB, uriC, false));
                sc.addStatement(uriA, uriB, uriC);
                assertEquals(1, countStatements(sc, uriA, uriB, uriC, false));

                sc.addStatement(uriA, uriB, uriC, uriC);
                assertEquals(2, countStatements(sc, uriA, uriB, uriC, false));
                assertEquals(1, countStatements(sc, uriA, uriB, uriC, false, uriC));
                commit(sc);
            }
        }
    }

    // IRIs ////////////////////////////////////////////////////////////////////

    // literals ////////////////////////////////////////////////////////////////

    // Note: this test will always pass as long as we're using ValueFactoryImpl

    @Test
    public void testCreateLiteralsThroughValueFactory() throws Exception {
        Literal l;
        ValueFactory vf = sail.getValueFactory();

        l = vf.createLiteral("a plain literal");
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("a plain literal", l.getLabel());
        assertEquals(XMLSchema.STRING, l.getDatatype());
        l = vf.createLiteral("auf Deutsch, bitte", "de");
        assertNotNull(l);
        assertEquals("de", l.getLanguage().get());
        assertEquals("auf Deutsch, bitte", l.getLabel());
        assertEquals(RDF.LANGSTRING, l.getDatatype());

        // Test data-typed createLiteral methods
        l = vf.createLiteral("foo", XMLSchema.STRING);
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("foo", l.getLabel());
        assertEquals(XMLSchema.STRING, l.getDatatype());
        l = vf.createLiteral(42);
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("42", l.getLabel());
        assertEquals(42, l.intValue());
        assertEquals(XMLSchema.INT, l.getDatatype());
        l = vf.createLiteral(42L);
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("42", l.getLabel());
        assertEquals(42L, l.longValue());
        assertEquals(XMLSchema.LONG, l.getDatatype());
        l = vf.createLiteral((short) 42);
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("42", l.getLabel());
        assertEquals((short) 42, l.shortValue());
        assertEquals(XMLSchema.SHORT, l.getDatatype());
        l = vf.createLiteral(true);
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("true", l.getLabel());
        assertEquals(true, l.booleanValue());
        assertEquals(XMLSchema.BOOLEAN, l.getDatatype());
        l = vf.createLiteral((byte) 'c');
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("99", l.getLabel());
        assertEquals((byte) 'c', l.byteValue());
        assertEquals(XMLSchema.BYTE, l.getDatatype());
        XMLGregorianCalendar calendar = XMLDatatypeUtil.parseCalendar("2002-10-10T12:00:00-05:00");
        l = vf.createLiteral(calendar);
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("2002-10-10T12:00:00-05:00", l.getLabel());
        assertEquals(calendar, l.calendarValue());
        assertEquals(XMLSchema.DATETIME, l.getDatatype());
    }

    @Test
    public void testGetLiteralsFromTripleStore() throws Exception {
        addTestFile();

        Literal l;
        String prefix = "urn:com.tinkerpop.blueprints.pgm.oupls.sail.test/";
        XMLGregorianCalendar calendar;
        ValueFactory vf = sail.getValueFactory();
        SailConnection sc = getConnection();

        // Get an actual plain literal from the triple store.
        IRI ford = vf.createIRI(prefix + "ford");
        l = (Literal) toSet(sc.getStatements(ford, RDFS.COMMENT, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertEquals("he really knows where his towel is", l.getLabel());
        assertEquals(XMLSchema.STRING, l.getDatatype());
        assertFalse(l.getLanguage().isPresent());
        IRI thor = vf.createIRI(prefix + "thor");

        // Get an actual language-tagged literal from the triple store.
        IRI foafName = vf.createIRI("http://xmlns.com/foaf/0.1/name");
        Iterator<Statement> iter = toSet(sc.getStatements(thor, foafName, null, false)).iterator();
        boolean found = false;
        while (iter.hasNext()) {
            l = (Literal) iter.next().getObject();
            if (l.getLanguage().get().equals("en")) {
                found = true;
                assertEquals("Thor", l.getLabel());
                assertEquals(RDF.LANGSTRING, l.getDatatype());
            }
            // if (l.getLanguage().equals("is")) {
            // found = true;
            // assertEquals("?�r", l.getLabel());
            // }
        }
        assertTrue(found);

        // Get an actual data-typed literal from the triple-store.
        IRI msnChatID = vf.createIRI("http://xmlns.com/foaf/0.1/msnChatID");
        l = (Literal) toSet(sc.getStatements(thor, msnChatID, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("Thorster123", l.getLabel());
        assertEquals(XMLSchema.STRING, l.getDatatype());

        // Test Literal.xxxValue() methods for Literals read from the triple
        // store
        IRI valueUri, hasValueUri;
        hasValueUri = vf.createIRI(prefix + "hasValue");
        valueUri = vf.createIRI(prefix + "stringValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("foo", l.getLabel());
        assertEquals(XMLSchema.STRING, l.getDatatype());
        valueUri = vf.createIRI(prefix + "byteValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("99", l.getLabel());
        assertEquals(XMLSchema.BYTE, l.getDatatype());
        assertEquals((byte) 'c', l.byteValue());
        valueUri = vf.createIRI(prefix + "booleanValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("false", l.getLabel());
        assertEquals(XMLSchema.BOOLEAN, l.getDatatype());
        assertEquals(false, l.booleanValue());
        valueUri = vf.createIRI(prefix + "intValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("42", l.getLabel());
        assertEquals(XMLSchema.INT, l.getDatatype());
        assertEquals(42, l.intValue());
        valueUri = vf.createIRI(prefix + "shortValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("42", l.getLabel());
        assertEquals(XMLSchema.SHORT, l.getDatatype());
        assertEquals((short) 42, l.shortValue());
        valueUri = vf.createIRI(prefix + "longValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("42", l.getLabel());
        assertEquals(XMLSchema.LONG, l.getDatatype());
        assertEquals(42L, l.longValue());
        valueUri = vf.createIRI(prefix + "floatValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("3.1415926", l.getLabel());
        assertEquals(XMLSchema.FLOAT, l.getDatatype());
        assertEquals((float) 3.1415, l.floatValue(), 0.0001);
        valueUri = vf.createIRI(prefix + "doubleValue");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("3.1415926", l.getLabel());
        assertEquals(XMLSchema.DOUBLE, l.getDatatype());
        assertEquals(3.1415, l.doubleValue(), 0.0001);
        valueUri = vf.createIRI(prefix + "dateTimeValue");
        calendar = XMLDatatypeUtil.parseCalendar("2002-10-10T12:00:00-05:00");
        l = (Literal) toSet(sc.getStatements(valueUri, hasValueUri, null, false)).iterator().next().getObject();
        assertNotNull(l);
        assertFalse(l.getLanguage().isPresent());
        assertEquals("2002-10-10T12:00:00-05:00", l.getLabel());
        assertEquals(XMLSchema.DATETIME, l.getDatatype());
        assertEquals(calendar, l.calendarValue());
        sc.close();
    }

    // blank nodes /////////////////////////////////////////////////////////////

    @Test
    public void testBlankNodes() throws Throwable {
        IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/S_POG#a");
        IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/S_POG#b");
        try (SailConnection sc = getConnection()) {
            ValueFactory factory = sail.getValueFactory();
            BNode bNode = factory.createBNode();
            try {
                sc.addStatement(uriA, uriA, bNode);
            } catch (SailException se) {
                // FIXME: not supporting blank nodes ATM
                assertTrue(se.getCause() instanceof UnsupportedOperationException);
            }
            commit(sc);
        }
    }

    // tuple queries ///////////////////////////////////////////////////////////

    @Test
    public void testEvaluate() throws Exception {
        addTestFile();

        Set<String> languages;
        String prefix = "urn:com.tinkerpop.blueprints.pgm.oupls.sail.test/";
        IRI thorUri = sail.getValueFactory().createIRI(prefix + "thor");

        try (SailConnection sc = getConnection()) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");
            sc.addStatement(uriA, uriB, uriC);
            commit(sc);

            SPARQLParser parser = new SPARQLParser();
            BindingSet bindings = new EmptyBindingSet();
            String baseIRI = "http://example.org/bogus/";
            String queryStr;
            ParsedQuery query;
            CloseableIteration<? extends BindingSet, QueryEvaluationException> results;
            int count;
            // s ?p ?o SELECT
            queryStr = "SELECT ?y ?z WHERE { <http://example.org/uriA> ?y ?z }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                IRI y = (IRI) set.getValue("y");
                Value z = set.getValue("z");
                assertNotNull(y);
                assertNotNull(z);
                // System.out.println("y = " + y + ", z = " + z);
            }
            results.close();
            assertTrue(count > 0);

            // s p ?o SELECT using a namespace prefix
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?z WHERE { <" + prefix + "thor> foaf:name ?z }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            languages = new HashSet<>();
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                Literal z = (Literal) set.getValue("z");
                assertNotNull(z);
                languages.add(z.getLanguage().get());
            }
            results.close();
            assertTrue(count > 0);
            assertEquals(2, languages.size());
            assertTrue(languages.contains("en"));
            assertTrue(languages.contains("is"));

            // ?s p o SELECT using a plain literal value with no language tag
            queryStr = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                    + "SELECT ?s WHERE { ?s rdfs:comment \"he really knows where his towel is\" }";
            IRI fordUri = sail.getValueFactory().createIRI(prefix + "ford");
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                IRI s = (IRI) set.getValue("s");
                assertNotNull(s);
                assertEquals(s, fordUri);
            }
            results.close();
            assertTrue(count > 0);

            // ?s p o SELECT using a language-specific literal value
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?s WHERE { ?s foaf:name \"Thor\"@en }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                IRI s = (IRI) set.getValue("s");
                assertNotNull(s);
                assertEquals(s, thorUri);
            }
            results.close();
            assertTrue(count > 0);

            // The language tag is necessary
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?s WHERE { ?s foaf:name \"Thor\" }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                results.next();
            }
            results.close();
            assertEquals(0, count);

            // ?s p o SELECT using a typed literal value
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + "SELECT ?s WHERE { ?s foaf:msnChatID \"Thorster123\"^^xsd:string }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                IRI s = (IRI) set.getValue("s");
                assertNotNull(s);
                assertEquals(s, thorUri);
            }
            results.close();
            assertTrue(count > 0);

            // In RDF 1.1, the xsd:string datatype is the default, and does not need to be given in the query
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
                    + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
                    + "SELECT ?s WHERE { ?s foaf:msnChatID \"Thorster123\" }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            assertEquals(1, IterUtils.count(results));

            // s ?p o SELECT
            // TODO: commented out languages for now
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + "SELECT ?p WHERE { <" + prefix + "thor> ?p \"Thor\"@en }";
            query = parser.parseQuery(queryStr, baseIRI);
            IRI foafNameUri = sail.getValueFactory().createIRI("http://xmlns.com/foaf/0.1/name");
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                IRI p = (IRI) set.getValue("p");
                assertNotNull(p);
                assertEquals(p, foafNameUri);
            }
            results.close();
            assertTrue(count > 0);

            // context-specific SELECT
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?z\n" + "FROM <" + prefix + "ctx1>\n" + "WHERE { <" + prefix + "thor> foaf:name ?z }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            languages = new HashSet<>();
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                Literal z = (Literal) set.getValue("z");
                assertNotNull(z);
                languages.add(z.getLanguage().get());
            }
            results.close();
            assertTrue(count > 0);
            assertEquals(2, languages.size());
            assertTrue(languages.contains("en"));
            assertTrue(languages.contains("is"));
            queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?z\n" + "FROM <http://example.org/emptycontext>\n" + "WHERE { <" + prefix + "thor> foaf:name ?z }";
            query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                results.next();
            }
            results.close();
            assertEquals(0, count);

            // s p o? select without and with inferencing
            // TODO commented out waiting for inferencing
            // queryStr =
            // "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            // + "SELECT ?o\n"
            // + "WHERE { <" + prefix + "instance1> rdf:type ?o }";
            // query = parser.parseQuery(queryStr, baseIRI);
            // results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
            // bindings, false);
            // count = 0;
            // while (results.hasNext()) {
            // count++;
            // BindingSet set = results.next();
            // IRI o = (IRI) set.getValue("o");
            // assertEquals(prefix + "classB", o.toString());
            // }
            // results.close();
            // assertEquals(1, count);
            // results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
            // bindings, true);
            // count = 0;
            // boolean foundA = false, foundB = false;
            // while (results.hasNext()) {
            // count++;
            // BindingSet set = results.next();
            // IRI o = (IRI) set.getValue("o");
            // String s = o.toString();
            // if (s.equals(prefix + "classA")) {
            // foundA = true;
            // } else if (s.equals(prefix + "classB")) {
            // foundB = true;
            // }
            // }
            // results.close();
            // assertEquals(2, count);
            // assertTrue(foundA);
            // assertTrue(foundB);

        }
    }

    @Test
    public void testJoins() throws Exception {
        addTestFile();

        SPARQLParser parser = new SPARQLParser();
        BindingSet bindings = new EmptyBindingSet();
        String baseIRI = "http://example.org/bogus/";

        try (SailConnection sc = getConnection()) {
            CloseableIteration<? extends BindingSet, QueryEvaluationException> results;
            int count;
            String queryStr = "PREFIX : <urn:com.tinkerpop.blueprints.pgm.oupls.sail.test/>\n" +
                    "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
                    "SELECT ?foaf WHERE {\n" +
                    "    :ford foaf:knows ?friend .\n" +
                    "    ?friend foaf:knows ?foaf .\n" +
                    "}";
            ParsedQuery query = parser.parseQuery(queryStr, baseIRI);
            results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
            count = 0;
            while (results.hasNext()) {
                count++;
                BindingSet set = results.next();
                IRI foaf = (IRI) set.getValue("foaf");
                assertTrue(foaf.stringValue().startsWith("urn:com.tinkerpop.blueprints.pgm.oupls.sail.test/"));
            }
            results.close();
            assertEquals(4, count);
        }
    }

    // listeners ///////////////////////////////////////////////////////////////
    // (disabled for Sails which do not implement NotifyingSail)

    @Test
    public void testSailConnectionListeners() throws Exception {
        if (sail instanceof NotifyingSail) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");

            TestListener listener1 = new TestListener(), listener2 = new TestListener();
            try (NotifyingSailConnection sc = (NotifyingSailConnection) getConnection()) {
                sc.clear();
                commit(sc);

                // Add a listener and add statements
                sc.addConnectionListener(listener1);
                sc.addStatement(uriA, uriB, uriC, uriA);
                sc.addStatement(uriB, uriC, uriA, uriA);
                commit(sc);

                // Add another listener and remove a statement
                sc.addConnectionListener(listener2);
                sc.removeStatements(uriA, null, null);
                commit(sc);

                assertEquals(2, listener1.getAdded());
                assertEquals(0, listener2.getAdded());
                assertEquals(1, listener1.getRemoved());
                assertEquals(1, listener2.getRemoved());

                // Remove a listener and clear
                sc.removeConnectionListener(listener1);
                sc.clear();
                commit(sc);

                assertEquals(1, listener1.getRemoved());
                assertEquals(2, listener2.getRemoved());
            }
        }
    }

    @Test
    public void testSailChangedListeners() throws Exception {
        if (sail instanceof NotifyingSail) {
            final Collection<SailChangedEvent> events = new LinkedList<>();
            SailChangedListener listener = events::add;
            ((NotifyingSail) sail).addSailChangedListener(listener);
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
            IRI uriC = sail.getValueFactory().createIRI("http://example.org/uriC");
            try (SailConnection sc = getConnection()) {
                sc.clear();
                commit(sc);
                events.clear();
                assertEquals(0, events.size());
                sc.addStatement(uriA, uriB, uriC, uriA);
                sc.addStatement(uriB, uriC, uriA, uriA);
                // Events are buffered until the commit
                assertEquals(0, events.size());
                commit(sc);
                // Only one SailChangedEvent per commit
                assertEquals(1, events.size());
                SailChangedEvent event = events.iterator().next();
                assertTrue(event.statementsAdded());
                assertFalse(event.statementsRemoved());
                events.clear();
                assertEquals(0, events.size());
                sc.removeStatements(uriA, uriB, uriC, uriA);
                commit(sc);
                assertEquals(1, events.size());
                event = events.iterator().next();
                assertFalse(event.statementsAdded());
                assertTrue(event.statementsRemoved());
                events.clear();
                assertEquals(0, events.size());
                sc.clear();
                commit(sc);
                assertEquals(1, events.size());
                event = events.iterator().next();
                assertFalse(event.statementsAdded());
                assertTrue(event.statementsRemoved());
            }
        }
    }

    // namespaces //////////////////////////////////////////////////////////////

    @Test
    public void testClearNamespaces() throws Exception {
        addTestFile();

        try (SailConnection sc = getConnection()) {
            CloseableIteration<? extends Namespace, SailException> namespaces;
            int count;
            count = 0;
            namespaces = sc.getNamespaces();
            while (namespaces.hasNext()) {
                namespaces.next();
                count++;
            }
            namespaces.close();
            assertTrue(count > 0);
            // TODO: actually clear namespaces (but this wipes them out for
            // subsequent tests)
        }
    }

    @Test
    public void testGetNamespace() throws Exception {
        addTestFile();

        try (SailConnection sc = getConnection()) {
            String name;
            name = sc.getNamespace("bogus");
            assertNull(name);
            name = sc.getNamespace("rdfs");
            assertEquals(name, "http://www.w3.org/2000/01/rdf-schema#");
        }
    }

    private void showNamespaces(final SailConnection c) throws SailException {
        System.out.println("namespaces:");
        try (CloseableIteration<? extends Namespace, SailException> iter = c.getNamespaces()) {
            while (iter.hasNext()) {
                Namespace n = iter.next();
                System.out.println("\t" + n.getPrefix() + ":\t" + n.getName());
            }
        }
    }

    @Test
    public void testGetNamespaces() throws Exception {
        try (SailConnection sc = getConnection()) {
            CloseableIteration<? extends Namespace, SailException> namespaces;
            int before = 0, during = 0, after = 0;
            // just iterate through all namespaces
            namespaces = sc.getNamespaces();
            while (namespaces.hasNext()) {
                Namespace ns = namespaces.next();
                before++;
                // System.out.println("namespace: " + ns);
            }
            namespaces.close();
            // Note: assumes that these namespace prefixes are unused.
            int nTests = 10;
            String prefixPrefix = "testns";
            String namePrefix = "http://example.org/test";
            for (int i = 0; i < nTests; i++) {
                sc.setNamespace(prefixPrefix + i, namePrefix + i);
            }
            commit(sc);
            namespaces = sc.getNamespaces();
            while (namespaces.hasNext()) {
                Namespace ns = namespaces.next();
                during++;
                String prefix = ns.getPrefix();
                String name = ns.getName();
                if (prefix.startsWith(prefixPrefix)) {
                    assertEquals(name, namePrefix + prefix.substring(prefixPrefix.length()));
                }
            }
            namespaces.close();
            for (int i = 0; i < nTests; i++) {
                sc.removeNamespace(prefixPrefix + i);
            }
            commit(sc);
            namespaces = sc.getNamespaces();
            while (namespaces.hasNext()) {
                namespaces.next();
                after++;
            }
            namespaces.close();
            assertEquals(during, before + nTests);
            assertEquals(after, before);
        }
    }

    @Test
    public void testSetNamespace() throws Exception {
        try (SailConnection sc = getConnection()) {
            String prefix = "foo";
            String emptyPrefix = "";
            String name = "http://example.org/foo";
            String otherName = "http://example.org/bar";

            sc.removeNamespace(prefix);
            sc.removeNamespace(emptyPrefix);
            commit(sc);

            // Namespace initially absent?
            assertNull(sc.getNamespace(prefix));
            assertNull(sc.getNamespace(emptyPrefix));

            // Can we set the namespace?
            sc.setNamespace(prefix, name);
            commit(sc);
            assertEquals(sc.getNamespace(prefix), name);

            // Can we reset the namespace?
            sc.setNamespace(prefix, otherName);
            commit(sc);
            assertEquals(sc.getNamespace(prefix), otherName);

            // Can we use an empty namespace prefix?
            sc.setNamespace(emptyPrefix, name);
            commit(sc);
            assertEquals(sc.getNamespace(emptyPrefix), name);
        }
    }

    @Test
    public void testRemoveNamespace() throws Exception {
        try (SailConnection sc = getConnection()) {
            String prefix = "foo";
            String emptyPrefix = "";
            String name = "http://example.org/foo";

            // Set namespace initially.
            sc.setNamespace(prefix, name);
            commit(sc);
            assertEquals(sc.getNamespace(prefix), name);

            // Remove the namespace and make sure it's gone.
            sc.removeNamespace(prefix);
            commit(sc);
            assertNull(sc.getNamespace(prefix));

            // Same thing for the default namespace.
            sc.setNamespace(emptyPrefix, name);
            commit(sc);
            assertEquals(sc.getNamespace(emptyPrefix), name);
            sc.removeNamespace(emptyPrefix);
            commit(sc);
            assertNull(sc.getNamespace(emptyPrefix));
        }
    }

    // connections and transactions ////////////////////////////////////////////

    @Test
    public void testPersistentCommits() throws Exception {
        SailConnection sc;
        long count;
        IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/persistentCommits#a");
        IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/persistentCommits#b");
        IRI uriC = sail.getValueFactory().createIRI("http://example.org/test/persistentCommits#c");
        sc = getConnection();
        try {
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
            sc.close();

            sc = getConnection();
            sc.addStatement(uriA, uriB, uriC);
            count = countStatements(sc, uriA, null, null, false);
            assertNotEquals(0, count);
            commit(sc);
            sc.close();

            sc = getConnection();
            count = countStatements(sc, uriA, null, null, false);
            assertNotEquals(0, count);
            sc.close();

            sc = getConnection();
            sc.removeStatements(uriA, null, null);
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
            commit(sc);
            sc.close();

            sc = getConnection();
            count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
        } finally {
            sc.close();
        }
    }

    @Test
    public void testVisibilityOfChanges() throws Exception {
        SailConnection sc1, sc2;
        long count;
        IRI uriA = sail.getValueFactory().createIRI(
                "http://example.org/test/visibilityOfChanges#a");
        sc1 = getConnection();
        sc2 = getConnection();
        try {
            sc1.clear();
            sc2.clear();

            // Statement doesn't exist for either connection.
            count = countStatements(sc1, uriA, null, null);
            assertEquals(0, count);
            count = countStatements(sc2, uriA, null, null);
            assertEquals(0, count);
            // First connection adds a statement. It is visible to the first
            // connection, but not to the second.
            sc1.addStatement(uriA, uriA, uriA);
            count = countStatements(sc1, null, null, null);
            assertEquals(1, count);
            count = countStatements(sc2, uriA, null, null);
            assertEquals(0, count);
        }
        finally {
            sc2.close();
            sc1.close();
        }
    }

    @Test
    public void testNullContext() throws Exception {
        IRI uriA = sail.getValueFactory().createIRI("http://example.org/test/nullContext#a");
        IRI uriB = sail.getValueFactory().createIRI("http://example.org/test/nullContext#b");
        IRI uriC = sail.getValueFactory().createIRI("http://example.org/test/nullContext#c");

        try (SailConnection sc = getConnection()) {
            long count = countStatements(sc, uriA, null, null, false);
            assertEquals(0, count);
            sc.addStatement(uriA, uriB, uriC);
            Statement statement = sc.getStatements(uriA, uriB, uriC, false, new Resource[]{null}).next();
            Resource context = statement.getContext();
            assertNull(context);
            sc.removeStatements(uriA, null, null);
            assertFalse(sc.getStatements(uriA, uriB, uriC, false, new Resource[]{null}).hasNext());
        }
    }

    // inference ////////////////////////////////////////////////////////////////

    @Ignore
    @Test
    public void testInference() throws Exception {
        if (null != inferencer) {
            IRI uriA = sail.getValueFactory().createIRI("http://example.org/uriA");
            IRI uriB = sail.getValueFactory().createIRI("http://example.org/uriB");
            IRI classX = sail.getValueFactory().createIRI("http://example.org/classX");
            IRI classY = sail.getValueFactory().createIRI("http://example.org/classY");

            try (SailConnection sc = inferencer.getConnection()) {
                sc.begin();
                sc.clear();

                sc.addStatement(classX, RDFS.SUBCLASSOF, classY);
                sc.addStatement(uriA, RDF.TYPE, classX);
                sc.addStatement(uriB, RDF.TYPE, classY);
                commit(sc);

                //showStatements(sc, uriA, RDF.TYPE, null);

                assertEquals(3, countStatements(sc, uriA, RDF.TYPE, null, true));
                assertEquals(1, countStatements(sc, uriA, RDF.TYPE, null, false));
                assertEquals(2, countStatements(sc, uriB, RDF.TYPE, null, true));
                assertEquals(1, countStatements(sc, uriB, RDF.TYPE, null, false));

                if (uniqueStatements) {
                    sc.addStatement(uriA, RDF.TYPE, classY);
                    commit(sc);

                    //showStatements(sc, uriA, RDF.TYPE, null);
                    assertEquals(3, countStatements(sc, uriA, RDF.TYPE, null, true));
                    assertEquals(2, countStatements(sc, uriA, RDF.TYPE, null, false));

                    sc.removeStatements(uriA, RDF.TYPE, classY);
                    commit(sc);

                    assertEquals(3, countStatements(sc, uriA, RDF.TYPE, null, true));
                    assertEquals(2, countStatements(sc, uriA, RDF.TYPE, null, false));

                    //sc.removeStatements(uriA, RDF.TYPE, classX);
                    //commit(sc);
                    //assertEquals(1, countStatements(sc, uriA, RDF.TYPE, null));
                }
            }
        }
    }

// TODO: concurrency testing ///////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////

    private class TestListener implements SailConnectionListener {
        private int added = 0, removed = 0;

        public void statementAdded(final Statement statement) {
            added++;
        }

        public void statementRemoved(final Statement statement) {
            removed++;
        }

        public int getAdded() {
            return added;
        }

        public int getRemoved() {
            return removed;
        }
    }

    protected void showStatements(final SailConnection sc,
                                  final Resource subject,
                                  final IRI predicate,
                                  final Value object,
                                  final Resource... contexts) throws SailException {
        int count = 0;
        try (CloseableIteration<?, SailException> statements
                     = sc.getStatements(subject, predicate, object, true, contexts)) {
            while (statements.hasNext()) {
                System.out.println("" + count + ") " + statements.next());
                count++;
            }
        }
    }

    protected long countStatements(final SailConnection sc,
                                  final Resource subject,
                                  final IRI predicate,
                                  final Value object,
                                  final Resource... contexts) throws SailException {
        return countStatements(sc, subject, predicate, object, false, contexts);
    }
    
    protected long countStatements(final SailConnection sc,
                                  final Resource subject,
                                  final IRI predicate,
                                  final Value object,
                                  final boolean includeInferred,
                                  final Resource... contexts) throws SailException {
        return IterUtils.count(sc.getStatements(subject, predicate, object, includeInferred, contexts));
    }

    private Set<Statement> toSet(final CloseableIteration<? extends Statement, SailException> i) throws SailException {
        try {
            Set<Statement> set = new HashSet<>();
            while (i.hasNext()) {
                set.add(i.next());
            }
            return set;
        } finally {
            i.close();
        }
    }

    private void addFile(final InputStream in,
                         final RDFFormat format) throws Exception {
        Preconditions.checkNotNull(in);

        try {
            try (SailConnection sc = getConnection()) {
                RDFHandler h = new SailAdder(sc);
                RDFParser p = Rio.createParser(format);
                p.setRDFHandler(h);
                p.parse(in, "http://example.org/bogusBaseIRI/");
                commit(sc);
            }
        } finally {
            in.close();
        }
    }
    
    private SailConnection getConnection() {
        SailConnection sc = sail.getConnection();
        sc.begin();
        return sc;
    }
    
    private void commit(final SailConnection conn) {
        conn.commit();
        conn.begin();
    }

    protected void addTestFile() throws Exception {
        addFile(SailTest.class.getResourceAsStream("graph-example-sail-test.trig"), RDFFormat.TRIG);
    }
}
