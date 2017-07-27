package net.fortytwo.tpop.sail;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GetAndRetrieveStatementsTest extends GraphSailTestBase {

    @Test
    public void firstTest() {
        createConnection();

        assertEquals(0, countStatements());
        connection.addStatement(RDF.TYPE, RDF.TYPE, RDF.TYPE);
        assertEquals(1, countStatements());
    }

    @Test
    public void testTmp() {
        for (int i = 0; i < 10; i++) {
            BNode node = graphSail.getValueFactory().createBNode();
            System.out.println("node: " + node.stringValue() + " " + node.getID() + " " + node.toString());
        }
    }
}
