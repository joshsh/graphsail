package net.fortytwo.tpop.sail;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

class SailAdder implements RDFHandler {
    private final SailConnection c;
    private final Resource[] contexts;

    public SailAdder(final SailConnection c,
                     final Resource... contexts) {
        this.c = c;
        this.contexts = contexts;
    }

    public void startRDF() throws RDFHandlerException {
    }

    public void endRDF() throws RDFHandlerException {
    }

    public void handleNamespace(final String prefix,
                                final String uri) throws RDFHandlerException {
        try {
            c.setNamespace(prefix, uri);
        } catch (SailException e) {
            throw new RDFHandlerException(e);
        }
    }

    public void handleStatement(final Statement s) throws RDFHandlerException {
        try {
            if (1 <= contexts.length) {
                for (Resource x : contexts) {
                    c.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), x);
                }
            } else {
                c.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), s.getContext());
            }
        } catch (SailException e) {
            throw new RDFHandlerException(e);
        }
    }

    public void handleComment(String s) throws RDFHandlerException {
    }
}
