package net.fortytwo.tpop.sail;

interface Schema {
    enum VertexLabel {IRI, BNode, Literal}

    interface VertexProperties {
        String DATATYPE = "datatype";
        String LANGUAGE = "language";
        String VALUE = "value";
    }

    interface EdgeProperties {
        String CONTEXT = "context";
    }
}
