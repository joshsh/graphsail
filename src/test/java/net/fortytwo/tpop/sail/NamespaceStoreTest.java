package net.fortytwo.tpop.sail;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NamespaceStoreTest extends GraphSailTestBase {
    private static final String EX_PREFIX = "ex";
    private static final String EX_NAME = "http://example.org/";

    private NamespaceStore namespaces;

    @Before
    public void setUp() {
        namespaces = new NamespaceStore();
    }

    @Test
    public void addAndGetNameSpaceIsSuccessful() {
        assertEquals(0, countNamespaces());
        namespaces.set(EX_PREFIX, EX_NAME);
        assertEquals(1, countNamespaces());
        assertEquals(EX_NAME, namespaces.get(EX_PREFIX));
        assertEquals(EX_PREFIX, namespaces.getAll().next().getPrefix());
        assertEquals(EX_NAME, namespaces.getAll().next().getName());
        namespaces.set(EX_PREFIX + "2", EX_NAME + "2");
        assertEquals(2, countNamespaces());
    }

    @Test(expected = NullPointerException.class)
    public void addNullPrefixFails() {
        namespaces.set(null, EX_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void addNullNameFails() {
        namespaces.set(EX_PREFIX, null);
    }

    @Test
    public void nameForNonexistentPrefixIsNull() {
        assertNull(namespaces.get("foo"));
    }
    @Test
    public void getAllFromEmptyStoreIsTrivial() {
        assertEquals(0, countNamespaces());
    }

    @Test(expected = NullPointerException.class)
    public void getNullPrefixFails() {
        namespaces.get(null);
    }

    @Test
    public void emptyPrefixIsAllowed() {
        assertNull(namespaces.get(""));
        namespaces.set("", EX_NAME);
        assertEquals(EX_NAME, namespaces.get(""));
        namespaces.set("", EX_NAME + "2");
        assertEquals(EX_NAME + "2", namespaces.get(""));
    }

    @Test
    public void emptyNameIsAllowed() {
        namespaces.set(EX_PREFIX, "");
        assertEquals("", namespaces.get(EX_PREFIX));
    }

    @Test
    public void duplicateNamesAreAllowed() {
        namespaces.set(EX_PREFIX, EX_NAME);
        namespaces.set(EX_PREFIX + "2", EX_NAME);
        assertEquals(2, countNamespaces());
        assertEquals(EX_NAME, namespaces.get(EX_PREFIX));
        assertEquals(EX_NAME, namespaces.get(EX_PREFIX + "2"));
    }

    @Test
    public void duplicatePrefixOverridesPreviousName() {
        namespaces.set(EX_PREFIX, EX_NAME);
        assertEquals(EX_NAME, namespaces.get(EX_PREFIX));
        namespaces.set(EX_PREFIX, EX_NAME + "2");
        assertEquals(EX_NAME + "2", namespaces.get(EX_PREFIX));
        assertEquals(1, countNamespaces());
    }

    @Test
    public void prefixesAndNamesAreTrimmed() {
        namespaces.set(" " + EX_PREFIX, EX_NAME + "  \t");
        assertEquals(EX_NAME, namespaces.get(EX_PREFIX));
    }

    private int countNamespaces() {
        return countIterator(namespaces.getAll());
    }
}
