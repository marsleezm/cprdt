package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.CRDTIdentifier;
import swift.crdt.SetIntegers;
import swift.crdt.interfaces.TxnHandle;

public class SetTest {
    TxnHandle txn;
    SetIntegers i;

    @Before
    public void setUp() {
        txn = new TxnHandleForTesting("client1", ClockFactory.newClock());
        // Unchecked casts like the following seem to be unavoidable in Java 1.6
        // unless we apply some rather complex scheme as given in
        // http://gafter.blogspot.com/search?q=super+type+token
        i = txn.get(new CRDTIdentifier("A", "Int"), true, SetIntegers.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void emptyTest() {
        // lookup on empty set
        assertTrue(!i.lookup(0));
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void insertTest() {
        int v = 5;
        int w = 7;
        // insert one element
        i.insert(v);
        assertTrue(i.lookup(v));
        assertTrue(!i.lookup(w));

        // insertion should be idempotent
        i.insert(v);
        assertTrue(i.lookup(v));
    }

    @Test
    public void deleteTest() {
        int v = 5;
        int w = 7;
        i.insert(v);
        i.insert(w);

        i.remove(v);
        assertTrue(!i.lookup(v));
        assertTrue(i.lookup(w));

        // remove should be idempotent
        i.remove(v);
        assertTrue(!i.lookup(v));
        assertTrue(i.lookup(w));

        i.remove(w);
        assertTrue(!i.lookup(v));
        assertTrue(!i.lookup(w));
    }
}