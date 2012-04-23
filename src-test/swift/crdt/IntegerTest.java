package swift.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.SwiftException;

public class IntegerTest {
    TxnHandle txn;
    IntegerTxnLocal i;

    @Before
    public void setUp() throws SwiftException {
        txn = new TxnTester("client1", ClockFactory.newClock());
        i = txn.get(new CRDTIdentifier("A", "Int"), true, IntegerVersioned.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue() == 0);
    }

    @Test
    public void addTest() {
        final int incr = 10;
        i.add(incr);
        assertTrue(incr == i.getValue());
    }

    @Test
    public void addTest2() {
        final int incr = 10;
        for (int j = 0; j < incr; j++) {
            i.add(1);
        }
        assertTrue(incr == i.getValue());
    }

    @Test
    public void subTest() {
        final int decr = 10;
        i.sub(decr);
        assertTrue(decr == -i.getValue());
    }

    @Test
    public void subTest2() {
        final int decr = 10;
        for (int j = 0; j < decr; j++) {
            i.sub(1);
        }
        assertTrue(decr == -i.getValue());
    }

    @Test
    public void addAndSubTest() {
        final int incr = 10;
        final int iterations = 5;
        for (int j = 0; j < iterations; j++) {
            i.add(incr);
            i.sub(incr);
            assertTrue(0 == i.getValue());
        }
    }
}
