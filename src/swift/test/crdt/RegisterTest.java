package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.CRDTIdentifier;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class RegisterTest {
    TxnHandle txn;
    RegisterTxnLocal<IntegerWrap> i;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        txn = new TxnTester("client1", ClockFactory.newClock());
        i = (RegisterTxnLocal<IntegerWrap>) txn.get(new CRDTIdentifier("A", "Int"), true, RegisterVersioned.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue() == null);
    }

    @Test
    public void setTest() {
        final int incr = 10;
        i.set(new IntegerWrap(incr));
        assertTrue(incr == i.getValue().i);
    }

    @Test
    public void getAndSetTest() {
        final int iterations = 5;
        for (int j = 0; j < iterations; j++) {
            i.set(new IntegerWrap(j));
            assertTrue(j == i.getValue().i);
        }
    }
}
