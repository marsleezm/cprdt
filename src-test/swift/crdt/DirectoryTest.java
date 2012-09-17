package swift.crdt;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class DirectoryTest {
    TxnHandle txn;
    DirectoryTxnLocal dir;

    @Before
    public void setUp() throws SwiftException {
        txn = new TxnTester("client1", ClockFactory.newClock());
        dir = txn.get(DirectoryTxnLocal.createRootId("root", DirectoryVersioned.class), true, DirectoryVersioned.class);
    }

    @Test
    public void idTest() {
        assertTrue(dir.id.equals(new CRDTIdentifier("DIR", "root:swift.crdt.DirectoryVersioned")));
    }

    @Test
    public void initTest() {
        assertTrue(dir.getValue().isEmpty());
    }

    @Test
    public void emptyTest() {
        // lookup on empty set
        assertTrue(!dir.contains("x", IntegerVersioned.class));
        assertTrue(dir.getValue().isEmpty());
    }

    @Test
    public void insertTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        // create one element
        dir.createNewEntry("x", IntegerVersioned.class);
        IntegerTxnLocal i2 = dir.get("x", IntegerVersioned.class);
        assertTrue(i2.getValue() == 0);
        assertTrue(dir.contains("x", IntegerVersioned.class));
    }

    @Test
    public void removeTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        // create one element
        dir.createNewEntry("x", IntegerVersioned.class);
        assertTrue(dir.contains("x", IntegerVersioned.class));

        dir.removeEntry("x", IntegerVersioned.class);
        assertFalse(dir.contains("x", IntegerVersioned.class));
    }

    @Test
    public void removeRecursiveTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        DirectoryTxnLocal parent = dir;
        for (int i = 0; i < 5; i++) {
            CRDTIdentifier childDirId = parent.createNewEntry("x" + i, DirectoryVersioned.class);
            parent.createNewEntry("y" + i, DirectoryVersioned.class);
            parent.createNewEntry("z" + i, IntegerVersioned.class);

            DirectoryTxnLocal child = txn.get(childDirId, false, DirectoryVersioned.class);
            parent = child;
        }
        dir.removeEntry("x" + 0, DirectoryVersioned.class);
        assertFalse(dir.contains("x" + 0, DirectoryVersioned.class));
        assertTrue(dir.contains("y" + 0, DirectoryVersioned.class));

        // Test directly one of the subdirectories to be empty
        CRDTIdentifier subdirId = new CRDTIdentifier(DirectoryTxnLocal.dirTable,
                "root/x0/x1/x2/x3/x4:swift.crdt.DirectoryVersioned");
        DirectoryTxnLocal subdir = txn.get(subdirId, false, DirectoryVersioned.class);
        assertTrue(subdir.getValue().isEmpty());
    }

    @Test
    public void differentEntryTypesTest() {
        dir.createNewEntry("x", IntegerVersioned.class);
        assertTrue(dir.contains("x", IntegerVersioned.class));
        dir.createNewEntry("x", DirectoryVersioned.class);
        assertTrue(dir.contains("x", DirectoryVersioned.class));

        dir.removeEntry("x", IntegerVersioned.class);
        assertFalse(dir.contains("x", IntegerVersioned.class));
        assertTrue(dir.contains("x", DirectoryVersioned.class));
    }
}
