package swift.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import swift.crdt.AddOnlySetCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * Test of SnapshotIsolationTxnHandle
 * 
 * @author Iwan Briquemont
 *
 */
public class TxnHandleTest {
    SwiftSessionTester session;
    
    @Before
    public void setUp() {
        session = new SwiftSessionTester();
    }
    
    @Test
    public void lazyCreateTest() {
        CRDTIdentifier setId = new CRDTIdentifier("Set", "1");
        try {
            TxnHandle txn = session.beginTxn(false);
            boolean objectNotFound = false;
            
            txn.get(setId, false, AddOnlySetCRDT.class, true);
            // Check that object is not yet created
            assertFalse(txn.objectIsFound(setId, AddOnlySetCRDT.class));
            txn.rollback();
            
            txn = session.beginTxn(false);
            // Create object
            txn.get(setId, true, AddOnlySetCRDT.class, true);
            txn.commit();
            
            txn = session.beginTxn(false);
            // Check that object was indeed created (with laziness)
            txn.get(setId, false, AddOnlySetCRDT.class, true);
                
            assertTrue(txn.objectIsFound(setId, AddOnlySetCRDT.class));
            txn.commit();
            
        } catch (SwiftException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    /**
     * Test with a CPRDT
     */
    public void addOnlySetTest() {
        CRDTIdentifier setId = new CRDTIdentifier("Set", "1");
        try {
            TxnHandle txn = session.beginTxn(false);
            
            txn = session.beginTxn(false);
            // Create object
            txn.get(setId, true, AddOnlySetCRDT.class, true);
            txn.commit();
            
            // With laziness
            txn = session.beginTxn(false);
            AddOnlySetCRDT<String> set = txn.get(setId, true, AddOnlySetCRDT.class, true);
            set.add("1");
            assertTrue(set.has("1"));
            assertFalse(set.has("2"));
            for (String element: set.getFullSet()) {
                assertEquals(element, "1");
            }
            assertTrue(set.has("1"));
            assertFalse(set.has("2"));
            txn.commit();
            
            // Without laziness
            txn = session.beginTxn(false);
            set = txn.get(setId, true, AddOnlySetCRDT.class, false);
            assertTrue(set.has("1"));
            assertFalse(set.has("2"));
            txn.commit();
            
            // With lazyness
            txn = session.beginTxn(false);
            set = txn.get(setId, true, AddOnlySetCRDT.class, true);
            txn.fetch(setId, AddOnlySetCRDT.class, Collections.singleton("1"));
            assertTrue(set.getShard().contains("1"));
            assertTrue(set.has("1"));
            txn.commit();
        } catch (SwiftException e) {
            e.printStackTrace();
        }
    }
}
