/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.application.swiftlinks.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.application.swiftlinks.crdt.RemoveOnceSetCRDT;
import swift.clocks.ClockFactory;
import swift.crdt.SwiftTester;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * 
 * @author Iwan Briquemont
 *
 */
public class RemoveOnceSetConcurrencyTest {
    SwiftTester swift1, swift2;
    ManagedCRDT<RemoveOnceSetCRDT<Integer>> i1, i2;

    private void mergeI2IntoI1() {
        swift1.merge(i1, i2, swift2);
    }

    private RemoveOnceSetCRDT<Integer> getLatestVersion(ManagedCRDT<RemoveOnceSetCRDT<Integer>> i, TxnTester txn) {
        return i.getVersion(i.getClock(), txn);
    }

    private void registerSingleAddTxn(int value, ManagedCRDT<RemoveOnceSetCRDT<Integer>> i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(i);
        try {
            final RemoveOnceSetCRDT<Integer> set = txn.get(i.getUID(), false, RemoveOnceSetCRDT.class);
            set.add(value);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
    }

    private void registerSingleRemoveTxn(int value, ManagedCRDT<RemoveOnceSetCRDT<Integer>> i, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(i);
        try {
            final RemoveOnceSetCRDT<Integer> set = txn.get(i.getUID(), false, RemoveOnceSetCRDT.class);
            set.remove(value);
        } catch (WrongTypeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
        } finally {
            txn.commit();
        }
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        final CRDTIdentifier id = new CRDTIdentifier("a", "a");
        i1 = new ManagedCRDT<RemoveOnceSetCRDT<Integer>>(id, new RemoveOnceSetCRDT<Integer>(id), ClockFactory.newClock(),
                true);
        i2 = new ManagedCRDT<RemoveOnceSetCRDT<Integer>>(id, new RemoveOnceSetCRDT<Integer>(id), ClockFactory.newClock(),
                true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerSingleAddTxn(5, i1, swift1);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).lookup(5));
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerSingleAddTxn(5, i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleAddTxn(5, i1, swift1);
        registerSingleAddTxn(6, i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).lookup(5));
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).lookup(6));
    }

    @Test
    public void mergeConcurrentInsert() {
        registerSingleAddTxn(5, i1, swift1);
        registerSingleAddTxn(5, i2, swift2);
        mergeI2IntoI1();
        assertTrue(getLatestVersion(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeConcurrentRemove() {
        registerSingleRemoveTxn(5, i1, swift1);
        registerSingleRemoveTxn(5, i2, swift2);
        mergeI2IntoI1();
        assertTrue(!getLatestVersion(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerSingleAddTxn(5, i1, swift1);

        registerSingleRemoveTxn(5, i2, swift2);
        mergeI2IntoI1();
        assertTrue(!getLatestVersion(i1, swift1.beginTxn()).lookup(5));

        registerSingleRemoveTxn(5, i1, swift1);
        assertTrue(!getLatestVersion(i1, swift1.beginTxn()).lookup(5));
    }

    @Test
    public void mergeConcurrentCausal() {
        registerSingleAddTxn(5, i1, swift1);
        registerSingleRemoveTxn(5, i1, swift1);
        
        // Cannot add again
        registerSingleAddTxn(5, i2, swift2);
        mergeI2IntoI1();
        // TesterUtils.printInformtion(i1, swift1.beginTxn());
        assertTrue(!getLatestVersion(i1, swift1.beginTxn()).lookup(5));
    }
}
