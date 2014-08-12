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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import swift.application.swiftlinks.crdt.VoteCounterCRDT;
import swift.application.swiftlinks.crdt.VoteDirection;
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
public class VoteCounterConcurrencyTest {
    SwiftTester swift1, swift2;
    ManagedCRDT<VoteCounterCRDT<String>> v1, v2;

    private void mergeV2IntoV1() {
        swift1.merge(v1, v2, swift2);
    }

    private VoteCounterCRDT<String> getLatestVersion(ManagedCRDT<VoteCounterCRDT<String>> v, TxnTester txn) {
        return v.getVersion(v.getClock(), txn);
    }

    private void registerSingleVoteTxn(String voter, VoteDirection direction, ManagedCRDT<VoteCounterCRDT<String>> v, SwiftTester swift) {
        final TxnTester txn = swift.beginTxn(v);
        try {
            final VoteCounterCRDT<String> counter = txn.get(v.getUID(), false, VoteCounterCRDT.class);
            counter.vote(voter, direction);
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
        v1 = new ManagedCRDT<VoteCounterCRDT<String>>(id, new VoteCounterCRDT<String>(id), ClockFactory.newClock(),
                true);
        v2 = new ManagedCRDT<VoteCounterCRDT<String>>(id, new VoteCounterCRDT<String>(id), ClockFactory.newClock(),
                true);
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty vote counter
    @Test
    public void mergeEmpty1() {
        registerSingleVoteTxn("Alice", VoteDirection.UP, v1, swift1);
        mergeV2IntoV1();
        assertEquals(getLatestVersion(v1, swift1.beginTxn()).getVoteOf("Alice"), VoteDirection.UP);
    }

    // Merge with empty vote counter
    @Test
    public void mergeEmpty2() {
        registerSingleVoteTxn("Alice", VoteDirection.UP, v2, swift2);
        mergeV2IntoV1();
        assertEquals(getLatestVersion(v1, swift1.beginTxn()).getVoteOf("Alice"), VoteDirection.UP);
    }

    @Test
    public void mergeNonEmpty() {
        registerSingleVoteTxn("Alice", VoteDirection.UP, v1, swift1);
        registerSingleVoteTxn("Bob", VoteDirection.UP, v2, swift2);
        mergeV2IntoV1();
        assertEquals(getLatestVersion(v1, swift1.beginTxn()).getVoteOf("Alice"), VoteDirection.UP);
        assertEquals(getLatestVersion(v1, swift1.beginTxn()).getVoteOf("Bob"), VoteDirection.UP);
    }

    @Test
    public void mergeConcurrentEqualVotes() {
        registerSingleVoteTxn("Alice", VoteDirection.UP, v1, swift1);
        registerSingleVoteTxn("Alice", VoteDirection.UP, v2, swift2);
        mergeV2IntoV1();
        assertEquals(getLatestVersion(v1, swift1.beginTxn()).getVoteOf("Alice"), VoteDirection.UP);
    }

    @Test
    public void mergeConcurrentDifferentVotes1() {
        registerSingleVoteTxn("Alice", VoteDirection.UP, v1, swift1);
        registerSingleVoteTxn("Alice", VoteDirection.DOWN, v2, swift2);
        mergeV2IntoV1();
        // Highest vote wins
        assertEquals(getLatestVersion(v1, swift1.beginTxn()).getVoteOf("Alice"), VoteDirection.UP);
    }

    @Test
    public void mergeConcurrentDifferentVotes2() {
        registerSingleVoteTxn("Alice", VoteDirection.DOWN, v1, swift1);
        registerSingleVoteTxn("Alice", VoteDirection.UP, v2, swift2);
        mergeV2IntoV1();
        // Highest vote wins
        assertEquals(getLatestVersion(v1, swift1.beginTxn()).getVoteOf("Alice"), VoteDirection.UP);
    }
}
