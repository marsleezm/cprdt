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
package swift.application.reddit.crdt;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import swift.crdt.TxnTester;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.SwiftException;

public class VoteCounterTest {
    TxnHandle txn;
    VoteCounterCRDT<String> voteCounter;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        voteCounter = txn.get(new CRDTIdentifier("A", "Vote"), true, VoteCounterCRDT.class);
    }

    @Test
    public void initTest() {
        assertTrue(voteCounter.getValue().isEmpty());
    }

    @Test
    public void noVoteTest() {
        // vote on empty counter
        assertEquals(voteCounter.getVoteOf("Alice"), VoteDirection.MIDDLE);
    }

    @Test
    public void voteTest() {
        voteCounter.vote("Alice", VoteDirection.UP);
        assertEquals(voteCounter.getScore(), 1);
        assertEquals(voteCounter.getUpvotes(), 1);
        assertEquals(voteCounter.getDownvotes(), 0);
        assertEquals(voteCounter.getVoteOf("Alice"), VoteDirection.UP);
        voteCounter.vote("Bob", VoteDirection.DOWN);
        voteCounter.vote("Charlie", VoteDirection.UP);
        assertEquals(voteCounter.getScore(), 1);
        assertEquals(voteCounter.getUpvotes(), 2);
        assertEquals(voteCounter.getDownvotes(), 1);
    }
    
    @Test
    public void voteChangeTest() {
        voteCounter.vote("Alice", VoteDirection.UP);
        voteCounter.vote("Bob", VoteDirection.DOWN);
        voteCounter.vote("Charlie", VoteDirection.UP);
        voteCounter.vote("Alice", VoteDirection.MIDDLE);
        voteCounter.vote("Bob", VoteDirection.UP);
        voteCounter.vote("Charlie", VoteDirection.UP);
        
        assertEquals(voteCounter.getVoteOf("Alice"), VoteDirection.MIDDLE);
        assertEquals(voteCounter.getVoteOf("Bob"), VoteDirection.UP);
        assertEquals(voteCounter.getVoteOf("Charlie"), VoteDirection.UP);
        
        assertEquals(voteCounter.getScore(), 2);
        assertEquals(voteCounter.getUpvotes(), 2);
        assertEquals(voteCounter.getDownvotes(), 0);
    }
}
