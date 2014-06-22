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
package swift.application.reddit.cprdt;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import swift.application.reddit.Link;
import swift.application.reddit.SortingOrder;
import swift.application.reddit.cprdt.IndexedVoteableSetCPRDT;
import swift.application.reddit.crdt.VoteDirection;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;

public class VoteableSetTest {
    TxnHandle txn;
    IndexedVoteableSetCPRDT<Link,String> set;
    
    // 500 links, oldest first
    List<Link> links;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        links = new ArrayList<Link>();
        set = txn.get(new CRDTIdentifier("A", "Vote"), true, IndexedVoteableSetCPRDT.class);
        
        // 28/04/2014
        long date = 1398725435;
        for (int i = 1; i <= 500; i++) {
            Link link = new Link(""+i, "Me", "dev", "Post number "+i, date + (i * 10), true, null, "Post content");
            links.add(link);
        }
    }
    
    private void fillSet() throws VersionNotFoundException, NetworkException {
        for (Link link: links) {
            set.add(link);
        }
    }

    @Test
    public void initTest() throws VersionNotFoundException, NetworkException {
        assertTrue(!set.lookup(links.get(0)));
    }

    @Test
    public void dateIndexTest() throws VersionNotFoundException, NetworkException {
        int i;
        fillSet();
        set.remove(links.get(10));
        set.remove(links.get(12));
        set.add(links.get(12));
        set.add(links.get(10));
        
        i = 499;
        for (Link link: set.applyFind(SortingOrder.NEW, null, null, Integer.MAX_VALUE)) {
            assertTrue(link.equals(links.get(i)));
            i--;
        }
        
        i = 99;
        // 150 links starting at the 100th link, oldest first
        List<Link> result = set.applyFind(SortingOrder.OLD, links.get(98), null, 150);
        assertEquals(150, result.size());
        for (Link link: result) {
            assertTrue(link.equals(links.get(i)));
            i++;
        }
        
        i = 450;
        // 150 links starting at the 451st link, oldest first
        result = set.applyFind(SortingOrder.OLD, links.get(449), null, 150);
        assertEquals(50, result.size());
        for (Link link: result) {
            assertTrue(link.equals(links.get(i)));
            i++;
        }
        
        i = 0;
        // All links before the 101th link, oldest first (=> from 0 to 100)
        result = set.applyFind(SortingOrder.OLD, null, links.get(100), Integer.MAX_VALUE);
        assertEquals(100, result.size());
        for (Link link: result) {
            assertTrue(link.equals(links.get(i)));
            i++;
        }
        
        // Removed links are removed from the index
        set.remove(links.get(0));
        result = set.applyFind(SortingOrder.OLD, null, links.get(1), Integer.MAX_VALUE);
        assertEquals(0, result.size());
    }
    
    @Test
    public void topIndexTest() throws VersionNotFoundException, NetworkException {
        int i;
        fillSet();
        
        // Vote on the links so that older links have more votes
        i = 500;
        for (Link link: links) {
            for (int j = 1; j < i; j++) {
                try {
                    set.vote(link, "User"+j, VoteDirection.UP);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (int j = i; j < 500; j++) {
                try {
                    set.vote(link, "User"+j, VoteDirection.DOWN);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            i--;
        }
        
        i = 0;
        try {
            for (Link link: set.find(SortingOrder.TOP, null, null)) {
                assertTrue(link.equals(links.get(i)));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
