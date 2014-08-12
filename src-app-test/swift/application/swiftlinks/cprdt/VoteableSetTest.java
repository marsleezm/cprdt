package swift.application.swiftlinks.cprdt;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import swift.application.swiftlinks.Link;
import swift.application.swiftlinks.SortingOrder;
import swift.application.swiftlinks.cprdt.IndexedVoteableSetCPRDT;
import swift.application.swiftlinks.crdt.VoteDirection;
import swift.cprdt.core.Shard;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;

/**
 * Test of IndexedVoteableSetCPRDT
 * 
 * @author Iwan Briquemont
 *
 */
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
    
    @Test
    public void copyFractionTest() throws VersionNotFoundException, NetworkException {
        fillSet();
        
        IndexedVoteableSetCPRDT<Link,String> copy = set.copyFraction(Collections.singleton(links.get(0)));
        for (Link elem: copy.getValue()) {
            assertTrue(elem.equals(links.get(0)));
        }
    }
    
    @Test
    public void mergeTest() throws VersionNotFoundException, NetworkException {
        fillSet();
        
        IndexedVoteableSetCPRDT<Link,String> hollowCopy = set.copyFraction(Collections.emptySet());
        
        assertTrue(hollowCopy.getShard().isHollow());
        
        hollowCopy.mergeSameVersion(set);
        hollowCopy.setShard(Shard.full);
        Set<Link> copyLinks = hollowCopy.getValue();
        for (Link link: links) {
            assertTrue(copyLinks.contains(link));
        }
    }
}
