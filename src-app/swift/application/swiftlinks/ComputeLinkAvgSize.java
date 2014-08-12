package swift.application.swiftlinks;


import com.javamex.classmexer.MemoryUtil;

import swift.application.swiftlinks.cprdt.IndexedVoteableSetCPRDT;
import swift.application.swiftlinks.crdt.VoteDirection;
import swift.crdt.AddWinsSetAddUpdate;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.TxnHandle;


/**
 * Simple test to compute size of link CPRDTs
 * 
 * @author Iwan Briquemont
 *
 */
public class ComputeLinkAvgSize {
    
    
    public static void main(String args[]) {
        
        TxnTester txn = TxnTester.createIsolatedTxnTester();
        
        try {
            CRDTIdentifier id = new CRDTIdentifier("A",
                    "Set");
            int numberOfLinks = 5000;
            int votesPerLink = 170;
            IndexedVoteableSetCPRDT<Link, String> set = txn.get(id, true, IndexedVoteableSetCPRDT.class);
            
            for (int i = 0; i < numberOfLinks; i++) {
                Link link = new Link(String.valueOf(i), "test" + String.valueOf(i), "asubreddit", "Unique title of the link "+i,
                        i, false, "http://www.uniqueurl.com/a/dir/"+i, null);
                set.add(link);
                for (int j = 0; j < votesPerLink; j++) {
                    set.vote(link, String.valueOf(j), VoteDirection.UP);
                }
            }
            
            txn.commit();
            
            set = set.copyWith(null, null);
            
            txn.commit();
            
            long size = MemoryUtil.deepMemoryUsageOf(set);
            
            System.out.println("Total size: " + size);
            System.out.println("Avg size: " + (size / numberOfLinks));
            
            ManagedCRDT<IndexedVoteableSetCPRDT<Link, String>> crdt = txn.getOrCreateVersionedCRDT(id, IndexedVoteableSetCPRDT.class, false);
            
            long managedSize = MemoryUtil.deepMemoryUsageOf(crdt);
            long checkpointSize = MemoryUtil.deepMemoryUsageOf(crdt.getVersion(crdt.getPruneClock(), null));
            
            System.out.println("Total Managed size: " + managedSize);
            System.out.println("Managed size without checkpoint: " + (managedSize - checkpointSize));
            System.out.println("Avg size per update: " + (managedSize - checkpointSize) / crdt.getNumberOfOperations());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
