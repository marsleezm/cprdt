package swift.application.swiftlinks;


import com.javamex.classmexer.MemoryUtil;

import swift.application.swiftlinks.cprdt.IndexedVoteableSetCPRDT;
import swift.application.swiftlinks.cprdt.SortedNode;
import swift.application.swiftlinks.cprdt.VoteableTreeCPRDT;
import swift.application.swiftlinks.crdt.VoteDirection;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;


/**
 * Simple test to compute size of comment tree CPRDTs
 * 
 * @author Iwan Briquemont
 *
 */
public class ComputeCommentAvgSize {
    public static void main(String args[]) {
        
        TxnHandle txn = TxnTester.createIsolatedTxnTester();
        
        try {
            int numberOfComments = 500;
            int votesPerComment = 13;
            VoteableTreeCPRDT<Comment, String> tree = txn.get(new CRDTIdentifier("A",
                    "Set"), true, VoteableTreeCPRDT.class);
            
            for (int i = 0; i < numberOfComments; i++) {
                Comment comment = new Comment("123", String.valueOf(i), "username"+i, 1390, "Unique content of the comment "+i);
                SortedNode<Comment> commentNode = tree.add(tree.getRoot(), comment);
                for (int j = 0; j < votesPerComment; j++) {
                    tree.vote(commentNode, String.valueOf(j), VoteDirection.UP);
                }
            }
            
            tree = tree.copyWith(null, null);
            
            long size = MemoryUtil.deepMemoryUsageOf(tree);
            
            System.out.println("Total size: " + size);
            System.out.println("Avg size: " + (size / numberOfComments));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
