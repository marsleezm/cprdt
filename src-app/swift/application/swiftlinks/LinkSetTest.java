package swift.application.swiftlinks;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.application.swiftlinks.cprdt.IndexedVoteableSetCPRDT;
import swift.application.swiftlinks.crdt.VoteDirection;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import sys.net.impl.KryoLib;


/**
 * Simple performance benchmark
 * 
 * @author Iwan Briquemont
 *
 */
public class LinkSetTest {
    
    
    public static void main(String args[]) {
        
        TxnHandle txn = TxnTester.createIsolatedTxnTester();
        
        try {
            int numberOfLinks= 5000;
            int votesPerLink = 170;
            IndexedVoteableSetCPRDT<Link,String> links = txn.get(new CRDTIdentifier("A",
                    "Set"), true, IndexedVoteableSetCPRDT.class, false);
            
            for (int i = 0; i < numberOfLinks; i++) {
                Link link = new Link(String.valueOf(i), "test" + String.valueOf(i), "asubreddit", "Unique title of the link "+i,
                        i, false, "http://www.uniqueurl.com/a/dir/"+i, null);
                links.add(link);
                for (int j = 0; j < votesPerLink; j++) {
                    links.vote(link, String.valueOf(j), VoteDirection.UP);
                }
            }
            
            long start = System.currentTimeMillis();
            List<Link> selected = links.applyFind(SortingOrder.TOP, null, null, 25);
            Set<Link> set = new HashSet<Link>(selected);
            links.copyFraction(set);
            long end = System.currentTimeMillis();
            System.out.println("Time: "+(end-start));
            
            start = System.currentTimeMillis();
            links = links.copy();
            end = System.currentTimeMillis();
            System.out.println("Copy time: "+(end-start));
            
            links = links.copyWith(null, null);
            
            Kryo kryo = KryoLib.kryoWithoutAutoreset();
            kryo.reset();
            
            Output buffer = new Output(1 << 10, 1 << 27);
            buffer.clear();
            kryo.writeObject(buffer, links);
            buffer.clear();
            kryo.reset();
            
            start = System.currentTimeMillis();
            kryo.writeObject(buffer, links);
            end = System.currentTimeMillis();
            System.out.println("Serialisation time: "+(end-start));
            System.out.println("Written: " + buffer.position() +" bytes");
            
            Input input = new Input(buffer.getBuffer());
            links = kryo.readObject(input, IndexedVoteableSetCPRDT.class);
            input.rewind();
            kryo.reset();
            start = System.currentTimeMillis();
            links = kryo.readObject(input, IndexedVoteableSetCPRDT.class);
            end = System.currentTimeMillis();
            System.out.println("Deserialisation time: "+(end-start));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
