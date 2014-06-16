package swift.cprdt.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;


public class ShardTest {
    
    @Test
    public void fullShardTest() {
        Shard shard = Shard.full;
        assertTrue(shard.contains("Test"));
        assertTrue(shard.containsAll(Collections.singleton("Test")));
        assertTrue(shard.containsAny(Collections.singleton("Test")));
        assertTrue(shard.isFull());
        assertFalse(shard.isHollow());
    }
    
    @Test
    public void hollowShardTest() {
        Shard shard = Shard.hollow;
        assertFalse(shard.contains("Test"));
        assertFalse(shard.containsAll(Collections.singleton("Test")));
        assertFalse(shard.containsAny(Collections.singleton("Test")));
        assertFalse(shard.isFull());
        assertTrue(shard.isHollow());
    }
    
    @Test
    public void setShardTest() {
        Set<String> oneAndTwo = new HashSet<String>();
        oneAndTwo.add("1");
        oneAndTwo.add("2");
        Set<String> oneAndTwoAndThree = new HashSet<String>();
        oneAndTwoAndThree.add("1");
        oneAndTwoAndThree.add("2");
        oneAndTwoAndThree.add("3");
        Set<String> twoAndThree = new HashSet<String>();
        twoAndThree.add("2");
        twoAndThree.add("3");
        
        Shard oneAndTwoShard = new Shard(oneAndTwo);
        assertTrue(oneAndTwoShard.contains("1"));
        assertTrue(oneAndTwoShard.contains("2"));
        assertTrue(oneAndTwoShard.containsAll(oneAndTwo));
        assertTrue(oneAndTwoShard.containsAny(oneAndTwoAndThree));
        assertTrue(oneAndTwoShard.containsAny(twoAndThree));
        
        assertFalse(oneAndTwoShard.contains("3"));
        assertFalse(oneAndTwoShard.containsAll(oneAndTwoAndThree));
        assertFalse(oneAndTwoShard.containsAll(twoAndThree));
    }
    
    @Test
    public void intervalShardTest() {
        Shard oneToThree = new Shard(0, 3);
        assertFalse(oneToThree.contains(0));
        assertTrue(oneToThree.contains(1));
        assertTrue(oneToThree.contains(2));
        assertTrue(oneToThree.contains(3));
        assertTrue(oneToThree.containsInterval(0, 3));
        
        Shard oneToThreeAndFourToFive = oneToThree.union(new Shard(3, 5));
        assertTrue(oneToThreeAndFourToFive.contains(1));
        assertTrue(oneToThreeAndFourToFive.contains(4));
        assertTrue(oneToThreeAndFourToFive.contains(5));
        assertTrue(oneToThreeAndFourToFive.containsInterval(0, 5));
        assertFalse(oneToThreeAndFourToFive.contains(0));
        assertFalse(oneToThreeAndFourToFive.contains(6));
        
        // Check commutativity
        oneToThreeAndFourToFive = (new Shard(3, 5)).union(oneToThree);
        assertTrue(oneToThreeAndFourToFive.contains(1));
        assertTrue(oneToThreeAndFourToFive.contains(4));
        assertTrue(oneToThreeAndFourToFive.contains(5));
        assertTrue(oneToThreeAndFourToFive.containsInterval(0, 5));
        assertFalse(oneToThreeAndFourToFive.contains(0));
        assertFalse(oneToThreeAndFourToFive.contains(6));
        
        
        Shard oneToThreeAndFiveToSix = oneToThree.union(new Shard(4, 6));
        assertTrue(oneToThreeAndFiveToSix.containsInterval(4, 6));
        assertFalse(oneToThreeAndFiveToSix.containsInterval(0, 6));
        
        Shard oneToSix = oneToThreeAndFiveToSix.union(new Shard(3, 4));
        assertTrue(oneToSix.containsInterval(4, 6));
        assertTrue(oneToSix.containsInterval(0, 6));
    }
    
    @Test
    public void unionTest() {
        Set<String> oneAndTwo = new HashSet<String>();
        oneAndTwo.add("1");
        oneAndTwo.add("2");
        Set<String> twoAndThree = new HashSet<String>();
        twoAndThree.add("2");
        twoAndThree.add("3");
        
        Shard someShard = new Shard(Collections.singleton((Object)"1"));
        Shard otherShard = new Shard(Collections.singleton((Object)"2"));
        Shard union = Shard.hollow.union(Shard.full);
        assertTrue(union.isFull());
        union = Shard.full.union(Shard.hollow);
        assertTrue(union.isFull());
        union = Shard.full.union(Shard.full);
        assertTrue(union.isFull());
        union = Shard.hollow.union(Shard.hollow);
        assertTrue(union.isHollow());
        
        union = someShard.union(otherShard);
        assertTrue(union.contains("1"));
        assertTrue(union.contains("2"));
        assertTrue(union.containsAll(oneAndTwo));
        assertTrue(union.containsAny(oneAndTwo));
        assertTrue(union.containsAny(twoAndThree));
        assertFalse(union.containsAll(twoAndThree));
    }
}
