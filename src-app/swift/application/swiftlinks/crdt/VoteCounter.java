package swift.application.swiftlinks.crdt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.crdt.core.Copyable;

/**
 * Count votes (upvotes and downvotes) of voters of type V
 * 
 * Each vote is a LWW register
 * Concurrent votes -> higher vote wins
 * 
 * @author Iwan Briquemont
 * @param <V> type of a voter
 */
public class VoteCounter<V> implements Copyable, KryoSerializable {
    private Map<V,VoteEntry> votes;
    // For caching purposes, can be computed from votes
    private int upvotes;
    private int downvotes;
    
    private static class VoteEntry {
        private VoteDirection direction;
        private long timestamp;
        
        // For Kryo
        VoteEntry() {
        }
        
        VoteEntry(VoteDirection direction, long timestamp) {
            this.direction = direction;
            this.timestamp = timestamp;
        }
    }

    public VoteCounter() {
        votes = new HashMap<V,VoteEntry>();
    }

    private VoteCounter(Map<V,VoteEntry> votes, int upvotes, int downvotes) {
        this.votes = votes;
        this.upvotes = upvotes;
        this.downvotes = downvotes;
    }
    
    // To keep the upvotes and downvotes variables consistent with the actual votes
    private void updateScore(VoteDirection oldDirection, VoteDirection newDirection) {
        if (oldDirection != newDirection) {
            switch (newDirection) {
                case UP:
                    upvotes++;
                    break;
                case DOWN:
                    downvotes++;
                    break;
                default:
                    break;
            }
            switch (oldDirection) {
                case UP:
                    upvotes--;
                    break;
                case DOWN:
                    downvotes--;
                    break;
                default:
                    break;
            }
        }
    }
    
    protected VoteEntry getEntry(V voter) {
        VoteEntry entry = votes.get(voter);
        if (entry == null) {
            entry = new VoteEntry(VoteDirection.MIDDLE, 0);
            votes.put(voter, entry);
        }
        return entry;
    }

    public VoteCounterVoteUpdate<V> vote(V voter, VoteDirection direction) {
        VoteEntry entry = getEntry(voter);
        
        updateScore(entry.direction, direction);
        
        entry = new VoteEntry(direction, entry.timestamp + 1);
        votes.put(voter, entry);
        
        return new VoteCounterVoteUpdate<V>(voter, direction, entry.timestamp);
    }

    public void applyVote(V voter, VoteDirection direction, long newTimestamp) {
        VoteEntry entry = getEntry(voter);
        if (entry.timestamp > newTimestamp) {
            // Update is older than current state
            return;
        }
        
        if (entry.timestamp == newTimestamp) {
            // Higher vote wins
            if (entry.direction.direction >= direction.direction) {
                // No need to update direction
                return;
            }
        }
        
        updateScore(entry.direction, direction);
        
        entry = new VoteEntry(direction, newTimestamp);
        votes.put(voter, entry);
    }
    
    public int getScore() {
        return upvotes - downvotes;
    }
    
    public int getUpvotes() {
        return upvotes;
    }
    
    public int getDownvotes() {
        return downvotes;
    }
    
    public int getNumberOfVotes() {
        return votes.size();
    }

    public VoteDirection getVoteOf(V voter) {
        VoteEntry entry = votes.get(voter);
        if (entry == null) {
            return VoteDirection.MIDDLE;
        }
        return entry.direction;
    }
    
    public Map<V,VoteDirection> getValue() {
        Map<V, VoteDirection> value = new HashMap<V, VoteDirection>();
        for (Map.Entry<V, VoteEntry> entry: votes.entrySet()) {
            value.put(entry.getKey(), entry.getValue().direction);
        }
        return Collections.unmodifiableMap(value);
    }
    
    @Override
    public VoteCounter<V> copy() {
        Map<V, VoteEntry> copy = new HashMap<V, VoteEntry>(votes);
        
        return new VoteCounter<V>(copy, upvotes, downvotes);
    }
    
    /**
     * Optimized serialization
     * 
     * int upvotes
     * int downvotes
     * int number of votes
     * class of V
     * for each vote
     *      V voter
     *      direction
     *      long timestamp
     */

    @Override
    public void read(Kryo kryo, Input input) {
        upvotes = 0;
        downvotes = 0;
        int numberOfVotes = input.readInt(true);
        votes = new HashMap<V,VoteEntry>(3 * numberOfVotes / 2 + 1);
        if (numberOfVotes == 0) {
            return;
        }
        Registration c = kryo.readClass(input);
        for (int i = 0; i < numberOfVotes; i++) {
            V voter = (V) kryo.readObject(input, c.getType());
            byte dir = input.readByte();
            if (dir > 0) {
                upvotes += dir;
            } else {
                if (dir < 0) {
                    downvotes -= dir;
                }
            }
            VoteEntry entry = new VoteEntry(VoteDirection.valueOf(dir), input.readLong(true));
            votes.put(voter, entry);
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        int numberOfVotes = votes.size();
        output.writeInt(numberOfVotes, true);
        if (numberOfVotes == 0) {
            return;
        }
        kryo.writeClass(output, votes.keySet().iterator().next().getClass());
        for (Map.Entry<V, VoteEntry> entry: votes.entrySet()) {
            kryo.writeObject(output, entry.getKey());
            VoteEntry ve = entry.getValue();
            output.writeByte(ve.direction.direction);
            output.writeLong(ve.timestamp, true);
        }
    }

}
