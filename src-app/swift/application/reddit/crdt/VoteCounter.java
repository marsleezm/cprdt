package swift.application.reddit.crdt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import swift.crdt.core.Copyable;

/**
 * Count votes (upvotes and downvotes) of voters of type V
 * 
 * Concurrent votes -> higher vote wins
 * 
 * @author Iwan Briquemont
 * @param <V> type of a voter
 */
public class VoteCounter<V> implements Copyable {
    private Map<V,VoteEntry> votes;
    // For caching purposes, can be computed from votes
    private int upvotes;
    private int downvotes;
    
    private static class VoteEntry {
        VoteDirection direction;
        long timestamp;
        
        VoteEntry(VoteDirection direction, long timestamp) {
            this.direction = direction;
            this.timestamp = timestamp;
        }

        public VoteCounter.VoteEntry copy() {
            return new VoteEntry(direction, timestamp);
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
        
        entry.timestamp++;
        entry.direction = direction;
        
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
        
        entry.direction = direction;
        
        entry.timestamp = newTimestamp;
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
        Map<V, VoteEntry> copy = new HashMap<V, VoteEntry>();
        for (Map.Entry<V, VoteEntry> entry: votes.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }
        return new VoteCounter<V>(copy, upvotes, downvotes);
    }

}
