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
    private Map<V,VoteDirection> votes;
    private Map<V,Long> voteTimestamps;
    // For caching purposes, can be computed from votes
    private int upvotes;
    private int downvotes;

    public VoteCounter() {
        votes = new HashMap<V,VoteDirection>();
        voteTimestamps = new HashMap<V,Long>();
    }

    private VoteCounter(Map<V,VoteDirection> votes, Map<V,Long> voteTimestamps, int upvotes, int downvotes) {
        this.votes = votes;
        this.voteTimestamps = voteTimestamps;
        this.upvotes = upvotes;
        this.downvotes = downvotes;
    }
    
    private long getTimestamp(V voter) {
        long timestamp = 0;
        if (voteTimestamps.containsKey(voter)) {
            timestamp = voteTimestamps.get(voter);
        }
        
        return timestamp;
    }
    
    private void setTimestamp(V voter, long timestamp) {
        voteTimestamps.put(voter, timestamp);
    }
    
    // To keep the upvotes and downvotes variables consistent with the actual votes
    private void updateScore(V voter, VoteDirection newDirection) {
        VoteDirection oldDirection = getVoteOf(voter);
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

    public VoteCounterVoteUpdate<V> vote(V voter, VoteDirection direction) {
        long voterTimestamp = getTimestamp(voter);
        voterTimestamp++;
        setTimestamp(voter, voterTimestamp);
        
        updateScore(voter, direction);
        votes.put(voter, direction);
        
        return new VoteCounterVoteUpdate<V>(voter, direction, voterTimestamp);
    }

    public void applyVote(V voter, VoteDirection direction, long newTimestamp) {
        long oldTimestamp = getTimestamp(voter);
        if (oldTimestamp > newTimestamp) {
            // Update is older than current state
            return;
        }
        
        if (oldTimestamp == newTimestamp) {
            VoteDirection oldDirection = votes.get(voter);
            // Higher vote wins
            if (oldDirection.direction >= direction.direction) {
                // No need to update direction
                return;
            }
        }
        
        updateScore(voter, direction);
        votes.put(voter, direction);
        
        setTimestamp(voter, newTimestamp);
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
        VoteDirection direction = votes.get(voter);
        if (direction == null) {
            direction = VoteDirection.MIDDLE;
        }
        return direction;
    }
    
    public Map<V,VoteDirection> getValue() {
        return Collections.unmodifiableMap(votes);
    }
    
    @Override
    public VoteCounter<V> copy() {
        return new VoteCounter<V>(new HashMap<V,VoteDirection>(votes), new HashMap<V,Long>(voteTimestamps), upvotes, downvotes);
    }

}
