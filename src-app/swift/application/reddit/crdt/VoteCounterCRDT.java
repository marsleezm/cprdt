package swift.application.reddit.crdt;

import java.util.Map;

import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * Count votes (upvotes and downvotes) of voters of type V
 * 
 * Concurrent votes -> higher vote wins
 * 
 * @author Iwan Briquemont
 * @param <V> type of a voter
 */
public class VoteCounterCRDT<V> extends BaseCRDT<VoteCounterCRDT<V>> {
    private VoteCounter<V> voteCounter;

    // Kryo
    public VoteCounterCRDT() {
    }

    public VoteCounterCRDT(CRDTIdentifier id) {
        super(id);
        voteCounter = new VoteCounter<V>();
    }

    private VoteCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, VoteCounter<V> voteCounter) {
        super(id, txn, clock);
        this.voteCounter = voteCounter;
    }

    public void vote(V voter, VoteDirection direction) {
        registerLocalOperation(voteCounter.vote(voter, direction));
    }

    public void applyVote(VoteCounterVoteUpdate<V> update) {
        update.applyToCounter(voteCounter);
    }
    
    public int getScore() {
        return voteCounter.getScore();
    }
    
    public int getUpvotes() {
        return voteCounter.getUpvotes();
    }
    
    public int getDownvotes() {
        return voteCounter.getDownvotes();
    }

    public VoteDirection getVoteOf(V voter) {
        return voteCounter.getVoteOf(voter);
    }

    @Override
    public Map<V,VoteDirection> getValue() {
        return voteCounter.getValue();
    }

    @Override
    public VoteCounterCRDT<V> copy() {
        return new VoteCounterCRDT<V>(id, txn, clock, voteCounter.copy());
    }

}
