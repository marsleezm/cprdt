package swift.application.reddit.crdt;

import java.util.Set;

import swift.crdt.core.CRDTUpdate;

public class VoteCounterVoteUpdate<V> implements CRDTUpdate<VoteCounterCRDT<V>> {
    private V voter;
    private VoteDirection direction;
    private long timestamp;

    // Kryo
    public VoteCounterVoteUpdate() {
    }

    public VoteCounterVoteUpdate(V voter, VoteDirection direction, long timestamp) {
        this.voter = voter;
        this.direction = direction;
        this.timestamp = timestamp;
    }

    @Override
    public void applyTo(VoteCounterCRDT<V> crdt) {
        crdt.applyVote(this);
    }
    
    public void applyToCounter(VoteCounter<V> counter) {
        counter.applyVote(voter, direction, timestamp);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return null;
    }

    @Override
    public Set<Object> affectedParticles() {
        return null;
    }
}
