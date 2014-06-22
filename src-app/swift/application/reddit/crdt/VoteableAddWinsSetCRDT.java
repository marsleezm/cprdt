package swift.application.reddit.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.cprdt.core.Shard;
import swift.crdt.AbstractAddWinsSetCRDT;
import swift.crdt.AddWinsUtils;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.VersionNotFoundException;

/**
 * Set with a vote counter associated with the elements of the set
 * 
 * @author Iwan Briquemont
 * @param <V> type of an element
 * @param <U> type of a voter
 */
public class VoteableAddWinsSetCRDT<V,U> extends AbstractAddWinsSetCRDT<V,VoteableAddWinsSetCRDT<V,U>> implements VoteableSet<VoteableAddWinsSetCRDT<V,U>, V, U> {
    protected Map<V, Set<TripleTimestamp>> elemsInstances;
    protected Map<V,VoteCounter<U>> voteCounters;
    
    public VoteableAddWinsSetCRDT() {
    }

    public VoteableAddWinsSetCRDT(CRDTIdentifier id) {
        super(id);
        elemsInstances = new HashMap<V, Set<TripleTimestamp>>();
        voteCounters = new HashMap<V,VoteCounter<U>>();
    }
    
    private VoteableAddWinsSetCRDT(CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock, Shard shard,
            Map<V, Set<TripleTimestamp>> elemsInstances, Map<V,VoteCounter<U>> voteCounters) {
        super(id, txn, clock, shard);
        this.elemsInstances = elemsInstances;
        this.voteCounters = voteCounters;
    }
    
    protected VoteCounter<U> getVoteCounter(V element) {
        VoteCounter<U> voteCounter = voteCounters.get(element);
        if (voteCounter == null) {
            voteCounter = new VoteCounter<U>();
            voteCounters.put(element, voteCounter);
        }
        
        return voteCounter;
    }
    
    public void vote(V element, U voter, VoteDirection direction) {
        VoteCounter<U> voteCounter = getVoteCounter(element);
        VoteCounterVoteUpdate<U> update = voteCounter.vote(voter, direction);
        registerLocalOperation(new VoteableSetVoteUpdate<VoteableAddWinsSetCRDT<V,U>, V, U>(element, update));
    }
    
    public void applyVote(V element, VoteCounterVoteUpdate<U> voteUpdate) {
        VoteCounter<U> voteCounter = getVoteCounter(element);
        voteUpdate.applyToCounter(voteCounter);
    }
    
    protected Map<V, Set<TripleTimestamp>> getElementsInstances() {
        return elemsInstances;
    }
    
    public VoteableAddWinsSetCRDT<V, U> copy() {
        final HashMap<V, Set<TripleTimestamp>> newInstances = new HashMap<V, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elemsInstances, newInstances);
        
        final HashMap<V, VoteCounter<U>> newCounters = new HashMap<V, VoteCounter<U>>();
        for (Map.Entry<V, VoteCounter<U>> entry: voteCounters.entrySet()) {
            newCounters.put(entry.getKey(), entry.getValue().copy());
        }
        
        return new VoteableAddWinsSetCRDT<V,U>(id, txn, clock, getShard(), newInstances, newCounters);
    }

    @Override
    public VoteCounter<U> voteCounterOf(V element) {
        return getVoteCounter(element).copy();
    }
}
