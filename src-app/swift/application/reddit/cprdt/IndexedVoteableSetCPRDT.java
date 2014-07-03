package swift.application.reddit.cprdt;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import swift.application.reddit.Date;
import swift.application.reddit.SortingOrder;
import swift.application.reddit.Thing;
import swift.application.reddit.VoteUtils;
import swift.application.reddit.crdt.VoteCounter;
import swift.application.reddit.crdt.VoteCounterVoteUpdate;
import swift.application.reddit.crdt.VoteDirection;
import swift.application.reddit.crdt.VoteableSet;
import swift.application.reddit.crdt.VoteableSetVoteUpdate;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.cprdt.core.Shard;
import swift.cprdt.core.SortedIndex;
import swift.crdt.AbstractAddWinsSetCRDT;
import swift.crdt.AddWinsUtils;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;

/**
 * Set with a vote counter associated with the elements of the set and indexes
 * to do queries based on the posting date and the votes
 * 
 * @author Iwan Briquemont
 * @param <V>
 *            type of an element
 * @param <U>
 *            type of a voter
 */
public class IndexedVoteableSetCPRDT<V extends Date<V>, U> extends
        AbstractAddWinsSetCRDT<V, IndexedVoteableSetCPRDT<V, U>> implements
        VoteableSet<IndexedVoteableSetCPRDT<V, U>, V, U> {
    protected Map<V, Set<TripleTimestamp>> elemsInstances;
    protected Map<V, VoteCounter<U>> voteCounters;

    // Index related
    // TODO abstract the indexes away somehow
    transient protected SortedIndex<Double, V> confidenceIndex;
    transient protected SortedIndex<Integer, V> scoreIndex;
    transient protected SortedIndex<Double, V> hotnessIndex;
    transient protected SortedIndex<Double, V> controversyIndex;
    transient protected SortedIndex<Long, V> dateIndex;

    private void updateIndexes(V element) {
        if (!contains(element)) {
            return;
        }
        
        checkIndexesExist();

        VoteCounter<U> voteCounter = getVoteCounter(element);

        Double newConfidence = VoteUtils.confidence(voteCounter.getUpvotes(), voteCounter.getDownvotes());
        Integer newScore = voteCounter.getScore();
        Double newHotness = VoteUtils.hotness(element.getDate(), voteCounter.getUpvotes(), voteCounter.getDownvotes());
        Double newControversy = VoteUtils.controversy(voteCounter.getUpvotes(), voteCounter.getDownvotes());

        confidenceIndex.update(newConfidence, element);
        scoreIndex.update(newScore, element);
        hotnessIndex.update(newHotness, element);
        controversyIndex.update(newControversy, element);
        dateIndex.update(element.getDate(), element);
    }

    private void removeFromIndexes(V element) {
        checkIndexesExist();

        confidenceIndex.remove(element);
        scoreIndex.remove(element);
        hotnessIndex.remove(element);
        controversyIndex.remove(element);
        dateIndex.remove(element);
    }

    private void checkIndexesExist() {
        // They either all exist or none
        if (confidenceIndex == null) {
            this.confidenceIndex = new SortedIndex<Double, V>();
            this.scoreIndex = new SortedIndex<Integer, V>();
            this.hotnessIndex = new SortedIndex<Double, V>();
            this.controversyIndex = new SortedIndex<Double, V>();
            this.dateIndex = new SortedIndex<Long, V>();
            rebuildIndexes();
        }
    }

    private void rebuildIndexes() {
        for (V element : elemsInstances.keySet()) {
            updateIndexes(element);
        }
    }

    public IndexedVoteableSetCPRDT() {
    }

    public IndexedVoteableSetCPRDT(CRDTIdentifier id) {
        super(id);
        elemsInstances = new HashMap<V, Set<TripleTimestamp>>();
        voteCounters = new HashMap<V, VoteCounter<U>>();
    }

    private IndexedVoteableSetCPRDT(CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock, Shard shard,
            Map<V, Set<TripleTimestamp>> elemsInstances, Map<V, VoteCounter<U>> voteCounters) {
        super(id, txn, clock, shard);
        this.elemsInstances = elemsInstances;
        this.voteCounters = voteCounters;
    }

    /*
     * private IndexedVoteableSetCPRDT(CRDTIdentifier id, final TxnHandle txn,
     * final CausalityClock clock, Shard shard, Map<V, Set<TripleTimestamp>>
     * elemsInstances, Map<V, VoteCounter<U>> voteCounters, SortedIndex<Double,
     * V> confidenceIndex, SortedIndex<Integer, V> topIndex, SortedIndex<Double,
     * V> hotnessIndex, SortedIndex<Double, V> controversyIndex,
     * SortedIndex<Long, V> newestIndex) { super(id, txn, clock, shard);
     * this.elemsInstances = elemsInstances; this.voteCounters = voteCounters;
     * this.confidenceIndex = confidenceIndex; this.scoreIndex = topIndex;
     * this.hotnessIndex = hotnessIndex; this.controversyIndex =
     * controversyIndex; this.dateIndex = newestIndex; }
     */

    protected VoteCounter<U> getVoteCounter(V element) {
        VoteCounter<U> voteCounter = voteCounters.get(element);
        if (voteCounter == null) {
            voteCounter = new VoteCounter<U>();
            voteCounters.put(element, voteCounter);
        }

        return voteCounter;
    }

    public void vote(V element, U voter, VoteDirection direction) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(element));

        VoteCounter<U> voteCounter = getVoteCounter(element);

        VoteCounterVoteUpdate<U> update = voteCounter.vote(voter, direction);
        registerLocalOperation(new VoteableSetVoteUpdate<IndexedVoteableSetCPRDT<V, U>, V, U>(element, update));

        updateIndexes(element);
    }

    public void applyVote(V element, VoteCounterVoteUpdate<U> voteUpdate) {
        VoteCounter<U> voteCounter = getVoteCounter(element);

        voteUpdate.applyToCounter(voteCounter);

        updateIndexes(element);
    }

    public VoteCounter<U> voteCounterOf(V element) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(element));
        return getVoteCounter(element).copy();
    }

    public void add(V element) throws VersionNotFoundException, NetworkException {
        super.add(element);
        if (contains(element)) {
            updateIndexes(element);
        }
    }

    public void remove(V element) throws VersionNotFoundException, NetworkException {
        super.remove(element);
        if (!contains(element)) {
            removeFromIndexes(element);
        }
    }

    protected void applyAdd(V element, TripleTimestamp instance, Set<TripleTimestamp> overwrittenInstances) {
        super.applyAdd(element, instance, overwrittenInstances);
        if (contains(element)) {
            updateIndexes(element);
        }
    }

    protected void applyRemove(V element, Set<TripleTimestamp> removedInstances) {
        super.applyRemove(element, removedInstances);
        if (!contains(element)) {
            removeFromIndexes(element);
        }
    }

    protected Map<V, Set<TripleTimestamp>> getElementsInstances() {
        return elemsInstances;
    }
    
    protected boolean contains(V element) {
        return getElementsInstances().containsKey(element);
    }

    /**
     * after and before cannot both be set but they can both be null to get the
     * head of the set
     * 
     * @param sort
     * @param after
     * @param before
     * @param limit
     * @return
     */
    public List<V> applyFind(SortingOrder sort, V after, V before, int limit) {
        V fromNotIncluded = (after != null) ? after : before;
        boolean reversed = (before != null);

        List<V> list = Collections.emptyList();
        if ((after != null && !getElementsInstances().containsKey(after))
                || (before != null && !getElementsInstances().containsKey(before))) {
            return list;
        }
        
        checkIndexesExist();

        switch (sort) {
        case CONFIDENCE:
            list = confidenceIndex.find(fromNotIncluded, false, null, false, limit, !reversed);
            break;
        case CONTROVERSIAL:
            list = controversyIndex.find(fromNotIncluded, false, null, false, limit, !reversed);
            break;
        case HOT:
            list = hotnessIndex.find(fromNotIncluded, false, null, false, limit, !reversed);
            break;
        case NEW:
            list = dateIndex.find(fromNotIncluded, false, null, false, limit, !reversed);
            break;
        case OLD:
            list = dateIndex.find(fromNotIncluded, false, null, false, limit, reversed);
            break;
        case TOP:
            list = scoreIndex.find(fromNotIncluded, false, null, false, limit, !reversed);
            break;
        }
        if (reversed) {
            Collections.reverse(list);
        }
        return list;
    }

    public List<V> find(SortingOrder sort, V after, V before) throws VersionNotFoundException,
            NetworkException {
        return find(sort, after, before, Integer.MAX_VALUE);
    }

    public List<V> find(SortingOrder sort, V after, V before, int limit) throws VersionNotFoundException, NetworkException {
        fetch(new IndexedVoteableSetSortedShardQuery<V, U>(sort, after, before, limit));
        return applyFind(sort, after, before, limit);
    }

    @Override
    public IndexedVoteableSetCPRDT<V, U> copyFraction(Set<?> elements) {
        HashMap<V, Set<TripleTimestamp>> elemsInstancesSubset = new HashMap<V, Set<TripleTimestamp>>();

        final HashMap<V, VoteCounter<U>> newCounters = new HashMap<V, VoteCounter<U>>();

        for (V element : (Set<V>) elements) {
            Set<TripleTimestamp> value = elemsInstances.get(element);
            if (value != null) {
                elemsInstancesSubset.put(element, value);
            }
            VoteCounter<U> voteCounter = voteCounters.get(element);
            if (voteCounter != null) {
                newCounters.put(element, voteCounter.copy());
            }
        }

        final HashMap<V, Set<TripleTimestamp>> newInstances = new HashMap<V, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elemsInstancesSubset, newInstances);

        return new IndexedVoteableSetCPRDT<V, U>(id, txn, clock, new Shard(elements), newInstances, newCounters);
    }

    public IndexedVoteableSetCPRDT<V, U> copy() {
        final HashMap<V, Set<TripleTimestamp>> newInstances = new HashMap<V, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elemsInstances, newInstances);

        final HashMap<V, VoteCounter<U>> newCounters = new HashMap<V, VoteCounter<U>>();
        for (Map.Entry<V, VoteCounter<U>> entry : voteCounters.entrySet()) {
            newCounters.put(entry.getKey(), entry.getValue().copy());
        }

        // To check: is it better to copy the indexes or to rebuild them when
        // needed ?
        // (cause when doing a copy at the DC to be sent over the network, it
        // seems wasteful to copy them for nothing)
        return new IndexedVoteableSetCPRDT<V, U>(id, txn, clock, shard, newInstances, newCounters);
        /*
         * return new IndexedVoteableSetCPRDT<V, U>(id, txn, clock, shard,
         * newInstances, newCounters, confidenceIndex.copy(), scoreIndex.copy(),
         * hotnessIndex.copy(), controversyIndex.copy(), dateIndex.copy());
         */
    }

    @Override
    public void mergeSameVersion(IndexedVoteableSetCPRDT<V, U> other) {
        for (Map.Entry<V, Set<TripleTimestamp>> entry : other.elemsInstances.entrySet()) {
            V element = entry.getKey();
            if ((!this.getShard().contains(element)) && other.getShard().contains(element)) {
                elemsInstances.put(element, entry.getValue());
                VoteCounter<U> voteCounter = other.voteCounters.get(element);
                if (voteCounter != null) {
                    voteCounters.put(element, voteCounter);
                }
                updateIndexes(element);
            }

            if ((!this.getShard().contains(entry.getKey())) && (!other.getShard().contains(entry.getKey()))) {
                removeFromIndexes(element);
            }
        }
    }
}
