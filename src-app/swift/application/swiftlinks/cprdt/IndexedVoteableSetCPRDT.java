package swift.application.swiftlinks.cprdt;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.application.swiftlinks.Dateable;
import swift.application.swiftlinks.SortingOrder;
import swift.application.swiftlinks.VoteUtils;
import swift.application.swiftlinks.crdt.VoteCounter;
import swift.application.swiftlinks.crdt.VoteCounterVoteUpdate;
import swift.application.swiftlinks.crdt.VoteDirection;
import swift.application.swiftlinks.crdt.VoteableSet;
import swift.application.swiftlinks.crdt.VoteableSetVoteUpdate;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.cprdt.core.Shard;
import swift.cprdt.core.SortedIndex;
import swift.crdt.AbstractAddWinsSetCRDT;
import swift.crdt.AddWinsUtils;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
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
public class IndexedVoteableSetCPRDT<V extends Dateable<V>, U> extends
        AbstractAddWinsSetCRDT<V, IndexedVoteableSetCPRDT<V, U>> implements
        VoteableSet<IndexedVoteableSetCPRDT<V, U>, V, U>,
        KryoSerializable
    {
    protected Map<V, Set<TripleTimestamp>> elemsInstances;
    protected Map<V, VoteCounter<U>> voteCounters;
    // For copy optimisation, make the voteCounters immutable
    // until they are changed -> no need to deep copy
    transient protected Set<V> immutableVoteCounters;

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

        if (!indexesExist()) {
            return;
        }

        ensureIndexesExist();

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
        ensureIndexesExist();

        confidenceIndex.remove(element);
        scoreIndex.remove(element);
        hotnessIndex.remove(element);
        controversyIndex.remove(element);
        dateIndex.remove(element);
    }

    private boolean indexesExist() {
        // They either all exist or none
        return (confidenceIndex != null);
    }

    private void ensureIndexesExist() {
        if (!indexesExist()) {
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

        ensureIndexesExist();
    }

    /*
    private IndexedVoteableSetCPRDT(CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock, Shard shard,
            Map<V, Set<TripleTimestamp>> elemsInstances, Map<V, VoteCounter<U>> voteCounters) {
        super(id, txn, clock, shard);
        this.elemsInstances = elemsInstances;
        this.voteCounters = voteCounters;
        makeImmutable(voteCounters.keySet());
    }*/

    public IndexedVoteableSetCPRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Shard shard,
            HashMap<V, Set<TripleTimestamp>> newInstances, HashMap<V, VoteCounter<U>> newCounters,
            SortedIndex<Double, V> confidenceIndex, SortedIndex<Integer, V> scoreIndex,
            SortedIndex<Double, V> hotnessIndex, SortedIndex<Double, V> controversyIndex, SortedIndex<Long, V> dateIndex) {
        super(id, txn, clock, shard);
        this.elemsInstances = newInstances;
        this.voteCounters = newCounters;
        makeImmutable(voteCounters.keySet());
        this.confidenceIndex = confidenceIndex;
        this.scoreIndex = scoreIndex;
        this.hotnessIndex = hotnessIndex;
        this.controversyIndex = controversyIndex;
        this.dateIndex = dateIndex;
    }

    @Override
    public long estimatedSize() {
        long size = 1;
        size += elemsInstances.size() * 10;
        for (VoteCounter<U> v : voteCounters.values()) {
            size += v.getNumberOfVotes();
        }
        return size;
    }
    
    protected void makeImmutable(Set<V> elements) {
        if (elements == null) {
            return;
        }
        if (immutableVoteCounters == null) {
            immutableVoteCounters = new HashSet<V>(elements);
        } else {
            immutableVoteCounters.addAll(elements);
        }
    }

    protected VoteCounter<U> getVoteCounter(V element) {
        VoteCounter<U> voteCounter = voteCounters.get(element);
        if (voteCounter == null) {
            voteCounter = new VoteCounter<U>();
            voteCounters.put(element, voteCounter);
        }
        
        if (immutableVoteCounters != null && immutableVoteCounters.contains(element)) {
            voteCounter = voteCounter.copy();
            voteCounters.put(element, voteCounter);
            immutableVoteCounters.remove(element);
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

    /**
     * Local vote that does not fetch anything.
     * Only works for the first vote of a user.
     * 
     * @param element
     * @param voter
     * @param direction
     */
    public void voteLazy(V element, U voter, VoteDirection direction) {
        registerLocalOperation(new VoteableSetVoteUpdate<IndexedVoteableSetCPRDT<V, U>, V, U>(element,
                new VoteCounterVoteUpdate<U>(voter, direction, 1)));
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
    
    public void addBlind(V element) {
        super.addBlind(element);
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

        ensureIndexesExist();

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

    public List<V> find(SortingOrder sort, V after, V before) throws VersionNotFoundException, NetworkException {
        return find(sort, after, before, Integer.MAX_VALUE);
    }

    public List<V> find(SortingOrder sort, V after, V before, int limit) throws VersionNotFoundException,
            NetworkException {
        fetch(new IndexedVoteableSetSortedShardQuery<V, U>(sort, after, before, limit));
        return applyFind(sort, after, before, limit);
    }

    @Override
    public IndexedVoteableSetCPRDT<V, U> copyFraction(Set<?> elements) {
        HashMap<V, Set<TripleTimestamp>> elemsInstancesSubset = new HashMap<V, Set<TripleTimestamp>>(3 * elements.size() / 2 + 1);

        final HashMap<V, VoteCounter<U>> newCounters = new HashMap<V, VoteCounter<U>>(3 * elements.size() / 2 + 1);

        int seen = 0;
        @SuppressWarnings("unchecked")
        Set<V> fraction = (Set<V>) elements;

        for (V element : fraction) {
            Set<TripleTimestamp> value = elemsInstances.get(element);
            if (value != null) {
                elemsInstancesSubset.put(element, value);
                seen++;
            }
            VoteCounter<U> voteCounter = voteCounters.get(element);
            if (voteCounter != null) {
                newCounters.put(element, voteCounter);
            }
        }
        makeImmutable(newCounters.keySet());

        final HashMap<V, Set<TripleTimestamp>> newInstances = new HashMap<V, Set<TripleTimestamp>>(3 * elemsInstancesSubset.size() / 2 + 1);
        AddWinsUtils.deepCopy(elemsInstancesSubset, newInstances);

        Shard fractionShard;
        if (seen >= elemsInstances.size()) {
            // The fraction is actually a full copy
            fractionShard = Shard.full;
        } else {
            fractionShard = new Shard(elements);
        }

        ensureIndexesExist();
        return new IndexedVoteableSetCPRDT<V, U>(id, txn, clock, fractionShard, newInstances, newCounters,
                confidenceIndex.copyFraction(fraction), scoreIndex.copyFraction(fraction),
                hotnessIndex.copyFraction(fraction), controversyIndex.copyFraction(fraction),
                dateIndex.copyFraction(fraction));

        // return new IndexedVoteableSetCPRDT<V, U>(id, txn, clock,
        // fractionShard, newInstances, newCounters);
    }

    public IndexedVoteableSetCPRDT<V, U> copy() {
        final HashMap<V, Set<TripleTimestamp>> newInstances = new HashMap<V, Set<TripleTimestamp>>(3 * elemsInstances.size() / 2 + 1);
        AddWinsUtils.deepCopy(elemsInstances, newInstances);

        final HashMap<V, VoteCounter<U>> newCounters = new HashMap<V, VoteCounter<U>>(voteCounters);
        
        makeImmutable(voteCounters.keySet());

        // To check: is it better to copy the indexes or to rebuild them when
        // needed ?
        // (cause when doing a copy at the DC to be sent over the network, it
        // seems wasteful to copy them for nothing)
        //return new IndexedVoteableSetCPRDT<V, U>(id, txn, clock, shard,
        //        newInstances, newCounters);
        
        ensureIndexesExist();
        return new IndexedVoteableSetCPRDT<V, U>(id, txn, clock, shard, newInstances, newCounters,
                confidenceIndex.copy(), scoreIndex.copy(), hotnessIndex.copy(), controversyIndex.copy(),
                dateIndex.copy());
    }

    @Override
    public void mergeSameVersion(IndexedVoteableSetCPRDT<V, U> other) {
        boolean updateIndexes = true;
        if (this.indexesExist() && other.indexesExist()) {
            this.confidenceIndex.merge(other.confidenceIndex);
            this.controversyIndex.merge(other.controversyIndex);
            this.dateIndex.merge(other.dateIndex);
            this.hotnessIndex.merge(other.hotnessIndex);
            this.scoreIndex.merge(other.scoreIndex);
            updateIndexes = false;
        }
        makeImmutable(other.immutableVoteCounters);
        for (Map.Entry<V, Set<TripleTimestamp>> entry : other.elemsInstances.entrySet()) {
            V element = entry.getKey();
            if ((!this.getShard().contains(element)) && other.getShard().contains(element)) {
                elemsInstances.put(element, entry.getValue());
                VoteCounter<U> voteCounter = other.voteCounters.get(element);
                if (voteCounter != null) {
                    voteCounters.put(element, voteCounter);
                }
                if (updateIndexes) {
                    updateIndexes(element);
                }
            }

            if ((!this.getShard().contains(entry.getKey())) && (!other.getShard().contains(entry.getKey()))) {
                removeFromIndexes(element);
            }
        }
    }
    
    /**
     * id
     * shard
     * int number of elements
     * class of elements
     * each element:
     *  element
     *  set of timestamps
     *  the votes
     */

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        this.elemsInstances = new HashMap<V,Set<TripleTimestamp>>();
        this.voteCounters = new HashMap<V,VoteCounter<U>>();
        this.id = kryo.readObject(input, CRDTIdentifier.class);
        this.shard = kryo.readObject(input, Shard.class);
        int numberOfElements = input.readInt(true);
        this.elemsInstances = new HashMap<V,Set<TripleTimestamp>>(3 * numberOfElements / 2 + 1);
        this.voteCounters = new HashMap<V,VoteCounter<U>>(3 * numberOfElements / 2 + 1);
        if (numberOfElements == 0) {
            return;
        }
        Registration c = kryo.readClass(input);
        for (int i = 0; i < numberOfElements; i++) {
            V element = kryo.readObject(input, (Class<V>) c.getType());
            Set<TripleTimestamp> ts = (Set<TripleTimestamp>) kryo.readClassAndObject(input);
            elemsInstances.put(element, ts);
            VoteCounter<U> vc = kryo.readObjectOrNull(input, VoteCounter.class);
            if (vc != null) {
                voteCounters.put(element, vc);
            }
        }
        ensureIndexesExist();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, id);
        kryo.writeObject(output, shard);
        int numberOfElements = elemsInstances.size();
        output.writeInt(elemsInstances.size(), true);
        if (numberOfElements == 0) {
            return;
        }
        kryo.writeClass(output, elemsInstances.keySet().iterator().next().getClass());
        for (Map.Entry<V, Set<TripleTimestamp>> entry: elemsInstances.entrySet()) {
            kryo.writeObject(output, entry.getKey());
            kryo.writeClassAndObject(output, entry.getValue());
            VoteCounter<U> vc = voteCounters.get(entry.getKey());
            kryo.writeObjectOrNull(output, vc, VoteCounter.class);
        }
    }
}
