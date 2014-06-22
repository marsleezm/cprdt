package swift.application.reddit.cprdt;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;

import swift.application.reddit.Date;
import swift.application.reddit.SortingOrder;
import swift.application.reddit.VoteUtils;
import swift.application.reddit.crdt.DecoratedNode;
import swift.application.reddit.crdt.VoteCounter;
import swift.application.reddit.crdt.VoteCounterVoteUpdate;
import swift.application.reddit.crdt.VoteDirection;
import swift.application.reddit.crdt.VoteableSet;
import swift.application.reddit.crdt.VoteableSetVoteUpdate;
import swift.clocks.CausalityClock;
import swift.cprdt.core.Shard;
import swift.cprdt.core.SortedIndex;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.VersionNotFoundException;

/**
 * Voteable tree where you keep removed nodes but as (visible) tombstones
 * 
 * @author Iwan Briquemont
 * @param <V>
 */
public class VoteableTreeCPRDT<V extends Date<V>, U> extends BaseCRDT<VoteableTreeCPRDT<V, U>> implements
        VoteableSet<VoteableTreeCPRDT<V, U>, SortedNode<V>, U> {
    // Non root nodes (including removed nodes)
    private Set<SortedNode<V>> nodes;
    // Quick access to children of node
    private Map<SortedNode<V>, Set<SortedNode<V>>> children;
    // Removed nodes
    private Set<SortedNode<V>> tombstones;
    // The root node (parent is null)
    private SortedNode<V> root;
    // To search nodes by their value
    private Map<V, Set<SortedNode<V>>> nodesByValue;

    // Votes
    protected Map<SortedNode<V>, VoteCounter<U>> voteCounters;

    // Index related
    // TODO abstract the indexes away somehow
    // Parent -> sorted children
    transient protected Map<SortedNode<V>, SortedIndex<Double, SortedNode<V>>> confidenceIndex;
    transient protected Map<SortedNode<V>, SortedIndex<Integer, SortedNode<V>>> scoreIndex;
    transient protected Map<SortedNode<V>, SortedIndex<Double, SortedNode<V>>> hotnessIndex;
    transient protected Map<SortedNode<V>, SortedIndex<Double, SortedNode<V>>> controversyIndex;
    transient protected Map<SortedNode<V>, SortedIndex<Long, SortedNode<V>>> dateIndex;

    private void updateIndexes(SortedNode<V> node) {
        if (!nodes.contains(node)) {
            return;
        }

        checkIndexesExist();

        VoteCounter<U> voteCounter = getVoteCounter(node);

        Double newConfidence = VoteUtils.confidence(voteCounter.getUpvotes(), voteCounter.getDownvotes());
        Integer newScore = voteCounter.getScore();
        Double newHotness = VoteUtils.hotness(node.getValue().getDate(), voteCounter.getUpvotes(),
                voteCounter.getDownvotes());
        Double newControversy = VoteUtils.controversy(voteCounter.getUpvotes(), voteCounter.getDownvotes());

        getConfidenceIndexFor(node.getParent()).update(newConfidence, node);
        getScoreIndexFor(node.getParent()).update(newScore, node);
        getHotnessIndexFor(node.getParent()).update(newHotness, node);
        getControversyIndexFor(node.getParent()).update(newControversy, node);
        getDateIndexFor(node.getParent()).update(node.getValue().getDate(), node);
    }

    private SortedIndex<Double, SortedNode<V>> getConfidenceIndexFor(SortedNode<V> node) {
        SortedIndex<Double, SortedNode<V>> index = this.confidenceIndex.get(node);
        if (index == null) {
            index = new SortedIndex<Double, SortedNode<V>>();
            this.confidenceIndex.put(node, index);
        }
        return index;
    }

    private SortedIndex<Integer, SortedNode<V>> getScoreIndexFor(SortedNode<V> node) {
        SortedIndex<Integer, SortedNode<V>> index = this.scoreIndex.get(node);
        if (index == null) {
            index = new SortedIndex<Integer, SortedNode<V>>();
            this.scoreIndex.put(node, index);
        }
        return index;
    }

    private SortedIndex<Double, SortedNode<V>> getHotnessIndexFor(SortedNode<V> node) {
        SortedIndex<Double, SortedNode<V>> index = this.hotnessIndex.get(node);
        if (index == null) {
            index = new SortedIndex<Double, SortedNode<V>>();
            this.hotnessIndex.put(node, index);
        }
        return index;
    }

    private SortedIndex<Double, SortedNode<V>> getControversyIndexFor(SortedNode<V> node) {
        SortedIndex<Double, SortedNode<V>> index = this.controversyIndex.get(node);
        if (index == null) {
            index = new SortedIndex<Double, SortedNode<V>>();
            this.controversyIndex.put(node, index);
        }
        return index;
    }

    private SortedIndex<Long, SortedNode<V>> getDateIndexFor(SortedNode<V> node) {
        SortedIndex<Long, SortedNode<V>> index = this.dateIndex.get(node);
        if (index == null) {
            index = new SortedIndex<Long, SortedNode<V>>();
            this.dateIndex.put(node, index);
        }
        return index;
    }

    private void checkIndexesExist() {
        // They either all exist or none
        if (confidenceIndex == null) {
            rebuildIndexes();
        }
    }

    private void rebuildIndexes() {
        this.confidenceIndex = new HashMap<SortedNode<V>, SortedIndex<Double, SortedNode<V>>>();
        this.scoreIndex = new HashMap<SortedNode<V>, SortedIndex<Integer, SortedNode<V>>>();
        this.hotnessIndex = new HashMap<SortedNode<V>, SortedIndex<Double, SortedNode<V>>>();
        this.controversyIndex = new HashMap<SortedNode<V>, SortedIndex<Double, SortedNode<V>>>();
        this.dateIndex = new HashMap<SortedNode<V>, SortedIndex<Long, SortedNode<V>>>();
        for (SortedNode<V> node : nodes) {
            updateIndexes(node);
        }
    }

    // Kryo
    public VoteableTreeCPRDT() {
    }

    public VoteableTreeCPRDT(CRDTIdentifier id) {
        super(id);
        this.root = new SortedNode<V>(null, null);
        this.nodes = new HashSet<SortedNode<V>>();
        this.tombstones = new HashSet<SortedNode<V>>();
        this.voteCounters = new HashMap<SortedNode<V>, VoteCounter<U>>();
        this.children = new HashMap<SortedNode<V>, Set<SortedNode<V>>>();
        this.nodesByValue = new HashMap<V, Set<SortedNode<V>>>();
    }

    private VoteableTreeCPRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Shard shard,
            Set<SortedNode<V>> nodes, Set<SortedNode<V>> tombstones, Map<SortedNode<V>, VoteCounter<U>> voteCounters) {
        super(id, txn, clock, shard);
        this.root = new SortedNode<V>(null, null);
        this.nodes = new HashSet<SortedNode<V>>(nodes);
        this.tombstones = new HashSet<SortedNode<V>>(tombstones);
        this.voteCounters = voteCounters;
        this.children = new HashMap<SortedNode<V>, Set<SortedNode<V>>>();
        this.nodesByValue = new HashMap<V, Set<SortedNode<V>>>();
        for (SortedNode<V> node : nodes) {
            addToChildren(node);
            addToNodesWithValue(node);
        }
    }

    public void add(SortedNode<V> newNode) throws VersionNotFoundException, NetworkException {
        if (newNode.getParent() == null) {
            return;
        }
        if (!newNode.getParent().isRoot()) {
            // We can blindly add a node if we know that its parent exist
            // but we won't know if it's really present as it could have been already deleted
            fetch(Collections.singleton(newNode.getParent()));
        }
        
        if (nodes.contains(newNode)) {
            // To avoid creating unnecessary updates
            return;
        }
        if (!(nodes.contains(newNode.getParent()) || newNode.getParent().isRoot())) {
            return;
        }
        
        nodes.add(newNode);
        addToNodesWithValue(newNode);
        addToChildren(newNode);
        registerLocalOperation(new VoteableTreeAddUpdate<V, U>(newNode));
    }

    public SortedNode<V> add(SortedNode<V> parent, V nodeValue) throws VersionNotFoundException, NetworkException {
        SortedNode<V> node = new SortedNode<V>(parent, nodeValue);
        add(node);
        return node;
    }

    public void applyAdd(SortedNode<V> node) {
        nodes.add(node);
        addToNodesWithValue(node);
        addToChildren(node);
    }

    private void addToChildren(SortedNode<V> node) {
        Set<SortedNode<V>> childrenOfParent = children.get(node.getParent());
        if (childrenOfParent == null) {
            childrenOfParent = new HashSet<SortedNode<V>>();
            children.put(node.getParent(), childrenOfParent);
        }
        childrenOfParent.add(node);
    }

    private void addToNodesWithValue(SortedNode<V> node) {
        Set<SortedNode<V>> nodes = nodesByValue.get(node.getValue());
        if (nodes == null) {
            nodes = new HashSet<SortedNode<V>>();
            nodesByValue.put(node.getValue(), nodes);
        }
        nodes.add(node);
    }

    /**
     * Tombstone remove, does not actually remove anything from the set
     * @throws NetworkException 
     * @throws VersionNotFoundException 
     */
    public void remove(SortedNode<V> node) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(node));
        
        if (!nodes.contains(node)) {
            // Must be added before being removed
            return;
        }
        tombstones.add(node);
        registerLocalOperation(new VoteableTreeRemoveUpdate<V, U>(node));
    }

    public void applyRemove(SortedNode<V> node) {
        tombstones.add(node);
    }

    /**
     * Number of nodes in (this part of) the tree including tombstones
     */
    public int size() {
        return nodes.size();
    }

    /**
     * Is this node in the tree (if not removed) ?
     * @throws NetworkException 
     * @throws VersionNotFoundException 
     */
    public boolean has(SortedNode<V> node) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(node));
        return nodes.contains(node) && !tombstones.contains(node);
    }

    /**
     * @param node
     * @return Node is in tree (removed or not) ?
     * @throws NetworkException 
     * @throws VersionNotFoundException 
     */
    public boolean isAdded(SortedNode<V> node) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(node));
        return nodes.contains(node);
    }

    /**
     * Is the node tagged as removed
     * @throws NetworkException 
     * @throws VersionNotFoundException 
     */
    public boolean isRemoved(SortedNode<V> node) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(node));
        return tombstones.contains(node);
    }

    @Override
    public Set<DecoratedNode<V>> getValue() {
        HashSet<DecoratedNode<V>> value = new HashSet<DecoratedNode<V>>();
        for (SortedNode<V> node : nodes) {
            value.add(new DecoratedNode<V>(node, tombstones.contains(node)));
        }
        return value;
    }

    public SortedNode<V> getRoot() {
        return this.root;
    }
    
    public Set<SortedNode<V>> allChildrenOf(SortedNode<V> node) throws VersionNotFoundException, NetworkException {
        fetch(new VoteableTreeChildrenShardQuery<V,U>(node));
        return applyAllChildrenOf(node);
    }

    public Set<SortedNode<V>> applyAllChildrenOf(SortedNode<V> node) {
        Set<SortedNode<V>> allChildren = children.get(node);
        if (allChildren == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(allChildren);
    }
    
    public SortedSet<SortedIndex<Comparable, SortedNode<V>>.Entry> allChildrenOf(SortedNode<V> node, SortingOrder sort) throws VersionNotFoundException, NetworkException {
        fetch(new VoteableTreeChildrenShardQuery<V,U>(node));
        return applyAllChildrenOf(node, sort);
    }

    public SortedSet<SortedIndex<Comparable, SortedNode<V>>.Entry> applyAllChildrenOf(SortedNode<V> node, SortingOrder sort) {
        checkIndexesExist();
        
        SortedIndex<Comparable, SortedNode<V>> index;
        boolean reversed = true;
        switch (sort) {
        case CONFIDENCE:
            index = (SortedIndex) confidenceIndex.get(node);
            break;
        case CONTROVERSIAL:
            index = (SortedIndex) controversyIndex.get(node);
            break;
        case HOT:
            index = (SortedIndex) hotnessIndex.get(node);
            break;
        case NEW:
            index = (SortedIndex) dateIndex.get(node);
            break;
        case OLD:
            index = (SortedIndex) dateIndex.get(node);
            reversed = false;
            break;
        case TOP:
            index = (SortedIndex) scoreIndex.get(node);
            break;
        default:
            return null;
        }
        if (index == null) {
            return null;
        }

        return index.entrySet(reversed);
    }

    public Set<DecoratedNode<V>> decoratedChildrenOf(SortedNode<V> node) {
        HashSet<DecoratedNode<V>> decoratedChildren = new HashSet<DecoratedNode<V>>();
        for (SortedNode<V> n : children.get(node)) {
            decoratedChildren.add(new DecoratedNode<V>(n, tombstones.contains(node)));
        }
        return decoratedChildren;
    }

    public Set<SortedNode<V>> getNodesByValue(V value) {
        return Collections.unmodifiableSet(nodesByValue.get(value));
    }

    @Override
    public void vote(SortedNode<V> node, U voter, VoteDirection direction) throws VersionNotFoundException,
            NetworkException {
        fetch(Collections.singleton(node));

        if (!nodes.contains(node)) {
            return;
        }

        VoteCounter<U> voteCounter = getVoteCounter(node);

        VoteCounterVoteUpdate<U> update = voteCounter.vote(voter, direction);
        registerLocalOperation(new VoteableSetVoteUpdate<VoteableTreeCPRDT<V, U>, SortedNode<V>, U>(node, update));

        updateIndexes(node);
    }

    @Override
    public void applyVote(SortedNode<V> node, VoteCounterVoteUpdate<U> voteUpdate) {
        VoteCounter<U> voteCounter = getVoteCounter(node);

        voteUpdate.applyToCounter(voteCounter);

        updateIndexes(node);
    }

    @Override
    public VoteCounter<U> voteCounterOf(SortedNode<V> node) throws VersionNotFoundException, NetworkException {
        return getVoteCounter(node).copy();
    }

    protected VoteCounter<U> getVoteCounter(SortedNode<V> node) {
        VoteCounter<U> voteCounter = voteCounters.get(node);
        if (voteCounter == null) {
            voteCounter = new VoteCounter<U>();
            voteCounters.put(node, voteCounter);
        }

        return voteCounter;
    }

    @Override
    public VoteableTreeCPRDT<V, U> copy() {
        final HashMap<SortedNode<V>, VoteCounter<U>> newCounters = new HashMap<SortedNode<V>, VoteCounter<U>>();
        for (Map.Entry<SortedNode<V>, VoteCounter<U>> entry : voteCounters.entrySet()) {
            newCounters.put(entry.getKey(), entry.getValue().copy());
        }
        return new VoteableTreeCPRDT<V, U>(id, txn, clock, shard, new HashSet<SortedNode<V>>(nodes),
                new HashSet<SortedNode<V>>(tombstones), newCounters);
    }

    @Override
    public VoteableTreeCPRDT<V, U> copyFraction(Set<?> fraction) {
        HashSet<SortedNode<V>> nodesSubset = new HashSet<SortedNode<V>>();
        HashSet<SortedNode<V>> tombstonesSubset = new HashSet<SortedNode<V>>();

        final HashMap<SortedNode<V>, VoteCounter<U>> newCounters = new HashMap<SortedNode<V>, VoteCounter<U>>();

        for (SortedNode<V> node : (Set<SortedNode<V>>) fraction) {
            if (nodes.contains(node)) {
                nodesSubset.add(node);
            }
            if (tombstones.contains(node)) {
                tombstonesSubset.add(node);
            }
            VoteCounter<U> voteCounter = voteCounters.get(node);
            if (voteCounter != null) {
                newCounters.put(node, voteCounter.copy());
            }
        }

        return new VoteableTreeCPRDT<V, U>(id, txn, clock, shard, nodesSubset, tombstonesSubset, newCounters);
    }

    public List<SortedNode<V>> applySortedSubtree(SortedNode<V> node, int context, SortingOrder sort, int limit) {
        // TODO actually build a tree instead of a list
        if (nodes.size() == 0) {
            return Collections.emptyList();
        }

        List<SortedNode<V>> list = new LinkedList<SortedNode<V>>();

        PriorityQueue<SortedIndex<Comparable, SortedNode<V>>.Entry> candidates = new PriorityQueue<SortedIndex<Comparable, SortedNode<V>>.Entry>();
        SortedSet<SortedIndex<Comparable, SortedNode<V>>.Entry> nodeChildren = applyAllChildrenOf(node, sort);
        if (nodeChildren != null) {
            candidates.addAll(nodeChildren);
        }

        SortedNode<V> ancestor = node.getParent();
        int i = 0;
        while (ancestor != null && i < context) {
            nodeChildren = applyAllChildrenOf(ancestor, sort);
            if (nodeChildren != null) {
                candidates.addAll(nodeChildren);
            }
        }

        while (candidates.size() < limit && !candidates.isEmpty()) {
            SortedIndex<Comparable, SortedNode<V>>.Entry bestEntry = candidates.poll();
            list.add(bestEntry.getValue());
            nodeChildren = applyAllChildrenOf(bestEntry.getValue(), sort);
            if (nodeChildren != null) {
                candidates.addAll(nodeChildren);
            }
        }

        return list;
    }

    public List<SortedNode<V>> sortedSubtree(SortedNode<V> node, int context, SortingOrder sort, int limit)
            throws VersionNotFoundException, NetworkException {
        fetch(new VoteableTreeSortedShardQuery<V,U>(node, context, sort, limit));
        return applySortedSubtree(node, context, sort, limit);
    }

    @Override
    public void mergeSameVersion(VoteableTreeCPRDT<V, U> other) {
        for (SortedNode<V> node : other.nodes) {
            if (!this.getShard().contains(node) && other.getShard().contains(node)) {
                nodes.add(node);
                VoteCounter<U> voteCounter = other.voteCounters.get(node);
                if (voteCounter != null) {
                    voteCounters.put(node, voteCounter);
                }
                addToChildren(node);
                addToNodesWithValue(node);
                updateIndexes(node);
            }
        }
    }
}
