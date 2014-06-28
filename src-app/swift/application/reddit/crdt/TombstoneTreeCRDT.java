package swift.application.reddit.crdt;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.application.reddit.cprdt.SortedNode;
import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * Tree where you keep removed nodes but as (visible) tombstones
 * 
 * @author Iwan Briquemont
 * @param <V>
 */
public class TombstoneTreeCRDT<V> extends BaseCRDT<TombstoneTreeCRDT<V>> {
    // Non root nodes (including removed nodes)
    private Set<Node<V>> nodes;
    // Quick access to children of node
    private Map<Node<V>, Set<Node<V>>> children;
    // Removed nodes
    private Set<Node<V>> tombstones;
    // The root node (parent is null)
    private Node<V> root;
    // To search nodes by their value
    private Map<V, Set<Node<V>>> nodesByValue;

    // Kryo
    public TombstoneTreeCRDT() {
    }

    public TombstoneTreeCRDT(CRDTIdentifier id) {
        super(id);
        this.root = new Node<V>(null, null);
        this.nodes = new HashSet<Node<V>>();
        this.tombstones = new HashSet<Node<V>>();
        this.children = new HashMap<Node<V>, Set<Node<V>>>();
        this.nodesByValue = new HashMap<V, Set<Node<V>>>();
    }

    private TombstoneTreeCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Set<Node<V>> nodes, Set<Node<V>> tombstones) {
        super(id, txn, clock);
        this.root = new Node<V>(null, null);
        this.nodes = new HashSet<Node<V>>(nodes);
        this.tombstones = new HashSet<Node<V>>(tombstones);
        this.children = new HashMap<Node<V>, Set<Node<V>>>();
        this.nodesByValue = new HashMap<V, Set<Node<V>>>();
        for (Node<V> node: nodes) {
            addToChildren(node);
            addToNodesWithValue(node);
        }
    }
    
    public void add(Node<V> newNode) {
        if (newNode.getParent() == null) {
            return;
        }
        if (nodes.contains(newNode)) {
            return;
        }
        if (!(nodes.contains(newNode.getParent()) || this.root.equals(newNode.getParent()))) {
            return;
        }
        nodes.add(newNode);
        addToNodesWithValue(newNode);
        addToChildren(newNode);
        registerLocalOperation(new TombstoneTreeAddUpdate<V>(newNode));
    }

    public void add(Node<V> parent, V nodeValue) {
        Node<V> node = new Node<V>(parent, nodeValue);
        add(node);
    }

    public void applyAdd(Node<V> node) {
        nodes.add(node);
        addToNodesWithValue(node);
        addToChildren(node);
    }
    
    private void addToChildren(Node<V> node) {
        Set<Node<V>> childrenOfParent = children.get(node.getParent());
        if (childrenOfParent == null) {
            childrenOfParent = new HashSet<Node<V>>();
            children.put(node.getParent(), childrenOfParent);
        }
        childrenOfParent.add(node);
    }
    
    private void addToNodesWithValue(Node<V> node) {
        Set<Node<V>> nodes = nodesByValue.get(node.getValue());
        if (nodes == null) {
            nodes = new HashSet<Node<V>>();
            nodesByValue.put(node.getValue(), nodes);
        }
        nodes.add(node);
    }
    
    /**
     * "Soft" remove, does not actually remove anything from the set
     */
    public void remove(Node<V> node) {
        if (!nodes.contains(node)) {
            return;
        }
        tombstones.add(node);
        registerLocalOperation(new TombstoneTreeRemoveUpdate<V>(node));
    }
    
    public void applyRemove(Node<V> node) {
        tombstones.add(node);
    }
    
    /**
     * Number of nodes in tree including tombstones
     */
    public int size() {
        return nodes.size();
    }
    
    /**
     * Is this node in the tree (if not removed) ?
     */
    public boolean has(Node<V> node) {
        return nodes.contains(node) && !tombstones.contains(node);
    }
    
    /**
     * @param node
     * @return Node is in tree (removed or not) ?
     */
    public boolean isAdded(Node<V> node) {
        return nodes.contains(node);
    }
    
    /**
     * Is the node tagged as removed
     */
    public boolean isRemoved(Node<V> node) {
        return tombstones.contains(node);
    }

    @Override
    public Set<DecoratedNode<Node<V>,V>> getValue() {
        HashSet<DecoratedNode<Node<V>,V>> value = new HashSet<DecoratedNode<Node<V>,V>>();
        for (Node<V> node: nodes) {
            value.add(new DecoratedNode<Node<V>,V>(node, tombstones.contains(node)));
        }
        return value;
    }
    
    public Node<V> getRoot() {
        return this.root;
    }
    
    public Set<Node<V>> allChildrenOf(Node<V> node) {
        return Collections.unmodifiableSet(children.get(node));
    }
    
    public Set<DecoratedNode<Node<V>,V>> decoratedChildrenOf(Node<V> node) {
        HashSet<DecoratedNode<Node<V>,V>> decoratedChildren = new HashSet<DecoratedNode<Node<V>,V>>();
        for (Node<V> n: children.get(node)) {
            decoratedChildren.add(new DecoratedNode<Node<V>,V>(n, tombstones.contains(node)));
        }
        return decoratedChildren;
    }
    
    public Set<Node<V>> getNodesByValue(V value) {
        return Collections.unmodifiableSet(nodesByValue.get(value));
    }

    @Override
    public TombstoneTreeCRDT<V> copy() {
        return new TombstoneTreeCRDT<V>(id, txn, clock, new HashSet<Node<V>>(nodes), new HashSet<Node<V>>(tombstones));
    }
}
