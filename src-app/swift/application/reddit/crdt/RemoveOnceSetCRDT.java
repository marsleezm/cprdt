package swift.application.reddit.crdt;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * @author Iwan Briquemont
 * @param <V>
 */
public class RemoveOnceSetCRDT<V> extends BaseCRDT<RemoveOnceSetCRDT<V>> {
    private Set<V> elements;
    private Set<V> removedElements;

    // Kryo
    public RemoveOnceSetCRDT() {
    }

    public RemoveOnceSetCRDT(CRDTIdentifier id) {
        super(id);
        elements = new HashSet<V>();
        removedElements = new HashSet<V>();
    }

    private RemoveOnceSetCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Set<V> elements, Set<V> removed) {
        super(id, txn, clock);
        this.elements = elements;
        this.removedElements = removed;
    }

    public void add(V element) {
        if (removedElements.contains(element)) {
            return;
        }
        elements.add(element);
        registerLocalOperation(new RemoveOnceSetAddUpdate<V>(element));
    }

    public void applyAdd(V element) {
        if (removedElements.contains(element)) {
            return;
        }
        elements.add(element);
    }
    
    public void remove(V element) {
        removedElements.add(element);
        elements.remove(element);
        registerLocalOperation(new RemoveOnceSetRemoveUpdate<V>(element));
    }
    
    public void applyRemove(V element) {
        removedElements.add(element);
        elements.remove(element);
    }
    
    public int size() {
        return elements.size();
    }

    public boolean lookup(V element) {
        return elements.contains(element);
    }
    
    /**
     * Has the element already been removed ?
     */
    public boolean removed(V element) {
        return removedElements.contains(element);
    }

    @Override
    public Set<V> getValue() {
        return Collections.unmodifiableSet(elements);
    }

    @Override
    public RemoveOnceSetCRDT<V> copy() {
        return new RemoveOnceSetCRDT<V>(id, txn, clock, new HashSet<V>(elements), new HashSet<V>(removedElements));
    }

}
