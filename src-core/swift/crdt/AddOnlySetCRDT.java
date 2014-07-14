package swift.crdt;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.cprdt.FullShardQuery;
import swift.cprdt.core.Shard;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;

/**
 * @author mzawirsk
 * @param <V>
 */
public class AddOnlySetCRDT<V> extends BaseCRDT<AddOnlySetCRDT<V>> {
    private Set<V> elements;

    // Kryo
    public AddOnlySetCRDT() {
    }

    public AddOnlySetCRDT(CRDTIdentifier id) {
        super(id);
        elements = new HashSet<V>();
    }

    private AddOnlySetCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Set<V> elements, Shard shard) {
        super(id, txn, clock);
        this.elements = elements;
        this.setShard(shard);
    }
    
    @Override
    public int estimatedSize() {
        return elements.size();
    }

    public void add(V element) {
        this.applyAdd(element);
        registerLocalOperation(new AddOnlySetUpdate<V>(element));
    }

    public void applyAdd(V element) {
        if (!this.getShard().contains(element)) {
            // Once we add an element, we cannot delete it => we know its state
            this.setShard(this.getShard().union(new Shard(Collections.singleton((Object) element))));
        }
        elements.add(element);
    }

    public boolean has(V element) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(element));
        return elements.contains(element);
    }

    @Override
    public Set<V> getValue() {
        return Collections.unmodifiableSet(elements);
    }
    
    public Set<V> getFullSet() throws VersionNotFoundException, NetworkException {
        fetch(new FullShardQuery());
        return this.getValue();
    }

    @Override
    public AddOnlySetCRDT<V> copy() {
        return new AddOnlySetCRDT<V>(id, txn, clock, new HashSet<V>(elements), this.getShard());
    }

    @Override
    public AddOnlySetCRDT<V> copyFraction(Set<?> particles) {
        Shard copyShard = new Shard((Set<Object>) particles);
        Set<V> copyElements = new HashSet<V>();
        for (V element : (Set<V>) particles) {
            if (elements.contains(element)) {
                copyElements.add(element);
            }
        }
        return new AddOnlySetCRDT<V>(id, txn, clock, copyElements, copyShard);
    }

    @Override
    public void mergeSameVersion(AddOnlySetCRDT<V> other) {
        for (V element : other.elements) {
            elements.add(element);
        }
    }
}
