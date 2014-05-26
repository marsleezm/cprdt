package swift.crdt;

import swift.crdt.core.AbstractCRDTUpdate;

public class MaxUpdate<V extends Comparable<V>> extends AbstractCRDTUpdate<MaxCRDT<V>> {
    V value;

    public MaxUpdate(V value) {
        this.value = value;
    }

    @Override
    public void applyTo(MaxCRDT<V> crdt) {
        crdt.applySet(value);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return value;
    }
}