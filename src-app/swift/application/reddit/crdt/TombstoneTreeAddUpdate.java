package swift.application.reddit.crdt;

import java.util.Collections;
import java.util.Set;

import swift.crdt.core.CRDTUpdate;

public class TombstoneTreeAddUpdate<V> implements CRDTUpdate<TombstoneTreeCRDT<V>> {
    private Node<V> node;

    // Kryo
    public TombstoneTreeAddUpdate() {
    }

    public TombstoneTreeAddUpdate(Node<V> node) {
        this.node = node;
    }

    @Override
    public void applyTo(TombstoneTreeCRDT<V> crdt) {
        crdt.applyAdd(node);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return null;
    }
    @Override
    public Set<Object> affectedParticles() {
        return (Set) Collections.singleton(node);
    }
}
