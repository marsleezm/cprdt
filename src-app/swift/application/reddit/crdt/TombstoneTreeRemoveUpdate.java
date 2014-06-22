package swift.application.reddit.crdt;

import java.util.Collections;
import java.util.Set;

import swift.crdt.core.CRDTUpdate;

public class TombstoneTreeRemoveUpdate<V> implements CRDTUpdate<TombstoneTreeCRDT<V>> {
    private Node<V> node;

    // Kryo
    public TombstoneTreeRemoveUpdate() {
    }

    public TombstoneTreeRemoveUpdate(Node<V> node) {
        this.node = node;
    }

    @Override
    public void applyTo(TombstoneTreeCRDT<V> crdt) {
        crdt.applyRemove(node);
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
