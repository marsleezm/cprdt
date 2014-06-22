package swift.application.reddit.cprdt;

import java.util.Collections;
import java.util.Set;

import swift.application.reddit.Date;
import swift.crdt.core.CRDTUpdate;

public class VoteableTreeRemoveUpdate<V extends Date<V>, U> implements CRDTUpdate<VoteableTreeCPRDT<V, U>> {
    private SortedNode<V> node;

    // Kryo
    public VoteableTreeRemoveUpdate() {
    }

    public VoteableTreeRemoveUpdate(SortedNode<V> node) {
        this.node = node;
    }

    @Override
    public void applyTo(VoteableTreeCPRDT<V, U> crdt) {
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
