package swift.application.swiftlinks.cprdt;

import java.util.Collections;
import java.util.Set;

import swift.application.swiftlinks.Dateable;
import swift.crdt.core.CRDTUpdate;

/**
 * Operation to remove a node from the tree
 * 
 * @author Iwan Briquemont
 *
 * @param <V>
 * @param <U>
 */
public class VoteableTreeRemoveUpdate<V extends Dateable<V>, U> implements CRDTUpdate<VoteableTreeCPRDT<V, U>> {
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
        return node;
    }
    
    @Override
    public Set<Object> affectedParticles() {
        return (Set) Collections.singleton(node);
    }
}
