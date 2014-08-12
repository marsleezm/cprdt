package swift.application.swiftlinks.cprdt;

import java.util.Collections;
import java.util.Set;

import swift.application.swiftlinks.Dateable;
import swift.crdt.core.CRDTUpdate;

/**
 * Operation to add a node to the tree
 * 
 * @author Iwan Briquemont
 *
 * @param <V>
 * @param <U>
 */
public class VoteableTreeAddUpdate<V extends Dateable<V>, U> implements CRDTUpdate<VoteableTreeCPRDT<V, U>> {
    private SortedNode<V> node;

    // Kryo
    public VoteableTreeAddUpdate() {
    }

    public VoteableTreeAddUpdate(SortedNode<V> node) {
        this.node = node;
    }

    @Override
    public void applyTo(VoteableTreeCPRDT<V, U> crdt) {
        crdt.applyAdd(node);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return node;
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Set<Object> affectedParticles() {
        return (Set) Collections.singleton(node);
    }
}
