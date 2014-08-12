package swift.application.swiftlinks.crdt;

import java.util.Collections;
import java.util.Set;

import swift.crdt.core.CRDTUpdate;

/**
 * 
 * @author Iwan Briquemont
 *
 * @param <V>
 */
public class RemoveOnceSetRemoveUpdate<V> implements CRDTUpdate<RemoveOnceSetCRDT<V>> {
    private V element;

    // Kryo
    public RemoveOnceSetRemoveUpdate() {
    }

    public RemoveOnceSetRemoveUpdate(V element) {
        this.element = element;
    }

    @Override
    public void applyTo(RemoveOnceSetCRDT<V> crdt) {
        crdt.applyRemove(element);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return null;
    }

    @Override
    public Set<Object> affectedParticles() {
        return (Set<Object>) Collections.singleton(element);
    }
}
