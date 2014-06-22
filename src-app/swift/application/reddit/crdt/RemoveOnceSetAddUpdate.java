package swift.application.reddit.crdt;

import java.util.Collections;
import java.util.Set;

import swift.crdt.core.CRDTUpdate;

public class RemoveOnceSetAddUpdate<V> implements CRDTUpdate<RemoveOnceSetCRDT<V>> {
    private V element;

    // Kryo
    public RemoveOnceSetAddUpdate() {
    }

    public RemoveOnceSetAddUpdate(V element) {
        this.element = element;
    }

    @Override
    public void applyTo(RemoveOnceSetCRDT<V> crdt) {
        crdt.applyAdd(element);
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
