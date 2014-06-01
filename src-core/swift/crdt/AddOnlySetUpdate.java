package swift.crdt;

import java.util.Collections;
import java.util.Set;

import swift.crdt.core.AbstractCRDTUpdate;

public class AddOnlySetUpdate<V> extends AbstractCRDTUpdate<AddOnlySetCRDT<V>> {
    private V element;

    // Kryo
    public AddOnlySetUpdate() {
    }

    public AddOnlySetUpdate(V element) {
        this.element = element;
    }

    @Override
    public void applyTo(AddOnlySetCRDT<V> crdt) {
        crdt.applyAdd(element);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return element;
    }
    
    @Override
    public Set<Object> affectedParticles() {
        return Collections.singleton((Object)element);
    }
}
