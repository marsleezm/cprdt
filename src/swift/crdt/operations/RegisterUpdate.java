package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.RegisterVersioned;

public class RegisterUpdate<V> extends BaseOperation<RegisterVersioned<V>> {
    private V val;

    public RegisterUpdate(TripleTimestamp ts, V val) {
        super(ts);
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
        // Insert does not rely on any timestamp dependency.
    }

    @Override
    public void applyTo(RegisterVersioned<V> register) {
        register.update(val, getTimestamp());
    }

}
