package swift.crdt.operations;

import java.util.HashSet;
import java.util.Set;

import swift.clocks.TripleTimestamp;
import swift.crdt.SortedSetVersioned;

public class SequenceRemove<V extends Comparable<V>, T extends SortedSetVersioned<V, T>> extends BaseUpdate<T> {
    private V val;
    private Set<TripleTimestamp> ids;

    // required for kryo
    public SequenceRemove() {
    }

    public SequenceRemove(TripleTimestamp ts, V val, Set<TripleTimestamp> ids) {
        super(ts);
        this.val = val;
        this.ids = new HashSet<TripleTimestamp>();
        for (final TripleTimestamp id : ids) {
            this.ids.add(id.copyWithCleanedMappings());
        }
    }

    public V getVal() {
        return this.val;
    }

    public Set<TripleTimestamp> getIds() {
        return this.ids;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.removeU(val, getTimestamp(), ids);
    }
}
