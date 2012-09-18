package swift.crdt;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class RegisterVersioned<V extends Copyable> extends BaseCRDT<RegisterVersioned<V>> {
    private static final long serialVersionUID = 1L;

    // Queue holding the versioning information, ordering is compatible with
    // causal dependency, newest entries coming first
    private SortedSet<QueueEntry<V>> values;

    public RegisterVersioned() {
        this.values = new TreeSet<QueueEntry<V>>();
    }

    @Override
    public void rollback(Timestamp ts) {
        Iterator<QueueEntry<V>> it = values.iterator();
        while (it.hasNext()) {
            QueueEntry<V> entry = it.next();
            if (!entry.c.includes(ts)) {
                break;
            }
            if (entry.ts.equals(ts)) {
                it.remove();
            }
        }
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        // Short cut for objects that are rarely updated: If there is only one
        // value, it must remain
        if (values.size() == 1) {
            return;
        }

        // Remove all values older than the pruningPoint, except the single
        // value representing purningPoint - there must be a summary of pruned
        // state.
        boolean firstMatchSkipped = false;
        final Iterator<QueueEntry<V>> iter = values.iterator();
        while (iter.hasNext()) {
            if (pruningPoint.includes(iter.next().ts)) {
                if (firstMatchSkipped) {
                    iter.remove();
                } else {
                    firstMatchSkipped = true;
                }
            }
        }
    }

    public void update(V val, TripleTimestamp ts, CausalityClock c) {
        values.add(new QueueEntry<V>(ts, c, val));
    }

    @Override
    protected void mergePayload(RegisterVersioned<V> otherObject) {
        CMP_CLOCK cmpClock = otherObject.getPruneClock().compareTo(getPruneClock());
        if (cmpClock == CMP_CLOCK.CMP_DOMINATES) {
            pruneImpl(otherObject.getPruneClock());
        } else {
            if (cmpClock == CMP_CLOCK.CMP_ISDOMINATED) {
                otherObject.pruneImpl(getPruneClock());
            }
        }
        values.addAll(otherObject.values);
    }

    @Override
    protected TxnLocalCRDT<RegisterVersioned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final RegisterVersioned<V> creationState = isRegisteredInStore() ? null : new RegisterVersioned<V>();
        RegisterTxnLocal<V> localview = new RegisterTxnLocal<V>(id, txn, versionClock, creationState,
                value(versionClock));
        return localview;
    }

    private V value(CausalityClock versionClock) {
        for (QueueEntry<V> e : values) {
            if (versionClock.includes(e.ts)) {
                return e.value;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return values.toString();
    }

    @Override
    protected void execute(CRDTUpdate<RegisterVersioned<V>> op) {
        op.applyTo(this);
    }

    @Override
    public RegisterVersioned<V> copy() {
        RegisterVersioned<V> copyObj = new RegisterVersioned<V>();
        for (QueueEntry<V> e : values) {
            copyObj.values.add(e.copy());
        }
        copyBase(copyObj);
        return copyObj;
    }

    @Override
    protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
        final Set<Timestamp> result = new HashSet<Timestamp>();
        for (QueueEntry<V> e : values) {
            if (!clock.includes(e.ts)) {
                result.add(e.ts.cloneBaseTimestamp());
            }
        }
        return result;
    }

    // public for sake of Kryo...
    public static class QueueEntry<V extends Copyable> implements Comparable<QueueEntry<V>>, Serializable {
        TripleTimestamp ts;
        CausalityClock c;
        V value;

        /**
         * Do not use: Empty constructor only to be used by Kryo serialization.
         */
        public QueueEntry() {
        }

        public QueueEntry(TripleTimestamp ts, CausalityClock c, V value) {
            this.ts = ts;
            this.c = c;
            this.value = value;
        }

        @Override
        public int compareTo(QueueEntry<V> other) {
            CMP_CLOCK result = this.c.compareTo(other.c);
            switch (result) {
            case CMP_CONCURRENT:
            case CMP_EQUALS:
                if (other.ts == null) {
                    return 1;
                } else {
                    return other.ts.compareTo(this.ts);
                }
            case CMP_ISDOMINATED:
                return 1;
            case CMP_DOMINATES:
                return -1;
            default:
                return 0;
            }
        }

        @Override
        public String toString() {
            return value + " -> " + ts + "," + c;
        }

        public QueueEntry<V> copy() {
            QueueEntry<V> copyObj = new QueueEntry<V>(ts, c.clone(), (V) value.copy());
            return copyObj;
        }
    }
}
