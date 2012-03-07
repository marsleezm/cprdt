package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetOperation;
import swift.crdt.operations.SetRemove;
import swift.exceptions.NotSupportedOperationException;
import swift.utils.Pair;
import swift.utils.PrettyPrint;

/**
 * CRDT SET with Versioning support
 * 
 * TODO: This implementation makes a copy of every object, to improve
 * performance it should use references instead (see merge)
 * 
 * Implementation was using GlobalCRDTRuntime, must check if it is necessary, or
 * was just an error
 * 
 * @author vb, annettebieniusa
 * 
 * @param <V>
 */
public abstract class SetVersioned<V, T extends SetVersioned<V, T>> extends BaseCRDT<T, SetOperation<V>> {

    private static final long serialVersionUID = 1L;
    private Map<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> elems;

    public SetVersioned() {
        elems = new HashMap<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>();
    }

    public boolean lookup(V e) {
        Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> entry = elems.get(e);
        if (entry == null) {
            return false;
        }

        boolean visible = false;
        for (Pair<TripleTimestamp, Set<TripleTimestamp>> valueTS : entry) {
            if (getClock().includes(valueTS.getFirst())) {
                boolean notRemoved = true;
                for (TripleTimestamp remTS : valueTS.getSecond()) {
                    if (getClock().includes(remTS)) {
                        notRemoved = false;
                    }
                }
                if (notRemoved) {
                    visible = true;
                    break;
                }
            }
        }
        return visible;
    }

    public Set<V> getValue() {
        Set<Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>> entrySet = elems.entrySet();
        Set<V> retValues = new HashSet<V>();

        for (Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> e : entrySet) {
            boolean add = false;
            for (Pair<TripleTimestamp, Set<TripleTimestamp>> p : e.getValue()) {
                if (getClock() == null && p.getSecond().isEmpty()) {
                    add = true;
                } else if (getClock() != null && getClock().includes(p.getFirst())) {
                    if (p.getSecond().isEmpty()) {
                        add = true;
                    } else {
                        add = true;
                        for (TripleTimestamp remTs : p.getSecond()) {
                            if (getClock().includes(remTs)) {
                                add = false;
                            }
                        }
                    }
                }
            }
            if (add) {
                retValues.add(e.getKey());
            }
        }
        return retValues;
    }

    /**
     * Insert element V in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(V e) {
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetInsert<V>(getUID(), ts, getClock(), e));
    }

    private void insertU(V e, TripleTimestamp uid) {
        Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> entry = elems.get(e);
        // if element not present in the set, add entry for it in payload
        if (entry == null) {
            entry = new HashSet<Pair<TripleTimestamp, Set<TripleTimestamp>>>();
            elems.put(e, entry);
        }

        /*
         * for (Pair<TripleTimestamp, Set<TripleTimestamp>> valueTS : entry) {
         * valueTS.getSecond().add(uid); }
         */
        Pair<TripleTimestamp, Set<TripleTimestamp>> newValue = new Pair<TripleTimestamp, Set<TripleTimestamp>>(uid,
                new HashSet<TripleTimestamp>());
        entry.add(newValue);
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(V e) {
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetRemove<V>(getUID(), ts, getClock(), e));
    }

    private void removeU(V e, TripleTimestamp uid) {
        Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> s = elems.get(e);
        if (s == null) {
            return;
        }
        for (Pair<TripleTimestamp, Set<TripleTimestamp>> p : s) {
            p.getSecond().add(uid);
        }

    }

    @Override
    protected void mergePayload(T other) {
        Iterator<Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>> it = other.elems.entrySet().iterator();
        while (it.hasNext()) {
            Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> e = it.next();
            Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> s = elems.get(e.getKey());
            if (s == null) {
                Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> newSet = new HashSet<Pair<TripleTimestamp, Set<TripleTimestamp>>>();
                for (Pair<TripleTimestamp, Set<TripleTimestamp>> op : e.getValue()) {
                    Pair<TripleTimestamp, Set<TripleTimestamp>> newPair = new Pair<TripleTimestamp, Set<TripleTimestamp>>(
                            op.getFirst(), new HashSet<TripleTimestamp>(op.getSecond()));
                    newSet.add(newPair);
                }
                elems.put(e.getKey(), newSet);
            } else {
                for (Pair<TripleTimestamp, Set<TripleTimestamp>> otherE : e.getValue()) {
                    boolean exists = false;
                    for (Pair<TripleTimestamp, Set<TripleTimestamp>> localE : s) {
                        if (localE.getFirst().equals(otherE.getFirst())) {
                            localE.getSecond().addAll(otherE.getSecond());
                            exists = true;
                        }
                    }
                    if (!exists) {
                        Pair<TripleTimestamp, Set<TripleTimestamp>> newPair = new Pair<TripleTimestamp, Set<TripleTimestamp>>(
                                otherE.getFirst(), otherE.getSecond());
                        s.add(newPair);
                    }
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SetVersioned)) {
            return false;
        }
        SetVersioned<?, ?> that = (SetVersioned<?, ?>) o;
        return that.elems.equals(this.elems);
    }

    @Override
    public String toString() {
        return PrettyPrint.printMap("{", "}", ";", "->", elems);

    }

    @Override
    public void rollback(Timestamp rollbackEvent) {
        Iterator<Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>> entries = elems.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> e = entries.next();
            Iterator<Pair<TripleTimestamp, Set<TripleTimestamp>>> perClient = e.getValue().iterator();
            while (perClient.hasNext()) {
                Pair<TripleTimestamp, Set<TripleTimestamp>> valueTS = perClient.next();
                if (valueTS.getFirst().equals(rollbackEvent)) {
                    perClient.remove();
                } else {
                    Iterator<TripleTimestamp> remTS = valueTS.getSecond().iterator();
                    while (remTS.hasNext()) {
                        if (remTS.next().equals(rollbackEvent)) {
                            remTS.remove();
                        }
                    }
                }
            }
            if (e.getValue().isEmpty()) {
                entries.remove();
            }
        }
    }

    @Override
    public void prune(CausalityClock pruningPoint) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void executeImpl(SetOperation<V> op) {
        if (op instanceof SetInsert) {
            SetInsert<V> addop = (SetInsert<V>) op;
            this.insertU(addop.getVal(), addop.getTimestamp());
        } else if (op instanceof SetRemove) {
            SetRemove<V> subop = (SetRemove<V>) op;
            this.removeU(subop.getVal(), subop.getTimestamp());
        } else {
            throw new NotSupportedOperationException();
        }
    }
}
