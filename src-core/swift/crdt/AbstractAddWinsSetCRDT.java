/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.crdt;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.cprdt.FullShardQuery;
import swift.cprdt.core.Shard;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.VersionNotFoundException;
import swift.utils.PrettyPrint;

/**
 * Abstract add-wins set CRDT, independent of set type (ordered vs unordered).
 * 
 * @author vb, annettebieniusa, mzawirsk
 * 
 * @param <V>
 *            elements type
 * @param <T>
 *            type of concrete implementation
 */
public abstract class AbstractAddWinsSetCRDT<V, T extends AbstractAddWinsSetCRDT<V, T>> extends BaseCRDT<T> {
    
    // Kryo
    protected AbstractAddWinsSetCRDT() {
    }

    protected AbstractAddWinsSetCRDT(final CRDTIdentifier id) {
        super(id);
    }
    
    protected AbstractAddWinsSetCRDT(final CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock) {
        super(id, txn, clock);
    }

    protected AbstractAddWinsSetCRDT(final CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock, final Shard shard) {
        super(id, txn, clock, shard);
    }

    protected abstract Map<V, Set<TripleTimestamp>> getElementsInstances();

    /**
     * @return non-modifiable reference of a set of elements
     */
    @Override
    public Set<V> getValue() {
        return Collections.unmodifiableSet(getElementsInstances().keySet());
    }
    
    public Set<V> getFullSet() throws VersionNotFoundException, NetworkException {
        fetch(new FullShardQuery());
        return getValue();
    }

    public int size() {
        return getElementsInstances().size();
    }
    
    @Override
    public long estimatedSize() {
        return size();
    }
    
    public int fullSize() throws VersionNotFoundException, NetworkException {
        fetch(new FullShardQuery());
        return size();
    }

    public boolean lookup(V element) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(element));
        return getElementsInstances().containsKey(element);
    }

    public void add(final V element) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(element));
        final TripleTimestamp ts = nextTimestamp();
        final Set<TripleTimestamp> existingInstances = AddWinsUtils.add(getElementsInstances(), element, ts);
        registerLocalOperation(new AddWinsSetAddUpdate<V, T>(element, ts, existingInstances));
    }
    
    /**
     * Add an element without garbage collection
     * @param element
     */
    public void addBlind(final V element) {
        final TripleTimestamp ts = nextTimestamp();
        AddWinsUtils.add(getElementsInstances(), element, ts);
        registerLocalOperation(new AddWinsSetAddUpdate<V, T>(element, ts, null));
    }

    public void remove(V element) throws VersionNotFoundException, NetworkException {
        fetch(Collections.singleton(element));
        Set<TripleTimestamp> removedInstances = AddWinsUtils.remove(getElementsInstances(), element);
        if (removedInstances != null) {
            registerLocalOperation(new AddWinsSetRemoveUpdate<V, T>(element, removedInstances));
        }
    }

    protected void applyAdd(V element, TripleTimestamp instance, Set<TripleTimestamp> overwrittenInstances) {
        AddWinsUtils.applyAdd(getElementsInstances(), element, instance, overwrittenInstances);
    }

    protected void applyRemove(V element, Set<TripleTimestamp> removedInstances) {
        AddWinsUtils.applyRemove(getElementsInstances(), element, removedInstances);
    }

    @Override
    public String toString() {
        return PrettyPrint.printMap("{", "}", ";", "->", getElementsInstances());
    }
}
