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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.client.CommitListener;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TimestampSource;
import swift.clocks.TripleTimestamp;
import swift.cprdt.FractionShardQuery;
import swift.cprdt.HollowShardQuery;
import swift.cprdt.core.CRDTShardQuery;
import swift.crdt.core.BulkGetProgressListener;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.CRDTUpdate;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.TxnHandle;
import swift.crdt.core.TxnStatus;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * Fake {@link TxnHandle} relying on external objects. Can be used in isolation,
 * to test sequential code of CRDTs and WrappedCRDT ( create instance using
 * {@link #createIsolatedTxnTester()} or over a shared state
 * {@link SwiftTester#beginTxn(ManagedCRDT...)}.
 * 
 * @author annettebieniusa, mzawirsk
 */
public class TxnTester implements TxnHandle {

    public static TxnTester createIsolatedTxnTester() {
        String siteId = "test-site";
        return new TxnTester(siteId, ClockFactory.newClock(), new IncrementalTimestampGenerator(siteId).generateNew(),
                new IncrementalTimestampGenerator("global:" + siteId).generateNew());
    }

    protected Map<CRDTIdentifier, CRDT<?>> versions;
    protected Map<CRDTIdentifier, ManagedCRDT<?>> objects;
    protected Map<CRDTIdentifier, CRDTObjectUpdatesGroup<?>> operations;
    protected CausalityClock cc;
    protected TimestampSource<TripleTimestamp> timestampGenerator;
    protected Timestamp globalTimestamp;
    protected TimestampMapping tm;

    protected TxnTester(String siteId, CausalityClock cc, ManagedCRDT<?>... managedCrdtsToTest) {
        this(siteId, cc, new IncrementalTimestampGenerator(siteId).generateNew(), new IncrementalTimestampGenerator(
                "global:" + siteId).generateNew(), managedCrdtsToTest);
    }

    protected TxnTester(String siteId, CausalityClock latestVersion, Timestamp ts, final Timestamp globalTs,
            ManagedCRDT<?>... existingObjects) {
        this.versions = new HashMap<CRDTIdentifier, CRDT<?>>();
        this.objects = new HashMap<CRDTIdentifier, ManagedCRDT<?>>();
        this.operations = new HashMap<CRDTIdentifier, CRDTObjectUpdatesGroup<?>>();
        this.cc = latestVersion;
        this.tm = new TimestampMapping(ts);
        this.timestampGenerator = new IncrementalTripleTimestampGenerator(ts);
        this.globalTimestamp = globalTs;
        for (final ManagedCRDT<?> managedCRDT : existingObjects) {
            objects.put(managedCRDT.getUID(), managedCRDT);
            operations
                    .put(managedCRDT.getUID(), new CRDTObjectUpdatesGroup(managedCRDT.getUID(), tm, null, cc.clone()));
        }
    }

    public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException,
            NoSuchObjectException {
        return get(id, create, classOfV, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener listener) throws WrongTypeException, NoSuchObjectException {
        return get(id, create, classOfV, listener, false);
    }

    public <V extends CRDT<V>> ManagedCRDT<V> getOrCreateVersionedCRDT(CRDTIdentifier id, Class<V> classOfV,
            boolean create) throws InstantiationException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException, NoSuchObjectException {
        @SuppressWarnings("unchecked")
        ManagedCRDT<V> managedCRDT = (ManagedCRDT<V>) objects.get(id);
        if (managedCRDT == null) {
            if (!create) {
                throw new NoSuchObjectException("Object " + id + " not found");
            }
            V checkpoint = classOfV.getConstructor(CRDTIdentifier.class).newInstance(id);
            managedCRDT = new ManagedCRDT<V>(id, checkpoint, cc.clone(), true);
            objects.put(id, managedCRDT);
        }
        return managedCRDT;
    }

    @Override
    public void commit() {
        commit(false);
    }

    public void commit(boolean globalCommit) {
        for (final Entry<CRDTIdentifier, CRDTObjectUpdatesGroup<?>> entry : operations.entrySet()) {
            if (globalCommit) {
                entry.getValue().addSystemTimestamp(globalTimestamp);
            }
            final ManagedCRDT<?> managedCRDT = objects.get(entry.getKey());
            managedCRDT.execute((CRDTObjectUpdatesGroup) entry.getValue(), CRDTOperationDependencyPolicy.CHECK);
        }
        for (final Timestamp ts : tm.getTimestamps()) {
            cc.record(ts);
        }
    }

    @Override
    public void commitAsync(final CommitListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() {
        throw new RuntimeException("Not supported for testing!");
    }

    @Override
    public TripleTimestamp nextTimestamp() {
        return timestampGenerator.generateNew();
    }

    public <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
    }

    @Override
    public TxnStatus getStatus() {
        return null;
    }

    public CausalityClock getClock() {
        return this.cc.clone();
    }

    @Override
    public <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTUpdate<V> op) {
        CRDTObjectUpdatesGroup<V> updates = (CRDTObjectUpdatesGroup<V>) operations.get(id);
        if (updates == null) {
            updates = new CRDTObjectUpdatesGroup<V>(id, tm, null, cc.clone());
            operations.put(id, updates);
        }
        updates.append(op);
    }

    @Override
    public Map<CRDTIdentifier, CRDT<?>> bulkGet(Set<CRDTIdentifier> ids, BulkGetProgressListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<CRDTIdentifier, CRDT<?>> bulkGet(CRDTIdentifier... ids) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV,
            boolean lazy) throws WrongTypeException,
            NoSuchObjectException {
        return get(id, create, classOfV, null, lazy);
    }

    private <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV, ObjectUpdatesListener listener,
            boolean lazy) throws WrongTypeException,
            NoSuchObjectException {
        try {
            CRDT<?> cached = versions.get(id);
            if (cached == null) {
                ManagedCRDT<V> managedObject = getOrCreateVersionedCRDT(id, classOfV, create);
                CRDT<V> localView;
                if (lazy) {
                    managedObject = managedObject.copyWithRestrictedVersioning(getClock());
                    managedObject.applyShardQuery(new HollowShardQuery(), getClock());
                    localView = managedObject.getVersion(getClock(), this);
                } else {
                    localView = managedObject.getVersion(getClock(), this);
                }
                versions.put(id, localView);
                return (V) localView;
            } else {
                return (V) cached;
            }
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.toString());
        } catch (InstantiationException e) {
            throw new WrongTypeException(e.toString());
        } catch (IllegalAccessException e) {
            throw new WrongTypeException(e.toString());
        } catch (NoSuchMethodException e) {
            throw new WrongTypeException(e.toString());
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.toString());
        }
    }

    @Override
    public <V extends CRDT<V>> void fetch(CRDTIdentifier id, Class<V> classOfV, Set<?> particles)
            throws WrongTypeException, VersionNotFoundException, NetworkException {
        fetch(id, classOfV, new FractionShardQuery<V>(particles));
    }

    @Override
    public <V extends CRDT<V>> void fetch(CRDTIdentifier id, Class<V> classOfV, CRDTShardQuery<V> query)
            throws WrongTypeException, VersionNotFoundException, NetworkException {
        fetch(id, classOfV, query, null);
    }

    @Override
    public <V extends CRDT<V>> void fetch(CRDTIdentifier id, Class<V> classOfV, CRDTShardQuery<V> query,
            ObjectUpdatesListener listener) throws WrongTypeException, VersionNotFoundException,
            NetworkException {
        CRDT<V> localView = (CRDT<V>) versions.get(id);
        if (localView == null) {
            // ignored for concurrent tests
            return;
        }
        if (query.isAvailableIn(localView.getShard())) {
            return;
        }
        ManagedCRDT<V> managedObject;
        try {
            managedObject = getOrCreateVersionedCRDT(id, classOfV, false);
        } catch (NoSuchObjectException e) {
            throw new IllegalStateException("No object during fetch "+e.getMessage());
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.toString());
        } catch (InstantiationException e) {
            throw new WrongTypeException(e.toString());
        } catch (IllegalAccessException e) {
            throw new WrongTypeException(e.toString());
        } catch (NoSuchMethodException e) {
            throw new WrongTypeException(e.toString());
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.toString());
        }
        managedObject = managedObject.copyWithRestrictedVersioning(getClock());
        managedObject.applyShardQuery(query, getClock());
        localView.mergeSameVersion(managedObject.getVersion(getClock(), this));
    }

    @Override
    public <V extends CRDT<V>> boolean objectIsFound(CRDTIdentifier id, Class<V> classOfV) {
        return objects.get(id) != null;
    }

}
