package swift.client;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.cprdt.core.CRDTShardQuery;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.TxnStatus;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * Really simple fake Txn manager implementation (to test the TxnHandle)
 * @author iwan
 *
 */
public class TxnManagerTester implements TxnManager {
    
    private Map<CRDTIdentifier,ManagedCRDT<?>> store;
    private CausalityClock latestClock;
    
    public TxnManagerTester() {
        this.store = new HashMap<CRDTIdentifier, ManagedCRDT<?>>();
        this.latestClock = ClockFactory.newClock();
    }
    
    public CausalityClock getLatestClock() {
        return latestClock;
    }

    @Override
    public <V extends CRDT<V>> CRDT<V> getObjectLatestVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CachePolicy cachePolicy, boolean create, Class<V> classOfV, ObjectUpdatesListener updatesListener,
            CRDTShardQuery<V> query) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        return getObjectVersionTxnView(txn, id, latestClock, true, classOfV, updatesListener, query);
    }

    @Override
    public <V extends CRDT<V>> CRDT<V> getObjectVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CausalityClock version, boolean create, Class<V> classOfV, ObjectUpdatesListener updatesListener,
            CRDTShardQuery<V> query) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        ManagedCRDT<V> crdt = (ManagedCRDT<V>) store.get(id);
        if (crdt == null) {
            if (!create) {
                throw new NoSuchObjectException(id + " not found in the store");
            }
            final V checkpoint;
            try {
                final Constructor<V> constructor = classOfV.getConstructor(CRDTIdentifier.class);
                checkpoint = constructor.newInstance(id);
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            final CausalityClock clock = version.clone();
            crdt = new ManagedCRDT<V>(id, checkpoint, clock, false);
            store.put(id, crdt);
        }
        
        crdt = crdt.copyWithRestrictedVersioning(version);
        
        crdt.applyShardQuery(query, version);
        
        return crdt.getVersion(version, txn);
    }

    @Override
    public void discardTxn(AbstractTxnHandle txn) {
        
    }

    @Override
    public void commitTxn(AbstractTxnHandle txn) {
        txn.markLocallyCommitted();

        if (requiresGlobalCommit(txn)) {

            latestClock.record(txn.getClientTimestamp());
            for (final CRDTObjectUpdatesGroup<?> opsGroup : txn.getAllUpdates()) {
                final CRDTIdentifier id = opsGroup.getTargetUID();
                applyLocalObjectUpdates(store.get(id), txn);
            }
            augmentAllWithScoutTimestampWithoutMappings(txn.getClientTimestamp());
            latestClock.merge(txn.getUpdatesDependencyClock());

            // Global commit
            txn.markGloballyCommitted(txn.getClientTimestamp());

        } else {
            txn.markGloballyCommitted(null);
        }
    }
    
    synchronized void augmentAllWithScoutTimestampWithoutMappings(Timestamp clientTimestamp) {
        for (final ManagedCRDT<?> crdt : store.values()) {
            crdt.augmentWithScoutTimestamp(clientTimestamp);
        }
    }
    
    // Txn is committed globally if it is not read-only, if it contains updates
    // and if it has not been cancelled
    private boolean requiresGlobalCommit(AbstractTxnHandle txn) {
        if (txn.isReadOnly()) {
            return false;
        }
        if (txn.getStatus() == TxnStatus.CANCELLED || txn.getAllUpdates().isEmpty()) {
            return false;
        }
        return true;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void applyLocalObjectUpdates(ManagedCRDT cachedCRDT, final AbstractTxnHandle localTxn) {
        if (cachedCRDT == null) {
            return;
        }

        final CRDTObjectUpdatesGroup objectUpdates = localTxn.getObjectUpdates(cachedCRDT.getUID());
        if (objectUpdates != null) {
            cachedCRDT.execute(objectUpdates, CRDTOperationDependencyPolicy.IGNORE);
        } else {
            cachedCRDT.augmentWithScoutTimestamp(localTxn.getClientTimestamp());
            CausalityClock dcTimetsamps = ClockFactory.newClock();
            for (final Timestamp sysTs : localTxn.getTimestampMapping().getSystemTimestamps()) {
                dcTimetsamps.record(sysTs);
            }
            cachedCRDT.augmentWithDCClockWithoutMappings(dcTimetsamps);
        }
    }

}
