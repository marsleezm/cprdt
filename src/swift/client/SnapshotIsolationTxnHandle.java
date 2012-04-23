package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * Implementation of SwiftCloud transaction, keeps track of its state and
 * mediates between the application and {@link TxnManager} communicating with
 * the store.
 * <p>
 * Transaction is <em>read-only transaction</em> if it does not issue any update
 * operation; transaction is <em>update transaction</em> otherwise. Update
 * transaction uses timestamp for its updates.
 * <p>
 * Instances shall be generated using and managed by {@link TxnManager}
 * implementation. Thread-safe.
 * <p>
 * <b>Implementation notes<b>. Each transaction defines a snapshot point which
 * is a set of update transactions visible to this transaction. This set
 * includes both:
 * <ul>
 * <li>globally committed transactions - update transactions committed in the
 * store using a stable global timestamp assigned by server, and</li>
 * <li>locally committed transactions - update transactions committed locally
 * with a tentative local timestamp, committing at the store; locally committed
 * transaction visible to this transaction must and will become globally
 * committed before this transaction commits globally, as guaranteed by
 * {@link TxnManager}.</li>
 * </ul>
 * Global visible transactions are defined by a global visibility clock, whereas
 * local visible transactions are explicitly referenced local instances of this
 * class. After every update transaction locally commits, its locally visible
 * (dependent) transactions become gradually globally committed, i.e. their
 * update operations get global timestamp assigned. This imposes some changes in
 * timestamps used for operations generated by this transaction.
 * 
 * @author mzawirski
 */
class SnapshotIsolationTxnHandle extends AbstractTxnHandle implements TxnHandle {
    final CausalityClock visibleTransactionsClock;
    final Map<CRDTIdentifier, TxnLocalCRDT<?>> objectViewsCache;

    /**
     * @param manager
     *            manager maintaining this transaction
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param localTimestamp
     *            local timestamp used for local operations of this transaction
     * @param globalVisibleTransactionsClock
     *            clock representing globally committed update transactions
     *            visible to this transaction; left unmodified
     */
    SnapshotIsolationTxnHandle(final TxnManager manager, final CachePolicy cachePolicy, final Timestamp localTimestamp,
            final CausalityClock globalVisibleTransactionsClock) {
        super(manager, cachePolicy, localTimestamp);
        this.visibleTransactionsClock = globalVisibleTransactionsClock.clone();
        this.objectViewsCache = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        updateUpdatesDependencyClock(visibleTransactionsClock);
    }

    @Override
    protected <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T getImpl(CRDTIdentifier id, boolean create,
            Class<V> classOfV, ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException, NetworkException {
        TxnLocalCRDT<V> localView = (TxnLocalCRDT<V>) objectViewsCache.get(id);
        if (localView == null) {
            localView = manager.getObjectTxnView(this, id, visibleTransactionsClock, false, create, classOfV,
                    updatesListener);
            objectViewsCache.put(id, localView);
        }
        return (T) localView;
    }
}
