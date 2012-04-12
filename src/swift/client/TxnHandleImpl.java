package swift.client;

import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * Implementation of SwiftCloud transaction, keeps track of its state and
 * mediates between application and low-level SwiftImpl client.
 * <p>
 * Instances shall be generated using and managed by Swift client implementation
 * only. Thread-safe.
 * <p>
 * <b>Implementation notes<b>. Each transaction defines a snapshot point which
 * is a set of transactions visible to this transaction. This set includes both:
 * <ul>
 * <li>globally committed transactions - transactions committed in the store
 * using a stable global timestamp assigned by server, and</li>
 * <li>locally committed transactions - transactions committed locally with a
 * tentative timestamp, committing at the store; locally committed transaction
 * visible to this transaction must and will become globally committed before
 * this transaction commits globally.</li>
 * </ul>
 * Global visible transactions are defined by a global visibility clock, whereas
 * local visible transactions are explicitly referenced local instances of this
 * class. After this transaction locally commits, its locally visible
 * (dependent) transactions become gradually committed. This imposes some
 * changes in timestamps used for operations generated by this transactions.
 * 
 * @author mzawirski
 */
class TxnHandleImpl implements TxnHandle {
    private final SwiftImpl swift;
    private final CausalityClock globalVisibleTransactionsClock;
    private final Deque<TxnHandleImpl> localVisibleTransactions;
    private final Timestamp localTimestamp;
    private final IncrementalTripleTimestampGenerator timestampSource;
    private Timestamp globalTimestamp;
    private final Map<CRDTIdentifier, TxnLocalCRDT<?>> objectsInUse;
    private final Map<CRDTIdentifier, CRDTObjectOperationsGroup<?>> localObjectOperations;
    private TxnStatus status;
    private CommitListener commitListener;

    /**
     * @param swift
     *            swift instance that is maintaining this transaction
     * @param globalVisibleTransactionsClock
     *            clock representing globally commited transactions visible to
     *            this transaction; left unmodified
     * @param localVisibleTransactions
     *            deque of locally committed transactions (in order of local
     *            commitment) visible to this transaction; left unmodified
     * @param localTimestamp
     *            local timestamp used for local operations of this transaction
     */
    TxnHandleImpl(final SwiftImpl swift, final CausalityClock globalVisibleTransactionsClock,
            final List<TxnHandleImpl> localVisibleTransactions, final Timestamp localTimestamp) {
        this.swift = swift;
        this.globalVisibleTransactionsClock = globalVisibleTransactionsClock.clone();
        this.localVisibleTransactions = new LinkedList<TxnHandleImpl>(localVisibleTransactions);
        this.localTimestamp = localTimestamp;
        this.timestampSource = new IncrementalTripleTimestampGenerator(localTimestamp);
        this.localObjectOperations = new HashMap<CRDTIdentifier, CRDTObjectOperationsGroup<?>>();
        this.objectsInUse = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        this.status = TxnStatus.PENDING;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create,
            Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        assertStatus(TxnStatus.PENDING);

        try {
            TxnLocalCRDT<V> localView = (TxnLocalCRDT<V>) objectsInUse.get(id);
            if (localView == null) {
                localView = swift.getObjectTxnView(this, id, create, classOfV);
                objectsInUse.put(id, localView);
            }
            return (T) localView;
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
    }

    @Override
    public void commit() {
        final Semaphore commitSem = new Semaphore(0);
        commitAsync(new CommitListener() {
            @Override
            public void onGlobalCommit(TxnHandle transaction) {
                commitSem.release();
            }
        });
        commitSem.acquireUninterruptibly();
    }

    @Override
    public synchronized void commitAsync(final CommitListener listener) {
        assertStatus(TxnStatus.PENDING);
        this.commitListener = listener;
        swift.commitTxn(this);
    }

    @Override
    public synchronized void rollback() {
        assertStatus(TxnStatus.PENDING);
        swift.discardTxn(this);
        status = TxnStatus.CANCELLED;
    }

    public synchronized TxnStatus getStatus() {
        return status;
    }

    @Override
    public synchronized TripleTimestamp nextTimestamp() {
        assertStatus(TxnStatus.PENDING);
        return timestampSource.generateNew();
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTOperation<V> op) {
        assertStatus(TxnStatus.PENDING);

        @SuppressWarnings("unchecked")
        CRDTObjectOperationsGroup<V> operationsGroup = (CRDTObjectOperationsGroup<V>) localObjectOperations.get(id);
        if (operationsGroup == null) {
            operationsGroup = new CRDTObjectOperationsGroup<V>(id, getAllVisibleTransactionsClock(),
                    getLocalTimestamp(), null);
            localObjectOperations.put(id, operationsGroup);
        }
        operationsGroup.append(op);
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
        final CRDTObjectOperationsGroup<V> operationsGroup = new CRDTObjectOperationsGroup<V>(id,
                getAllVisibleTransactionsClock(), getLocalTimestamp(), creationState);
        if (localObjectOperations.put(id, operationsGroup) != null) {
            throw new IllegalStateException("Object creation operation was preceded by some another operation");
        }
    }

    /**
     * @return a reference to the list of all locally committed transactions
     *         visible to this transaction
     */
    synchronized Deque<TxnHandleImpl> getLocalVisibleTransactions() {
        return localVisibleTransactions;
    }

    /**
     * @return a reference to the clock representing all globally committed
     *         transactions visible to this transaction; this clock includes
     *         only global timestamps
     */
    synchronized CausalityClock getGlobalVisibleTransactionsClock() {
        return globalVisibleTransactionsClock;
    }

    /**
     * @return a copy of a clock representing both locally and globally
     *         committed transactions visible to this transaction
     */
    synchronized CausalityClock getAllVisibleTransactionsClock() {
        // WISHME: to improve performance, we could cache the clock and
        // invalidate it only on markFirstLocalVisibleTransactionGlobal() calls.
        final CausalityClock clock = globalVisibleTransactionsClock.clone();
        for (final TxnHandleImpl txn : localVisibleTransactions) {
            clock.record(txn.getLocalTimestamp());
        }
        return clock;
    }

    /**
     * @return stable, local timestamp of this transaction
     */
    Timestamp getLocalTimestamp() {
        return localTimestamp;
    }

    /**
     * @return currently assigned global timestamp; can be null if not set
     */
    synchronized Timestamp getGlobalTimestamp() {
        return globalTimestamp;
    }

    /**
     * Marks the first local visible transaction as global. This transition of a
     * dependent (visible) transaction can happen only when this transaction is
     * locally committed, so the internal state can be safely altered and
     * remains integral.
     */
    synchronized void markFirstLocalVisibleTransactionGlobal() {
        assertStatus(TxnStatus.COMMITTED_LOCAL);

        final TxnHandleImpl txn = localVisibleTransactions.removeFirst();
        txn.assertStatus(TxnStatus.COMMITTED_GLOBAL);
        final Timestamp oldTs = txn.getLocalTimestamp();
        final Timestamp newTs = txn.getGlobalTimestamp();
        globalVisibleTransactionsClock.record(newTs);

        for (final CRDTObjectOperationsGroup<?> ops : localObjectOperations.values()) {
            ops.replaceDependentTimestamp(oldTs, newTs);
        }
    }

    /**
     * Assigns a (new) global timestamp to this transaction.
     * 
     * @param globalTimestamp
     *            global timestamp for this transaction
     */
    synchronized void setGlobalTimestamp(final Timestamp globalTimestamp) {
        assertStatus(TxnStatus.COMMITTED_LOCAL);
        if (!localVisibleTransactions.isEmpty()) {
            throw new IllegalStateException("There is a local dependent transaction that was not globally committed");
        }
        this.globalTimestamp = globalTimestamp;
    }

    /**
     * Marks transaction as locally committed.
     */
    synchronized void markLocallyCommitted() {
        assertStatus(TxnStatus.PENDING);

        status = TxnStatus.COMMITTED_LOCAL;
    }

    /**
     * Marks transaction as globally committed, using currently assigned global
     * timestamp.
     */
    void markGloballyCommitted() {
        synchronized (this) {
            assertStatus(TxnStatus.COMMITTED_LOCAL);
            assertGlobalTimestampDefined();

            status = TxnStatus.COMMITTED_GLOBAL;
        }
        if (commitListener != null) {
            commitListener.onGlobalCommit(this);
        }
    }

    /**
     * @return a collection of operations group on objects updated by this
     *         transactions; these operation groups use local timestamp (
     *         {@link #getLocalTimestamp()}); the content of collection is
     *         mutable while transaction is pending
     */
    synchronized Collection<CRDTObjectOperationsGroup<?>> getAllLocalOperations() {
        return localObjectOperations.values();
    }

    /**
     * @return an operations group on object updated by this transaction; this
     *         operation group uses local timestamp (
     *         {@link #getLocalTimestamp()}); null if object is not updated by
     *         this transaction
     */
    synchronized CRDTObjectOperationsGroup<?> getObjectLocalOperations(CRDTIdentifier id) {
        return localObjectOperations.get(id);
    }

    /**
     * @return a copy of collection of operations group on objects updated by
     *         this transactions; these operation groups use a global timestamp
     *         ( {@link #getGlobalTimestamp()})
     * @throws IllegalStateException
     *             when global timestamp is undefined for this transaction
     */
    synchronized Collection<CRDTObjectOperationsGroup<?>> getAllGlobalOperations() {
        assertGlobalTimestampDefined();

        final List<CRDTObjectOperationsGroup<?>> objectOperationsGlobal = new LinkedList<CRDTObjectOperationsGroup<?>>();
        for (final CRDTObjectOperationsGroup<?> localGroup : localObjectOperations.values()) {
            objectOperationsGlobal.add(localGroup.withBaseTimestamp(globalTimestamp));
        }
        return objectOperationsGlobal;
    }

    synchronized void assertStatus(final TxnStatus expectedStatus) {
        if (status != expectedStatus) {
            throw new IllegalStateException("Unexpected transaction status: was " + status + ", expected "
                    + expectedStatus);
        }
    }

    private void assertGlobalTimestampDefined() {
        if (globalTimestamp == null) {
            throw new IllegalStateException("Global timestamp is not yet defined");
        }
    }
}
