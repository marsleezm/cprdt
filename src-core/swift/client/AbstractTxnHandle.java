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
package swift.client;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.cprdt.FractionShardQuery;
import swift.cprdt.FullShardQuery;
import swift.cprdt.HollowShardQuery;
import swift.cprdt.core.CRDTShardQuery;
import swift.crdt.core.BulkGetProgressListener;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTUpdate;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.TxnHandle;
import swift.crdt.core.TxnStatus;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.DummyLog;
import swift.utils.TransactionsLog;
import sys.stats.Stats;
import sys.stats.StatsConstants;
import sys.stats.StatsImpl;
import sys.stats.sources.CounterSignalSource;
import sys.stats.sources.ValueSignalSource;
import sys.stats.sources.ValueSignalSource.Stopper;
import sys.utils.Threading;

/**
 * Implementation of abstract SwiftCloud transaction with unspecified isolation
 * level. It keeps track of its state and mediates between the application and
 * {@link TxnManager} communicating with the store. Implementations define the
 * read algorithm for CRDT and transaction dependencies, and should ensure
 * session guarantees between transactions.
 * <p>
 * Transaction is <em>read-only transaction</em> if it does not issue any update
 * operation; transaction is <em>update transaction</em> otherwise. Update
 * transaction uses timestamp for its updates.
 * <p>
 * A transaction is first locally committed with a client timestamp, and then
 * globally committed with a stable system timestamp assigned by server to
 * facilitate efficient timestamps summary. The mapping between these timestamps
 * is defined within a TimestampMapping object of the transaction.
 * <p>
 * This base implementation primarily keeps track of transaction states and
 * updates on objects.
 * <p>
 * Instances shall be generated using and managed by {@link TxnManager}
 * implementation. Thread-safe.
 * 
 * @author mzawirski
 */
abstract class AbstractTxnHandle implements TxnHandle, Comparable<AbstractTxnHandle> {

    protected final TxnManager manager;
    protected final boolean readOnly;
    protected final IsolationLevel isolationLevel;
    protected CachePolicy cachePolicy;
    protected final TimestampMapping timestampMapping;
    protected final CausalityClock updatesDependencyClock;
    protected final IncrementalTripleTimestampGenerator timestampSource;
    protected final Map<CRDTIdentifier, CRDTObjectUpdatesGroup<?>> localObjectOperations;
    // Operations that might need to be applied on the additional parts we fetch
    protected final Map<CRDTIdentifier, CRDTObjectUpdatesGroup<?>> notFullyAppliedOperations;
    protected TxnStatus status;
    protected CommitListener commitListener;
    protected final Map<CRDT<?>, ObjectUpdatesListener> objectUpdatesListeners;
    protected final TransactionsLog durableLog;
    protected final long id;
    protected final long serial;

    protected final String sessionId;
    protected static final AtomicLong serialGenerator = new AtomicLong();
    private CounterSignalSource locallyCommitCountStats;
    private CounterSignalSource unstableCommitCountStats;
    private ValueSignalSource unstableCommitDurationStats;
    private Stopper unstableGlocalCron;

    final protected Map<CRDTIdentifier, CRDT<?>> objectViewsCache;
    final protected Map<CRDTIdentifier, CRDT<?>> checkpointCache;
    final protected Map<CRDTIdentifier, Set<CRDTShardQuery<?>>> objectQueriesCache;
    final protected Set<CRDTIdentifier> toCreate;
    final protected Set<CRDTIdentifier> fetched;
    final protected Set<CRDTIdentifier> notFound;

    /**
     * Creates an update transaction.
     * 
     * @param manager
     *            manager maintaining this transaction
     * @param sessionId
     *            id of the client session issuing this transaction
     * @param durableLog
     *            durable log for recovery
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param timestampMapping
     *            timestamp and timestamp mapping information used for all
     *            updates of this transaction
     */
    AbstractTxnHandle(final TxnManager manager, final String sessionId, final TransactionsLog durableLog,
            final IsolationLevel isolationLevel, final CachePolicy cachePolicy,
            final TimestampMapping timestampMapping, Stats stats) {
        this.manager = manager;
        this.readOnly = false;
        this.sessionId = sessionId;
        this.durableLog = durableLog;
        this.id = timestampMapping.getClientTimestamp().getCounter();
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.timestampMapping = timestampMapping;
        this.updatesDependencyClock = ClockFactory.newClock();
        this.timestampSource = new IncrementalTripleTimestampGenerator(timestampMapping.getClientTimestamp());
        this.localObjectOperations = new HashMap<CRDTIdentifier, CRDTObjectUpdatesGroup<?>>();
        this.notFullyAppliedOperations = new HashMap<CRDTIdentifier, CRDTObjectUpdatesGroup<?>>();
        this.status = TxnStatus.PENDING;
        this.objectUpdatesListeners = new HashMap<CRDT<?>, ObjectUpdatesListener>();
        this.serial = serialGenerator.getAndIncrement();
        
        this.objectViewsCache = new ConcurrentHashMap<CRDTIdentifier, CRDT<?>>();
        this.checkpointCache = new ConcurrentHashMap<CRDTIdentifier, CRDT<?>>();
        this.objectQueriesCache = new HashMap<CRDTIdentifier, Set<CRDTShardQuery<?>>>();
        this.toCreate = Collections.newSetFromMap(new ConcurrentHashMap<CRDTIdentifier, Boolean>());
        this.fetched = Collections.newSetFromMap(new ConcurrentHashMap<CRDTIdentifier, Boolean>());
        this.notFound = Collections.newSetFromMap(new ConcurrentHashMap<CRDTIdentifier, Boolean>());
        
        initStats(stats);
    }

    /**
     * Creates a read-only transaction.
     * 
     * @param manager
     *            manager maintaining this transaction
     * @param sessionId
     *            id of the client session issuing this transaction
     * @param durableLog
     *            durable log for recovery
     * @param cachePolicy
     *            cache policy used by this transaction
     */
    AbstractTxnHandle(final TxnManager manager, final String sessionId, final IsolationLevel isolationLevel,
            final CachePolicy cachePolicy, Stats stats) {
        this.manager = manager;
        this.readOnly = true;
        this.sessionId = sessionId;
        this.durableLog = new DummyLog();
        this.id = -1;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.timestampMapping = null;
        this.updatesDependencyClock = ClockFactory.newClock();
        this.timestampSource = null;
        this.localObjectOperations = new HashMap<CRDTIdentifier, CRDTObjectUpdatesGroup<?>>();
        this.notFullyAppliedOperations = new HashMap<CRDTIdentifier, CRDTObjectUpdatesGroup<?>>();
        this.status = TxnStatus.PENDING;
        this.objectUpdatesListeners = new HashMap<CRDT<?>, ObjectUpdatesListener>();

        this.objectViewsCache = new ConcurrentHashMap<CRDTIdentifier, CRDT<?>>();
        this.checkpointCache = new ConcurrentHashMap<CRDTIdentifier, CRDT<?>>();
        this.objectQueriesCache = new HashMap<CRDTIdentifier, Set<CRDTShardQuery<?>>>();
        this.toCreate = Collections.newSetFromMap(new ConcurrentHashMap<CRDTIdentifier, Boolean>());
        this.fetched = Collections.newSetFromMap(new ConcurrentHashMap<CRDTIdentifier, Boolean>());
        this.notFound = Collections.newSetFromMap(new ConcurrentHashMap<CRDTIdentifier, Boolean>());

        this.serial = serialGenerator.getAndIncrement();
        initStats(stats);
    }

    @Override
    public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        return get(id, create, classOfV, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener listener) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        return get(id, create, classOfV, listener, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV, boolean lazy)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        return get(id, create, classOfV, null, lazy);
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener listener, boolean lazy) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        assertStatus(TxnStatus.PENDING);
        
        if (isReadOnly() && create) {
            throw new IllegalArgumentException("Create is true on a read-only transaction");
        }
        
        try {
            if (SwiftImpl.DEFAULT_LISTENER_FOR_GET && listener == null)
                listener = SwiftImpl.DEFAULT_LISTENER;
            
            CRDT<V> localView = (CRDT<V>) objectViewsCache.get(id);
            CRDTShardQuery<V> query = new FullShardQuery<V>();
            if (lazy) {
                if (localView != null) {
                    return (V) localView;
                }
                query = new HollowShardQuery<V>();
            }
            
            V view = (V) getImpl(id, create, classOfV, listener, query, lazy);
            if (!lazy) {
                fetched.add(id);
            }
            return view;
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
    }
    
    @Override
    public <V extends CRDT<V>> void fetch(CRDTIdentifier id, Class<V> classOfV, Set<?> particles) throws WrongTypeException, VersionNotFoundException, NetworkException {
        fetch(id, classOfV, new FractionShardQuery<V>(particles), null);
    }

    public <V extends CRDT<V>> void fetch(CRDTIdentifier id, Class<V> classOfV, CRDTShardQuery<V> query)
            throws WrongTypeException, VersionNotFoundException, NetworkException {
        fetch(id, classOfV, query, null);
    }

    public <V extends CRDT<V>> void fetch(CRDTIdentifier id, Class<V> classOfV, CRDTShardQuery<V> query,
            ObjectUpdatesListener updatesListener) throws WrongTypeException,
            VersionNotFoundException, NetworkException {
        V localView = (V) this.objectViewsCache.get(id);
        if (localView == null) {
            throw new IllegalStateException("Doing a fetch() without a previous get()");
        }
        
        if (query.isAvailableIn(localView.getShard())) {
            // No need to fetch anything, we already have it
            return;
        }
        // Check the previously made queries
        Set<CRDTShardQuery<?>> cachedQueries = objectQueriesCache.get(id);
        if (cachedQueries != null) {
            for (CRDTShardQuery<?> cachedQuery: cachedQueries) {
                if (query.isSubqueryOf((CRDTShardQuery<V>) cachedQuery)) {
                    return;
                }
            }
        }
        try {
            getImpl(id, true, classOfV, updatesListener, query, false);
        } catch (NoSuchObjectException e) {
            throw new IllegalStateException("Object not found during fetch");
        }
        
        fetched.add(id);
        
        if (!query.isAvailableIn(localView.getShard())) {
            // We cache the query to know we already did it
            // (only if it's a complex query that can't be compared with the shard)
            Set<CRDTShardQuery<?>> queries = objectQueriesCache.get(id);
            if (queries == null) {
                queries = new HashSet<CRDTShardQuery<?>>();
                objectQueriesCache.put(id, queries);
            }
            queries.add(query);
        }
    }
    
    public <V extends CRDT<V>> boolean objectIsFound(CRDTIdentifier id, Class<V> classOfV) throws WrongTypeException,
    VersionNotFoundException, NetworkException {
        V localView = (V) this.objectViewsCache.get(id);
        if (localView == null) {
            throw new IllegalStateException("Checking if an object was found without a previous get");
        }
        if (notFound.contains(id)) {
            return false;
        }
        if (fetched.contains(id)) {
            return true;
        }
        try {
            getImpl(id, false, classOfV, null, new HollowShardQuery<V>(), false);
            
            fetched.add(id);
        } catch (NoSuchObjectException e) {
            notFound.add(id);
            return false;
        }
        return true;
    }
    
    /**
     * With lazy fetching, a (blind) update can be registered even if it affects a part not in the shard
     * A subsequent fetch might add an affected part to the shard
     * which means this update should now be applied
     * 
     * @param id
     * @param localView
     */
    protected <V extends CRDT<V>> void updateWithNotFullyAppliedOperations(CRDTIdentifier id, V localView) {
        @SuppressWarnings("unchecked")
        CRDTObjectUpdatesGroup<V> toApplyOperationsGroup = (CRDTObjectUpdatesGroup<V>) notFullyAppliedOperations
                .get(id);
        if (toApplyOperationsGroup != null) {
            toApplyOperationsGroup.applyAndRemove(localView);
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
        for (CRDTIdentifier id: toCreate) {
            if (!fetched.contains(id)) {
                registerObjectCreation(id, (CRDT) checkpointCache.get(id));
            }
        }
        this.commitListener = listener;
        manager.commitTxn(this);
    }

    @Override
    public synchronized void rollback() {
        assertStatus(TxnStatus.PENDING);
        status = TxnStatus.CANCELLED;
        logStatusChange();
        manager.discardTxn(this);
    }

    protected void logStatusChange() {
        durableLog.writeEntry(getId(), status);
    }

    public synchronized TxnStatus getStatus() {
        return status;
    }

    @Override
    public synchronized TripleTimestamp nextTimestamp() {
        assertStatus(TxnStatus.PENDING);
        assertNotReadOnly();
        return timestampSource.generateNew();
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTUpdate<V> op) {
        assertStatus(TxnStatus.PENDING);
        assertNotReadOnly();

        @SuppressWarnings("unchecked")
        CRDTObjectUpdatesGroup<V> operationsGroup = (CRDTObjectUpdatesGroup<V>) localObjectOperations.get(id);
        if (operationsGroup == null) {
            operationsGroup = new CRDTObjectUpdatesGroup<V>(id, getTimestampMapping(), null,
                    getUpdatesDependencyClock());
            localObjectOperations.put(id, operationsGroup);
        }
        operationsGroup.append(op);
        durableLog.writeEntry(getId(), op);

        V localView = (V) this.objectViewsCache.get(id);
        if (localView == null || !localView.getShard().containsAll(op.affectedParticles())) {
            // This operation affects some particles that are not in the local
            // view
            // It needs to be applied to the affected particles if they are
            // requested later
            @SuppressWarnings("unchecked")
            CRDTObjectUpdatesGroup<V> toApplyOperationsGroup = (CRDTObjectUpdatesGroup<V>) notFullyAppliedOperations
                    .get(id);
            if (toApplyOperationsGroup == null) {
                toApplyOperationsGroup = new CRDTObjectUpdatesGroup<V>(id, getTimestampMapping(), null,
                        getUpdatesDependencyClock());
                notFullyAppliedOperations.put(id, toApplyOperationsGroup);
            }
            toApplyOperationsGroup.append(op);
        }
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
        notFound.add(id);
        
        if (isReadOnly()) {
            return;
        }
        
        CRDTObjectUpdatesGroup<V> opGroup = (CRDTObjectUpdatesGroup<V>) localObjectOperations.get(id);
        
        if (toCreate.contains(id) && opGroup != null) {
            if (opGroup.hasCreationState()) {
                return;
            }
            opGroup.setCreationState(creationState);
        } else {
            final CRDTObjectUpdatesGroup<V> operationsGroup = new CRDTObjectUpdatesGroup<V>(id, timestampMapping,
                    creationState, getUpdatesDependencyClock());
            if (localObjectOperations.put(id, operationsGroup) != null) {
                throw new IllegalStateException("Object creation operation was preceded by some another operation");
            }
        }
        durableLog.writeEntry(getId(), id);
    }

    /**
     * @return timestamp mapping used by all updates of this transaction; note
     *         that it may be mutated
     */
    TimestampMapping getTimestampMapping() {
        return timestampMapping;
    }

    Timestamp getClientTimestamp() {
        return getTimestampMapping().getClientTimestamp();
    }

    /**
     * Marks transaction as locally committed.
     */
    synchronized void markLocallyCommitted() {
        assertStatus(TxnStatus.PENDING);
        status = TxnStatus.COMMITTED_LOCAL;
        logStatusChange();
        // Flush the log before returning to the client call.
        durableLog.flush();
        locallyCommitCountStats.incCounter();
        unstableGlocalCron = unstableCommitDurationStats.createEventDurationSignal();
    }

    /**
     * Adds a (new) system timestamp to this transaction and marks transaction
     * as globally committed.
     * 
     * @param globalTimestamp
     *            a system timestamp for this transaction; ignored for read-only
     *            transaction
     */
    void markGloballyCommitted(final Timestamp systemTimestamp) {
        boolean justGloballyCommitted = false;
        synchronized (this) {
            assertStatus(TxnStatus.COMMITTED_LOCAL, TxnStatus.COMMITTED_GLOBAL);
            if (status == TxnStatus.COMMITTED_LOCAL) {
                justGloballyCommitted = true;
                status = TxnStatus.COMMITTED_GLOBAL;

                // include Read-Only transactions
                unstableCommitCountStats.incCounter();
                unstableGlocalCron.stop();

            }
            if (systemTimestamp != null) {
                timestampMapping.addSystemTimestamp(systemTimestamp);
            }
        }
        if (systemTimestamp != null) {
            durableLog.writeEntry(getId(), systemTimestamp);
        }
        logStatusChange();
        if (justGloballyCommitted) {
            if (commitListener != null) {
                // TODO: wouldn't it be safer to call it from a different
                // thread?
                commitListener.onGlobalCommit(this);
            }
        }
    }

    /**
     * @return true when the transaction is read-only
     */
    boolean isReadOnly() {
        return readOnly;
    }

    /**
     * @return a collection of update operations group on objects updated by
     *         this transactions; empty for read-only transaction
     */
    synchronized Collection<CRDTObjectUpdatesGroup<?>> getAllUpdates() {
        assertStatus(TxnStatus.CANCELLED, TxnStatus.COMMITTED_LOCAL, TxnStatus.COMMITTED_GLOBAL);
        return localObjectOperations.values();
    }

    /**
     * @return an update operations group on object updated by this transaction;
     *         null if object is not updated by this transaction
     */
    synchronized CRDTObjectUpdatesGroup<?> getObjectUpdates(CRDTIdentifier id) {
        assertStatus(TxnStatus.COMMITTED_LOCAL, TxnStatus.COMMITTED_GLOBAL);
        return localObjectOperations.get(id);
    }

    synchronized CausalityClock getUpdatesDependencyClock() {
        return updatesDependencyClock;
    }

    String getSessionId() {
        return sessionId;
    }

    /**
     * Implementation of read request, can use {@link #manager} for that
     * purposes. Implementation is responsible for maintaining dependency clock
     * of the transaction using
     * {@link #updateUpdatesDependencyClock(CausalityClock)} to ensure that it
     * depends on every version read by the transaction.
     */
    protected abstract <V extends CRDT<V>> V getImpl(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener updatesListener, CRDTShardQuery<V> query, boolean createLocally) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException;

    /**
     * Updates dependency clock of the transaction.
     * 
     * @param clock
     *            clock to include in the dependency clock of this transaction
     * @throws IllegalStateException
     *             when transaction is not pending or locally committed
     * @return true if the provided clock included some new events
     */
    protected boolean updateUpdatesDependencyClock(final CausalityClock clock) {
        assertStatus(TxnStatus.PENDING, TxnStatus.COMMITTED_LOCAL);
        if (updatesDependencyClock.merge(clock).is(CMP_CLOCK.CMP_CONCURRENT, CMP_CLOCK.CMP_ISDOMINATED)) {
            durableLog.writeEntry(getId(), clock);
            return true;
        }
        return false;
    }

    protected synchronized void assertStatus(final TxnStatus... expectedStatuses) {
        for (final TxnStatus expectedStatus : expectedStatuses) {
            if (status == expectedStatus) {
                return;
            }
        }
        throw new IllegalStateException("Unexpected transaction status: was " + status + ", expected "
                + Arrays.asList(expectedStatuses));
    }

    protected void assertNotReadOnly() {
        if (readOnly) {
            throw new IllegalStateException("update request for read-only transaction");
        }
    }

    @Override
    public String toString() {
        return (readOnly ? "read-only" : "update") + " transaction ts=" + timestampMapping;
    }

    protected long getId() {
        return id;
    }

    protected long getSerial() {
        return serial;
    }

    @Override
    public int compareTo(AbstractTxnHandle o) {
        return Long.signum(orderingScore() - o.orderingScore());
    }

    private long orderingScore() {
        return getTimestampMapping() == null ? 0 : getTimestampMapping().getClientTimestamp().getCounter();
    }

    @Override
    public Map<CRDTIdentifier, CRDT<?>> bulkGet(Set<CRDTIdentifier> ids, final BulkGetProgressListener listener) {
        final Map<CRDTIdentifier, CRDT<?>> res = Collections.synchronizedMap(new HashMap<CRDTIdentifier, CRDT<?>>());

        if (ids.isEmpty())
            return res;

        for (final CRDTIdentifier i : ids)
            execute(new Runnable() {
                @Override
                public void run() {
                    CRDT<?> val;
                    try {
                        val = get(i, false, null);
                        res.put(i, val);
                        if (listener != null)
                            listener.onGet(AbstractTxnHandle.this, i, val);
                    } catch (Exception e) {
                        e.printStackTrace();
                        res.put(i, null);
                    }
                    Threading.synchronizedNotifyAllOn(res);
                }
            });

        while (res.size() != ids.size()) {
            Threading.synchronizedWaitOn(res, 100);
        }
        return res;
    }

    public Map<CRDTIdentifier, CRDT<?>> bulkGet(CRDTIdentifier... crdtIdentifiers) {
        Set<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>(Arrays.asList(crdtIdentifiers));
        return this.bulkGet(ids, null);
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    private void execute(Runnable r) {
        ((SwiftImpl) manager).execute(r);
    }

    private void initStats(Stats stats) {
        locallyCommitCountStats = stats.getCountingSourceForStat("transactions-local-commit");
        unstableCommitCountStats = stats.getCountingSourceForStat("transactions-unstable-commit");
        unstableCommitDurationStats = stats.getValuesFrequencyOverTime("transactions-unstable-commit-duration",
                StatsConstants.UNSTABLE_COMMIT_DURATION);

    }
}
