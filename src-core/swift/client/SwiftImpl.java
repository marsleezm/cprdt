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

import static sys.net.api.Networking.Networking;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import swift.client.LRUObjectsCache.EvictionListener;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.cprdt.FullShardQuery;
import swift.cprdt.HollowShardQuery;
import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.SwiftScout;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.crdt.core.TxnStatus;
import swift.dc.DCConstants;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.proto.BatchCommitUpdatesReply;
import swift.proto.BatchCommitUpdatesRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesRequest;
import swift.proto.FetchObjectVersionReply;
import swift.proto.FetchObjectVersionReply.FetchStatus;
import swift.proto.FetchObjectVersionRequest;
import swift.proto.LatestKnownClockReply;
import swift.proto.LatestKnownClockRequest;
import swift.proto.MetadataStatsCollector;
import swift.proto.PingReply;
import swift.proto.PingRequest;
import swift.proto.SwiftProtocolHandler;
import swift.pubsub.BatchUpdatesNotification;
import swift.pubsub.ScoutPubSubService;
import swift.utils.DummyLog;
import swift.utils.KryoDiskLog;
import swift.utils.NoFlushLogDecorator;
import swift.utils.TransactionsLog;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.stats.DummyStats;
import sys.stats.Stats;
import sys.stats.StatsConstants;
import sys.stats.StatsImpl;
import sys.stats.sources.CounterSignalSource;
import sys.stats.sources.PollingBasedValueProvider;
import sys.stats.sources.ValueSignalSource;
import sys.utils.Threading;

/**
 * Implementation of Swift scout and transactions manager. Scout can either
 * support one or multiple client sessions.
 * 
 * @see Swift, TxnManager
 * @author mzawirski
 */
// DISCLAIMER: this class is an unmanageable monolithic monster, as a result of
// continued design decision changes over the project. It includes many legacy
// options that contributed to the complexity.
// The focus of the current version is on providing SNAPSHOT_ISOLATION+CACHED
// transaction mode, although an unrelated legacy code may coexist.
// TODO: drop legacy functionality, divide into smaller components.
public class SwiftImpl implements SwiftScout, TxnManager, FailOverHandler {
    // Temporary Nuno's hack:
    public static final boolean DEFAULT_LISTENER_FOR_GET = false;
    static ObjectUpdatesListener DEFAULT_LISTENER = new AbstractObjectUpdatesListener() {
        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, CRDT<?> previousValue) {
            // do nothing
        };
    };

    // Experimental feature: share dependency clock in a commit batch to reduce
    // metadata size.
    public static final boolean USE_SHARED_DEPENDENCIES_IN_COMMIT_BATCH = true;

    // TODO: complete server failover

    // WISHME: notifications are quite CPU/memory scans-intensive at the
    // moment; consider more efficient implementation if this is an issue
    // (especially for scenarios where dummy TxnHandle.UPDATES_SUBSCRIBER is
    // employed).

    // WISHME: This class uses very coarse-grained locking, but given the
    // complexity of causality tracking and timestamps remapping, unless we
    // prove it is a real issue for a client application, I would rather keep it
    // this way. In any case, locking should not affect responsiveness to
    // pendingTxn requests.

    // WISHME: subscribe updates of frequently accessed objects

    private static Logger logger = Logger.getLogger(SwiftImpl.class.getName());

    /**
     * Creates new single session instance backed by a scout.
     * 
     * @param options
     *            Swift scout options
     * @return instance of Swift client session
     * @see SwiftOptions
     */
    public static SwiftSession newSingleSessionInstance(final SwiftOptions options) {
        Endpoint[] servers = parseEndpoints(options.getServerHostname());
        final SwiftScout sharedImpl = new SwiftImpl(Networking.rpcConnect().toDefaultService(), servers,
                new LRUObjectsCache(options.getCacheEvictionTimeMillis(), options.getCacheSize(), options.getQueryCacheTimeThreshold(), options.getCacheSizeLimit()), options);
        return sharedImpl.newSession("singleton-session");
    }

    /**
     * Creates a new scout that allows many open sessions.
     * 
     * @param options
     *            Swift scout options
     * @return instance of Swift client session
     * @see SwiftOptions
     */
    public static SwiftScout newMultiSessionInstance(final SwiftOptions options) {
        Endpoint[] servers = parseEndpoints(options.getServerHostname());

        return new SwiftImpl(Networking.rpcConnect().toDefaultService(), servers, new LRUObjectsCache(
                options.getCacheEvictionTimeMillis(), options.getCacheSize(), options.getQueryCacheTimeThreshold(), options.getCacheSizeLimit()), options);
    }

    private static String generateScoutId() {
        final UUID uuid = UUID.randomUUID();
        final byte[] uuidBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE * 2).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
        final String result = DatatypeConverter.printBase64Binary(uuidBytes);
        // FIXME: measurement hack, shrink the String size, since we encode it
        // inefficiently (we should have a separate ClientId class).
        // Realistically, these 4 UTF-16 chars represent what's needed with a
        // decent encoding.
        return result.substring(0, 4);
    }

    volatile private boolean stopFlag;
    private boolean stopGracefully;

    private final String scoutId;
    private final RpcEndpoint localEndpoint;
    private final Endpoint[] serverEndpoints;

    // Cache of objects.
    // Invariant: if object is in the cache, it includes all updates of locally
    // and globally committed locally-originating transactions.
    // private final TimeSizeBoundedObjectsCache objectsCache;

    private final LRUObjectsCache objectsCache;

    // CLOCKS: all clocks grow over time. Careful with references, use copies.

    // A clock known to be committed at the store.
    private final CausalityClock committedVersion;
    // A clock known to be committed at the store and eventually observable
    // across the system even in case of disaster affecting part of the store.
    private final CausalityClock committedDisasterDurableVersion;
    // Last locally committed txn clock + dependencies.
    // Attenzione attenzione! Can be slightly overestimated when
    // concurrentOpenTransactions = true, but it shouldn't hurt since cache will
    // not contain local transactions before they are committed.
    private CausalityClock lastLocallyCommittedTxnClock;
    // Last globally committed txn clock + dependencies.
    // Commented-out to avoid holes on the wire.
    // private CausalityClock lastGloballyCommittedTxnClock;
    // Generator of local timestamps.
    private final ReturnableTimestampSourceDecorator<Timestamp> clientTimestampGenerator;

    // Set of versions for fetch requests in progress.
    // private final Set<CausalityClock> fetchVersionsInProgress;
    private final Set<CausalityClock> fetchVersionsInProgress;

    private Set<AbstractTxnHandle> pendingTxns;
    // Locally committed transactions (in begin-txn order), the first one is
    // possibly committing to the store.
    private final SortedSet<AbstractTxnHandle> locallyCommittedTxnsOrderedQueue;
    // Globally committed local transactions (in commit order), but possibly not
    // stable, i.e. not distaster-safe in the store.
    private final LinkedList<AbstractTxnHandle> globallyCommittedUnstableTxns;

    // Thread sequentially committing transactions from the queue.
    private final CommitterThread committerThread;

    // Update subscriptions stuff.
    // id -> sessionId -> update subscription information; the presence of any
    // first-level mapping corresponds roughly to the fact that
    // updates are currently subscribed; if there is any 2nd level mapping, it
    // means that a listener is additionally installed and awaits notification
    private final Map<CRDTIdentifier, Map<String, UpdateSubscriptionWithListener>> objectSessionsUpdateSubscriptions;
    // map from timestamp mapping of an uncommitted update to objects that may
    // await notification with this mapping
    private final Map<TimestampMapping, Set<CRDTIdentifier>> uncommittedUpdatesObjectsToNotify;

    private final ExecutorService executorService;

    private final TransactionsLog durableLog;

    // OPTIONS
    private final int deadlineMillis;
    // If true, only disaster safe committed (and local) transactions are read
    // by transactions, so the scout virtually never blocks due to systen
    // failures.
    private final boolean disasterSafe;
    // If true, multiple transactions can be open. Note that (1) transactions
    // are always committed in the order of beginTxn() calls, not commit(), and
    // (2) with the enabled option, all update transactions need to be committed
    // even if they made no updates or rolled back.
    private final boolean concurrentOpenTransactions;
    // Maximum number of asynchronous transactions queued locally before they
    // block the application.
    private final int maxAsyncTransactionsQueued;
    // Maximum number of transactions in a single commit request to the store.
    private final int maxCommitBatchSize;

    // TODO Q: what is the semantics/role w.r.t. to
    // objectSessionsUpdateSubscriptions?
    private final ScoutPubSubService scoutPubSub;

    // TODO: track stable global commits
    private Stats stats;
    private final CoarseCacheStats cacheStats;
    private MetadataStatsCollector metadataStatsCollector;

    private CounterSignalSource ongoingObjectFetchesStats;
    private ValueSignalSource batchSizeOnCommitStats;
    private CausalityClock nextPruneClock;

    SortedSet<CRDTIdentifier> notified = new TreeSet<CRDTIdentifier>();
    private final boolean assumeAtomicCausalNotifications;
    
    // Latest version that was requested (if the object is not in the cache)
    private final Map<CRDTIdentifier, CausalityClock> requestedVersions;

    SwiftImpl(final RpcEndpoint localEndpoint, final Endpoint[] serverEndpoints, final LRUObjectsCache objectsCache,
            final SwiftOptions options) {
        this.scoutId = generateScoutId();
        this.concurrentOpenTransactions = options.isConcurrentOpenTransactions();
        this.maxAsyncTransactionsQueued = options.getMaxAsyncTransactionsQueued();
        this.disasterSafe = options.isDisasterSafe();
        this.deadlineMillis = options.getDeadlineMillis();
        this.maxCommitBatchSize = options.getMaxCommitBatchSize();
        this.localEndpoint = localEndpoint;
        this.serverEndpoints = serverEndpoints;
        this.objectsCache = objectsCache;
        this.assumeAtomicCausalNotifications = options.assumeAtomicCausalNotifications();

        if (options.isEnableStatistics()) {
            this.stats = StatsImpl.getInstance("scout-" + scoutId, StatsImpl.SAMPLING_INTERVAL_MILLIS,
                    options.getStatisticsOuputDir(), options.getStatisticsOverwriteDir());
        } else {
            this.stats = new DummyStats();
        }
        this.cacheStats = new CoarseCacheStats(stats, objectsCache);

        this.metadataStatsCollector = options.getMetadataStatsCollector();

        this.locallyCommittedTxnsOrderedQueue = new TreeSet<AbstractTxnHandle>();
        this.globallyCommittedUnstableTxns = new LinkedList<AbstractTxnHandle>();
        this.lastLocallyCommittedTxnClock = ClockFactory.newClock();
        // this.lastGloballyCommittedTxnClock = ClockFactory.newClock();
        this.committedDisasterDurableVersion = ClockFactory.newClock();

        this.committedVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new ReturnableTimestampSourceDecorator<Timestamp>(
                new IncrementalTimestampGenerator(scoutId));

        this.pendingTxns = new HashSet<AbstractTxnHandle>();
        this.committerThread = new CommitterThread();
        this.fetchVersionsInProgress = new HashSet<CausalityClock>();
        this.committerThread.start();
        this.objectSessionsUpdateSubscriptions = new HashMap<CRDTIdentifier, Map<String, UpdateSubscriptionWithListener>>();
        this.uncommittedUpdatesObjectsToNotify = new HashMap<TimestampMapping, Set<CRDTIdentifier>>();

        this.executorService = Executors.newFixedThreadPool(8, Threading.factory("Client"));

        localEndpoint.setHandler(new SwiftProtocolHandler());

        this.scoutPubSub = new ScoutPubSubService(scoutId, disasterSafe, serverEndpoint(), metadataStatsCollector) {
            private int i;

            public void onNotification(final BatchUpdatesNotification batch) {
                // FIXME: handle violated FIFO channel?
                for (Entry<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>> entry : batch.getObjectsUpdates()
                        .entrySet()) {
                    applyObjectUpdates(entry.getKey(), entry.getValue());
                }
                objectsCache.augmentAllWithDCCausalClockWithoutMappings(batch.getNewVersion());
                updateCommittedVersions(batch.isNewVersionDisasterSafe() ? null : batch.getNewVersion(),
                        batch.isNewVersionDisasterSafe() ? batch.getNewVersion() : null);
                if (i++ % 50 == 0) {
                    System.err.println("Committed DC vector: " + getGlobalCommittedVersion(false));
                    System.err.println("Last local committed vector: " + lastLocallyCommittedTxnClock);
                }
                batch.recordMetadataSample(metadataStatsCollector);

                tryPrune();
            }
        };

        this.objectsCache.setEvictionListener(new EvictionListener() {
            public void onEviction(CRDTIdentifier id) {
                scoutPubSub.unsubscribe(id);
            }
        });

        this.ongoingObjectFetchesStats = this.stats.getCountingSourceForStat("ongoing-object-fetches");

        this.stats.registerPollingBasedValueProvider("uncommited-updates-objects-to-notify",
                new PollingBasedValueProvider() {
                    @Override
                    public double poll() {
                        double count = 0;
                        synchronized (SwiftImpl.this) {
                            for (Entry<TimestampMapping, Set<CRDTIdentifier>> uncommittedUpdates : uncommittedUpdatesObjectsToNotify
                                    .entrySet()) {
                                count += uncommittedUpdates.getValue().size();
                            }
                        }
                        return count;
                    }
                }, StatsImpl.SAMPLING_INTERVAL_MILLIS);

        this.stats.registerPollingBasedValueProvider("pending-txns", new PollingBasedValueProvider() {

            @Override
            public double poll() {
                synchronized (SwiftImpl.this) {
                    return pendingTxns.size();
                }
            }
        }, StatsImpl.SAMPLING_INTERVAL_MILLIS);

        this.stats.registerPollingBasedValueProvider("locally-committed-txns-queue", new PollingBasedValueProvider() {

            @Override
            public double poll() {
                synchronized (SwiftImpl.this) {
                    return locallyCommittedTxnsOrderedQueue.size();
                }
            }
        }, StatsImpl.SAMPLING_INTERVAL_MILLIS);

        this.stats.registerPollingBasedValueProvider("global-committed-unstable-txns-queue",
                new PollingBasedValueProvider() {

                    @Override
                    public double poll() {
                        synchronized (SwiftImpl.this) {
                            return globallyCommittedUnstableTxns.size();
                        }
                    }
                }, StatsImpl.SAMPLING_INTERVAL_MILLIS);

        batchSizeOnCommitStats = this.stats.getValuesFrequencyOverTime("batch-size-on-commit",
                StatsConstants.BATCH_SIZE);

        TransactionsLog log = new DummyLog();
        if (options.getLogFilename() != null) {
            try {
                log = new KryoDiskLog(options.getLogFilename());
            } catch (FileNotFoundException x) {
                // TODO: Propagate the exception
                logger.warning("Could not create a log file " + options.getLogFilename() + " (using no log instead): "
                        + x);
            }
        }
        if (!options.isLogFlushOnCommit()) {
            log = new NoFlushLogDecorator(log);
        }
        this.durableLog = log;

        // TODO: make it configurable
        // new FailOverWatchDog().start();

        clockSkewEstimate();
        
        getDCClockEstimates();
        
        this.requestedVersions = new HashMap<CRDTIdentifier, CausalityClock>();
    }

    protected synchronized void tryPrune() {
        if (nextPruneClock != null) {
            // for (final AbstractTxnHandle txn : pendingTxns) {
            // if (txn.getUpdatesDependencyClock().compareTo(nextPruneClock)
            // .is(CMP_CLOCK.CMP_CONCURRENT, CMP_CLOCK.CMP_ISDOMINATED)) {
            // // Pruning is unsafe at this stage.
            // return;
            // }
            // }
            // objectsCache.pruneAll(nextPruneClock);
            // nextPruneClock = null;
        } else {
            nextPruneClock = getGlobalCommittedVersion(true);
        }
    }

    public void stop(boolean waitForCommit) {
        logger.info("Stopping scout");
        synchronized (this) {
            if (stopFlag) {
                logger.warning("Scout is already stopped");
                return;
            }
            if (!pendingTxns.isEmpty()) {
                logger.warning("Stopping while there are pending transactions!");

                try {
                    stats.dump();
                    stats.terminate();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return;
            }

            stopFlag = true;
            stopGracefully = waitForCommit;
            this.notifyAll();
        }
        try {
            committerThread.join();
            for (final CRDTIdentifier id : new ArrayList<CRDTIdentifier>(objectSessionsUpdateSubscriptions.keySet())) {
                removeUpdateSubscriptionAsyncUnsubscribe(id);
            }
            durableLog.close();
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
        }
        logger.info("scout stopped");
        cacheStats.printAndReset();

        try {
            stats.dump();
            stats.terminate();
        } catch (IOException e) {
            logger.warning("Couldn't write statistics file: cause: " + e);
        }
    }

    @Override
    public SwiftSession newSession(String sessionId) {
        return new SwiftSessionToScoutAdapter(this, sessionId);
    }

    @Override
    public void printAndResetCacheStats() {
        // TODO is it really what we want?
        cacheStats.printAndReset();
        // try {
        // Output output = new Output(new FileOutputStream("/dev/null"));
        // synchronized (objectsCache) {
        // KryoLib.kryo().writeObject(output, objectsCache);
        // }
        // output.close();
        // System.out.printf("CACHE SIZE: %s KB\n", output.total() >> 10);
        // } catch (Exception e) {
        // // e.printStackTrace();
        // }
    }

    private boolean clockSkewEstimate() {
        try {
            PingRequest request;
            PingReply reply;
            long rttAcum = 0;
            int numAcum = 0;
            long clockSkewPlusLatency = 0;
            for (int i = 0; i < 5; i++) {
                request = new PingRequest(scoutId, disasterSafe, System.nanoTime());
                reply = localEndpoint.request(serverEndpoint(), request);
                long receivedTime = System.nanoTime();
                if (reply != null) {
                    numAcum++;
                    rttAcum = rttAcum + receivedTime - reply.getTimeAtSender();
                    clockSkewPlusLatency = clockSkewPlusLatency + reply.getTimeAtReceiver() - reply.getTimeAtSender();
                }
            }
            long rtt = rttAcum / numAcum;
            long clockSkew = (clockSkewPlusLatency - rtt / 2) / numAcum;
            return numAcum > 0;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean getDCClockEstimates() {
        LatestKnownClockRequest request = new LatestKnownClockRequest(scoutId, disasterSafe);
        LatestKnownClockReply reply = localEndpoint.request(serverEndpoint(), request);

        if (reply != null) {
            reply.recordMetadataSample(metadataStatsCollector);
            reply.getDistasterDurableClock().intersect(reply.getClock());
            updateCommittedVersions(reply.getClock(), reply.getDistasterDurableClock());
            return true;
        } else
            return false;
    }

    public synchronized AbstractTxnHandle beginTxn(String sessionId, IsolationLevel isolationLevel,
            CachePolicy cachePolicy, boolean readOnly) throws NetworkException {
        if (!concurrentOpenTransactions && !pendingTxns.isEmpty()) {
            throw new IllegalStateException("Only one transaction can be executing at the time");
        }
        assertRunning();

        switch (isolationLevel) {
        case SNAPSHOT_ISOLATION:
            final CausalityClock snapshotClock;
            // TODO Q: is this flag really respected everywhere or should the
            // whole code assume only one option?
            if (assumeAtomicCausalNotifications && cachePolicy == CachePolicy.CACHED) {
                snapshotClock = getGlobalCommittedVersion(true);
            } else {
                if (cachePolicy == CachePolicy.MOST_RECENT || cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                    if (!getDCClockEstimates() && cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                        throw new NetworkException("timed out to get transaction snapshot point");
                    }
                }
                // Invariant: for SI snapshotClock of a new transaction
                // dominates
                // clock of all previous SI transaction (monotonic reads), since
                // commitedVersion only grows.
                snapshotClock = getGlobalCommittedVersion(true);
            }
            snapshotClock.merge(lastLocallyCommittedTxnClock);

            final SnapshotIsolationTxnHandle siTxn;
            if (readOnly) {
                siTxn = new SnapshotIsolationTxnHandle(this, sessionId, cachePolicy, snapshotClock, stats);
            } else {
                final TimestampMapping timestampMapping = generateNextTimestampMapping();
                siTxn = new SnapshotIsolationTxnHandle(this, sessionId, durableLog, cachePolicy, timestampMapping,
                        snapshotClock, stats);
            }
            addPendingTxn(siTxn);
            if (logger.isLoggable(Level.INFO)) {
                logger.info("SI " + siTxn + " started with snapshot point: " + snapshotClock);
            }
            return siTxn;

        case REPEATABLE_READS:
            // TODO Q: do we ever use RR in any recent experiments?
            final RepeatableReadsTxnHandle rrTxn;
            if (readOnly) {
                rrTxn = new RepeatableReadsTxnHandle(this, sessionId, cachePolicy, stats);
            } else {
                final TimestampMapping timestampMapping = generateNextTimestampMapping();
                rrTxn = new RepeatableReadsTxnHandle(this, sessionId, durableLog, cachePolicy, timestampMapping, stats);
            }
            addPendingTxn(rrTxn);
            if (logger.isLoggable(Level.INFO)) {
                logger.info("REPEATABLE READS  " + rrTxn + " started");
            }
            return rrTxn;

        case READ_COMMITTED:
        case READ_UNCOMMITTED:
            // TODO: implement!
        default:
            throw new UnsupportedOperationException("isolation level " + isolationLevel + " unsupported");
        }
    }

    void execute(Runnable r) {
        executorService.execute(r);
    }

    private TimestampMapping generateNextTimestampMapping() {
        return new TimestampMapping(clientTimestampGenerator.generateNew());
    }

    private void returnLastTimestamp() {
        // Return and reuse last timestamp to avoid holes in VV.
        clientTimestampGenerator.returnLastTimestamp();
    }

    private synchronized void updateCommittedVersions(final CausalityClock newCommittedVersion,
            final CausalityClock newCommittedDisasterDurableVersion) {
        boolean committedVersionUpdated = false;
        if (newCommittedVersion != null) {
            committedVersionUpdated = this.committedVersion.merge(newCommittedVersion).is(CMP_CLOCK.CMP_ISDOMINATED,
                    CMP_CLOCK.CMP_CONCURRENT);
        }
        boolean committedDisasterDurableUpdated = false;
        if (newCommittedDisasterDurableVersion != null) {
            committedDisasterDurableUpdated = this.committedDisasterDurableVersion.merge(
                    newCommittedDisasterDurableVersion).is(CMP_CLOCK.CMP_ISDOMINATED, CMP_CLOCK.CMP_CONCURRENT);
        }
        if (!committedVersionUpdated && !committedDisasterDurableUpdated) {
            // No changes.
            return;
        }

        // Find and clean new stable local txns logs that we won't need anymore.
        int stableTxnsToDiscard = 0;
        int evaluatedTxns = 0;
        for (final AbstractTxnHandle txn : globallyCommittedUnstableTxns) {
            final TimestampMapping txnMapping = txn.getTimestampMapping();
            if (txnMapping.hasSystemTimestamp()) {
                // FIXME: keep txn longer, until it is stable in all DCs
                boolean notNeeded = txnMapping.allSystemTimestampsIncluded(committedDisasterDurableVersion);
                for (final CausalityClock fetchedVersion : fetchVersionsInProgress) {
                    if (!txnMapping.allSystemTimestampsIncluded(fetchedVersion)) {
                        notNeeded = false;
                        break;
                    }
                }
                if (notNeeded) {
                    stableTxnsToDiscard = evaluatedTxns + 1;
                } else {
                    break;
                }
            } else {
                // The txn has unknown system timestamp, so we need to rely on
                // subsequent transactions to determine if it can be removed.
            }
            evaluatedTxns++;
        }
        for (int i = 0; i < stableTxnsToDiscard; i++) {
            globallyCommittedUnstableTxns.removeFirst();
        }

        // Go through updates to notify and see if any become committed.
        final Iterator<Entry<TimestampMapping, Set<CRDTIdentifier>>> iter = uncommittedUpdatesObjectsToNotify
                .entrySet().iterator();
        while (iter.hasNext()) {
            final Entry<TimestampMapping, Set<CRDTIdentifier>> entry = iter.next();
            if (entry.getKey().anyTimestampIncluded(getGlobalCommittedVersion(false))) {
                iter.remove();
                for (final CRDTIdentifier id : entry.getValue()) {
                    final Map<String, UpdateSubscriptionWithListener> subscriptions = objectSessionsUpdateSubscriptions
                            .get(id);
                    if (subscriptions != null) {
                        for (UpdateSubscriptionWithListener subscription : subscriptions.values()) {
                            executorService.execute(subscription.generateNotificationAndDiscard(this, id));
                        }
                    }
                }
            }
        }
    }

    private synchronized CausalityClock getGlobalCommittedVersion(boolean copy) {
        CausalityClock result;
        if (disasterSafe) {
            result = this.committedDisasterDurableVersion;
        } else {
            result = this.committedVersion;
        }

        if (copy) {
            result = result.clone();
        }
        return result;
    }

    @Override
    public <V extends CRDT<V>> V getObjectLatestVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CachePolicy cachePolicy, boolean create, Class<V> classOfV, final ObjectUpdatesListener updatesListener, CRDTShardQuery<V> query)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

        synchronized (this) {
            assertPendingTransaction(txn);

            if (cachePolicy == CachePolicy.CACHED) {
                try {
                    final V view = getCachedObjectForTxn(txn, id, create, null, classOfV, updatesListener, query, null, false);
                    cacheStats.addCacheHit(id);
                    return view;
                } catch (NoSuchObjectException x) {
                    cacheStats.addCacheMissNoObject(id);
                    // Ok, let's try to fetch then.
                } catch (VersionNotFoundException x) {
                    cacheStats.addCacheMissBizarre(id);
                    logger.info("No self-consistent version found in cache: " + x);
                }
            }
        }

        while (true) {

            final CausalityClock fetchClock;
            final boolean fetchStrictlyRequired;
            synchronized (this) {
                fetchStrictlyRequired = (cachePolicy == CachePolicy.STRICTLY_MOST_RECENT || objectsCache
                        .getAndTouch(id) == null);
                fetchClock = getGlobalCommittedVersion(true);
                // fetchClock.merge(lastLocallyCommittedTxnClock);
                // Try to get the latest one.
            }

            boolean fetchError = false;
            try {
                fetchObjectVersion(txn, id, create, classOfV, fetchClock, query, false, updatesListener != null);
            } catch (VersionNotFoundException x) {
                if (fetchStrictlyRequired) {
                    throw x;
                }
                fetchError = true;
            } catch (NetworkException x) {
                if (fetchStrictlyRequired) {
                    throw x;
                }
                fetchError = true;
            } catch (InterruptedException x) {
                if (fetchStrictlyRequired) {
                    throw new NetworkException("Scout was shut down while  fetching an object: " + x.getMessage());
                }
                fetchError = true;
            }
            // Pass other exceptions through.

            try {
                return getCachedObjectForTxn(txn, id, create, null, classOfV, updatesListener, query, null, !fetchError);
            } catch (NoSuchObjectException x) {
                logger.warning("Object not found in the cache just after fetch (retrying): " + x);
            } catch (VersionNotFoundException x) {
                logger.warning("No self-consistent version found in cache (retrying): " + x);
            }
        }
    }

    @Override
    public <V extends CRDT<V>> V getObjectVersionTxnView(AbstractTxnHandle txn, final CRDTIdentifier id,
            final CausalityClock version, final boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener, CRDTShardQuery<V> query) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        assertPendingTransaction(txn);

        try {
            final V view = getCachedObjectForTxn(txn, id, create, version, classOfV, updatesListener, query, version, false);
            cacheStats.addCacheHit(id);
            return view;
        } catch (NoSuchObjectException x) {
            cacheStats.addCacheMissNoObject(id);
            // Ok, let's try to fetch then.
        } catch (VersionNotFoundException x) {
            cacheStats.addCacheMissWrongVersion(id);
            // Ok, let's try to fetch the right version.
        }
        
        // Make sure we ask the cache for the same version we fetched (needed for query caching)
        CausalityClock requestedVersion = version.clone();

        while (true) {
            try {
                fetchObjectVersion(txn, id, create, classOfV, requestedVersion, query, true, updatesListener != null);
            } catch (InterruptedException x) {
                throw new NetworkException("Scout was shut down while  fetching an object: " + x.getMessage());
            }

            try {
                return getCachedObjectForTxn(txn, id, create, version, classOfV, updatesListener, query, requestedVersion, true);
                // TODO: does it work? it used to fail (hotfix: drop scout's
                // entry from version) as reported by smduarte:
                // WITH SI, at least, FAILS AFTER 10min running, as
                // shown below...[pre causal notifications hack]

                // WARNING: Object not found in appropriate version, probably
                // pruned: swift.exceptions.VersionNotFoundException: Object not
                // available in the cache in appropriate version: provided clock
                // ([OXcsOsX5RSurtRuQtdXdtQ==:[1-2],X0:[1-3884]]) is not less or
                // equal to the object updates clock
                // ([OXcsOsX5RSurtRuQtdXdtQ==:[1-1],X0:[1-3894]])
            } catch (NoSuchObjectException x) {
                logger.warning("Object not found in the cache just after fetch (retrying): " + x);
            } catch (VersionNotFoundException x) {
                logger.warning("Object not found in appropriate version, probably pruned: " + x);
                x.printStackTrace();
                throw x;
            }
        }
    }

    /**
     * Returns a view of an object version from the cache, if the object is
     * available in the appropriate version.
     * 
     * @param id
     * @param clock
     *            requested object version or null to specify the most recent
     *            version available
     * @param classOfV
     * @param updatesListener
     * @return object view
     * @throws WrongTypeException
     * @throws NoSuchObjectException
     * @throws VersionNotFoundException
     */
    @SuppressWarnings("unchecked")
    private synchronized <V extends CRDT<V>> V getCachedObjectForTxn(final AbstractTxnHandle txn, CRDTIdentifier id, boolean create,
            CausalityClock clock, Class<V> classOfV, ObjectUpdatesListener updatesListener, CRDTShardQuery<V> query, CausalityClock queryVersion, boolean justFetched)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {

        // System.err.println("---->" + id + "      " + clock);
        
        CausalityClock requestedClock = clock;

        if (clock == null) {
            // Set the requested clock to the latest committed version including
            // prior scout's transactions (the most recent thing we would like
            // to read).

            clock = getGlobalCommittedVersion(true);
            clock.merge(lastLocallyCommittedTxnClock);

            if (concurrentOpenTransactions && !txn.isReadOnly()) {
                // Make sure we do not introduce cycles in dependencies. Include
                // only transactions with lower timestamp in the snapshot,
                // because timestamp order induces the commit order.
                clock.drop(scoutId);
                // FIXME: This hack looks tricky, can we do it better?
                clock.recordAllUntil(txn.getClientTimestamp());
            }
        }

        ManagedCRDT<V> crdt = null;
        try {
            crdt = (ManagedCRDT<V>) objectsCache.getAndTouch(id, query, queryVersion);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
        
        if (crdt == null) {
            throw new NoSuchObjectException("Object not available in the cache");
        }
        
        if (requestedClock == null) {
            // Check if such a recent version is available in the cache. If not,
            // take the intersection of the clocks.

            clock.intersect(crdt.getClock());
            // TODO: Discuss. This is a very aggressive caching mode.
        }

        // Experimentally removed this suspicious heuristic
        // if (txn.isolationLevel == IsolationLevel.SNAPSHOT_ISOLATION &&
        // assumeAtomicCausalNotifications
        // && txn.cachePolicy == CachePolicy.CACHED)
        // clock.intersect(crdt.getClock());

        final V crdtView;
        try {
            crdtView = crdt.getVersion(clock, txn);
        } catch (IllegalStateException x) {
            // No appropriate version found in the object from the cache.
            throw new VersionNotFoundException("Object not available in the cache in appropriate version: "
                    + x.getMessage());
        }

        if (updatesListener != null) {
            // TODO support queries
            if (updatesListener.isSubscriptionOnly()) {
                addUpdateSubscriptionNoListener(crdt, !justFetched);
            } else {
                final UpdateSubscriptionWithListener subscription = addUpdateSubscriptionWithListener(txn, crdt,
                        crdtView, updatesListener, !justFetched);
                // Trigger update listener if we already know updates more
                // recent than the returned version.
                handleObjectNewVersionTryNotify(id, subscription, crdt);
            }

            if (txn.isReadOnly() && !crdt.isRegisteredInStore()) {
                logger.warning("The read-only transaction request for updates listener on inexisting object cannot be fulfilled");
            }
        }
        return crdtView;
    }
    
    /**
     * 
     * @param id
     * @return
     */
    private synchronized CausalityClock getKnownVersion(CRDTIdentifier id, CausalityClock requestedVersion) {
        ManagedCRDT crdt = objectsCache.getWithoutTouch(id);
        CausalityClock version;
        version = requestedVersions.get(id);
        if (version == null) {
            version = requestedVersion.clone();
        } else {
            if (crdt == null) {
                // If a newer version was evicted from the cache
                version.intersect(requestedVersion);
            }
        }
        if (crdt != null) {
            // In case the object was evicted then put back in the cache
            // in a lower version
            version.intersect(crdt.getClock());
        }
        requestedVersions.put(id, version);
        return version.clone();
    }

    private <V extends CRDT<V>> void fetchObjectVersion(final AbstractTxnHandle txn, CRDTIdentifier id, boolean create,
            Class<V> classOfV, final CausalityClock version, CRDTShardQuery<V> query, final boolean strictUnprunedVersion,
            final boolean subscribeUpdates) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, InterruptedException {
        // WISHME: Add a real delta-log shipping support?

        fetchObjectFromScratch(txn, id, create, classOfV, version, query, strictUnprunedVersion, subscribeUpdates);
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void fetchObjectFromScratch(final AbstractTxnHandle txn, CRDTIdentifier id,
            boolean create, Class<V> classOfV, CausalityClock version, CRDTShardQuery<V> query, boolean strictUnprunedVersion,
            boolean subscribeUpdates) throws NoSuchObjectException, WrongTypeException, VersionNotFoundException,
            NetworkException, InterruptedException {

        // TODO Q: what is this?
        if (subscribeUpdates)
            scoutPubSub.subscribe(id, scoutPubSub);
        else
            subscribeUpdates = scoutPubSub.isSubscribed(id);

        // Record and drop scout's entry from the requested clock (efficiency?).
        final Timestamp requestedScoutVersion = version.getLatest(scoutId);
        version.drop(this.scoutId);
        
        // To allow pruning updates from the reply
        final CausalityClock versionInCache = getKnownVersion(id, version);
        
        // TODO: do we always need a DC vector? Not necessarily.
        final boolean requestDCVector = true;
        final FetchObjectVersionRequest fetchRequest = new FetchObjectVersionRequest(scoutId, disasterSafe, id,
                version, versionInCache, query, strictUnprunedVersion, subscribeUpdates, requestDCVector);

        doFetchObjectVersionOrTimeout(txn, fetchRequest, classOfV, create, requestedScoutVersion);
    }

    private <V extends CRDT<V>> void doFetchObjectVersionOrTimeout(final AbstractTxnHandle txn,
            final FetchObjectVersionRequest fetchRequest, Class<V> classOfV, boolean create,
            Timestamp requestedScoutVersion) throws NetworkException, NoSuchObjectException, WrongTypeException,
            InterruptedException {

        synchronized (this) {
            fetchVersionsInProgress.add(fetchRequest.getVersion());
            ongoingObjectFetchesStats.incCounter();
        }

        try {
            final long firstRequestTimestamp = System.currentTimeMillis();
            FetchObjectVersionReply reply;
            do {
                final long requestDeadline = deadlineMillis - (System.currentTimeMillis() - firstRequestTimestamp);
                if (requestDeadline <= 0) {
                    throw new NetworkException("Deadline exceeded to get appropriate answer from the store;"
                            + "note it may be caused by prior errors");
                }
                reply = localEndpoint.request(serverEndpoint(), fetchRequest);
                if (reply == null) {
                    throw new NetworkException("Fetching object version exceeded the deadline");
                }
                if (stopFlag) {
                    throw new InterruptedException("Fetching object version was interrupted by scout shutdown.");
                }

                // TODO: record request only once in case of retry?
                fetchRequest.recordMetadataSample(metadataStatsCollector);
                reply.recordMetadataSample(metadataStatsCollector);

            } while (!processFetchObjectReply(txn, fetchRequest, reply, classOfV, create, requestedScoutVersion));
        } finally {
            synchronized (this) {
                fetchVersionsInProgress.remove(fetchRequest.getVersion());
                ongoingObjectFetchesStats.decCounter();
            }
        }
    }

    /**
     * @return when the request was successful
     */
    private <V extends CRDT<V>> boolean processFetchObjectReply(final AbstractTxnHandle txn,
            final FetchObjectVersionRequest request, final FetchObjectVersionReply fetchReply, Class<V> classOfV,
            boolean create, Timestamp requestedScoutVersion) throws NoSuchObjectException, WrongTypeException {
        final ManagedCRDT<V> crdt;

        {
            // Map<String, Object> staleReadInfo = fetchReply.staleReadsInfo;
            // if (staleReadInfo != null && staleReadInfo.size() > 0) {
            // long serial = txn == null ? -1 : txn.getSerial();
            // long rtt = sys.Sys.Sys.timeMillis() - (Long)
            // staleReadInfo.get("timestamp");
            // Object diff1 =
            // staleReadInfo.get("Diff1-scout-normal-vs-scout-stable");
            // Object diff2 =
            // staleReadInfo.get("Diff2-dc-normal-vs-scout-stable");
            // Object diff3 = staleReadInfo.get("Diff3-dc-normal-vs-dc-stable");
            // // TODO: replace with stats?
            // System.out.printf("SYS, GET, %s, %s, %s, %s, %s, %s\n", serial,
            // rtt, request.getUid(), diff1, diff2,
            // diff3);
            // }
        }

        switch (fetchReply.getStatus()) {
        case OBJECT_NOT_FOUND:
            if (!create) {
                throw new NoSuchObjectException("object " + request.getUid() + " not found");
            }
            final V checkpoint;
            try {
                final Constructor<V> constructor = classOfV.getConstructor(CRDTIdentifier.class);
                checkpoint = constructor.newInstance(request.getUid());
            } catch (Exception e) {
                e.printStackTrace();
                throw new WrongTypeException(e.getMessage());
            }
            final CausalityClock clock = request.getRequestedVersion().clone();
            if (fetchReply.getEstimatedDisasterDurableCommittedVersion() != null) {
                clock.merge(fetchReply.getEstimatedDisasterDurableCommittedVersion());
            }
            if (fetchReply.getEstimatedCommittedVersion() != null) {
                clock.merge(fetchReply.getEstimatedCommittedVersion());
            }
            clock.recordAllUntil(requestedScoutVersion);
            crdt = new ManagedCRDT<V>(request.getUid(), checkpoint, clock, false);
            break;
        case VERSION_NOT_FOUND:
        case OK:
            try {
                crdt = (ManagedCRDT<V>) fetchReply.getCrdt();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            // stats.getValuesFrequencyOverTime("crdt-clock-intervals", new
            // double[] { 1, 2, 4, 8, 16, 32, 64, 128, 256 })
            // .setValue(((VersionVectorWithExceptions)
            // crdt.getClock()).getNumPairs());
            // stats.getValuesFrequencyOverTime("dc-clock-intervals", new
            // double[] { 1, 2, 4, 8, 16, 32, 64, 128, 256 })
            // .setValue(((VersionVectorWithExceptions)
            // fetchReply.getEstimatedCommittedVersion()).getNumPairs());

            break;
        default:
            throw new IllegalStateException("Unexpected status code" + fetchReply.getStatus());
        }

        synchronized (this) {
            updateCommittedVersions(fetchReply.getEstimatedCommittedVersion(),
                    fetchReply.getEstimatedDisasterDurableCommittedVersion());
            
            /*
            // Now done in objectsCache.add()
            ManagedCRDT<V> cacheCRDT;
            try {
                if (objectsCache.has(request.getUid(), request.getQuery(), crdt.getClock())) {
                    if (txn != null) {
                        cacheCRDT = (ManagedCRDT<V>) objectsCache.getAndTouch(request.getUid());
                    } else {
                        cacheCRDT = (ManagedCRDT<V>) objectsCache.getWithoutTouch(request.getUid());
                    }
                }
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            if (cacheCRDT == null
                    || crdt.getClock().compareTo(cacheCRDT.getClock())
                            .is(CMP_CLOCK.CMP_DOMINATES, CMP_CLOCK.CMP_EQUALS)) {
                // If clock >= cacheCrdt.clock, it 1) does not make sense to
                // merge, 2) received version may have different pruneClock,
                // which could be either helpful or not depending on the case.
                objectsCache.add(crdt, txn == null ? -1L : txn.serial, request.getQuery(), crdt.getClock());
                cacheCRDT = crdt;
            } else {
                try {
                    cacheCRDT.merge(crdt);
                } catch (IllegalStateException x) {
                    logger.warning("Merging incoming object version " + crdt.getClock() + " with the cached version "
                            + cacheCRDT.getClock() + " has failed with our heuristic - dropping cached version" + x);
                    cacheCRDT = crdt;
                    objectsCache.add(crdt, txn == null ? -1L : txn.serial, request.getQuery(), crdt.getClock());
                }
            }
            */
            ManagedCRDT<V> cacheCRDT;
            CausalityClock queriedVersion = request.getRequestedVersion();
            try {
                cacheCRDT = (ManagedCRDT)objectsCache.add(crdt, txn == null ? -1L : txn.serial, request.getQuery(), queriedVersion);
            } catch (ClassCastException e) {
                throw new WrongTypeException(e.getMessage());
            }
            
            if (fetchReply.getStatus() != FetchStatus.VERSION_NOT_FOUND) {
                CausalityClock version;
                version = requestedVersions.get(request.getUid());
                if (version == null) {
                    version = request.getVersion();
                    requestedVersions.put(request.getUid(), version);
                } else {
                    if (version.compareTo(request.getVersion()).is(CMP_CLOCK.CMP_ISDOMINATED, CMP_CLOCK.CMP_CONCURRENT)) {
                        version.merge(request.getVersion());
                    }
                }
            }
            
            // Apply any local updates that may not be present in received
            // version.
            for (final AbstractTxnHandle localTxn : globallyCommittedUnstableTxns) {
                applyLocalObjectUpdates(cacheCRDT, localTxn);
            }
            for (final AbstractTxnHandle localTxn : locallyCommittedTxnsOrderedQueue) {
                applyLocalObjectUpdates(cacheCRDT, localTxn);
            }
            // FIXME: check scout clock and trigger recovery?

            Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(request
                    .getUid());

            // if (request.getSubscriptionType() != SubscriptionType.NONE &&
            // sessionsSubs == null) {
            // // Add temporary subscription entry without specifying full
            // // information on what value has been read.
            // addUpdateSubscriptionNoListener(crdt, false);
            // }

            // See if anybody is interested in new updates on this object.
            if (sessionsSubs != null) {
                for (final UpdateSubscriptionWithListener subscription : sessionsSubs.values()) {
                    handleObjectNewVersionTryNotify(crdt.getUID(), subscription, cacheCRDT);
                }
            }
        }

        if (fetchReply.getStatus() == FetchStatus.VERSION_NOT_FOUND) {
            // System.err.println( sys.Sys.Sys.mainClass + "&&&&&&&&&&" +
            // fetchReply.getEstimatedCommittedVersion() + "/" +
            // fetchReply.getVersion() + "/wanted:" + request.getVersion() +
            // "ownCLOCK:" + committedVersion );

            logger.warning("requested object version not found in the store, retrying fetch");
            return false;
        }
        return true;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void applyLocalObjectUpdates(ManagedCRDT cachedCRDT, final AbstractTxnHandle localTxn) {
        // Try to apply changes in a cached copy of an object.
        if (cachedCRDT == null) {
            logger.info("object evicted from the local cache, cannot apply local transaction changes");
            return;
        }

        final CRDTObjectUpdatesGroup objectUpdates = localTxn.getObjectUpdates(cachedCRDT.getUID());
        if (objectUpdates != null) {
            // IGNORE dependencies checking, for RR transaction
            // dependencies are overestimated.
            // TODO: during failover, it may be unsafe to IGNORE.
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

    /**
     * @return true if subscription should be continued for this object
     */
    private synchronized void applyObjectUpdates(CRDTIdentifier id, List<CRDTObjectUpdatesGroup<?>> ops) {
        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }

        final Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(id);
        if (sessionsSubs == null) {
            removeUpdateSubscriptionAsyncUnsubscribe(id);
        }

        final ManagedCRDT crdt = objectsCache.getWithoutTouch(id);

        if (crdt == null) {
            // Ooops, we evicted the object from the cache.
            logger.info("cannot apply received updates on object " + id + " evicted from the cache; re-fetching");
            if (sessionsSubs != null) {
                if (!sessionsSubs.isEmpty()) {
                    if (!ops.isEmpty()) {
                        // There is still listener waiting, make some efforts to
                        // fire the notification.
                        asyncFetchAndSubscribeObjectUpdates(id);
                    }
                } else {
                    // Stop subscription for object evicted from the cache.
                    removeUpdateSubscriptionAsyncUnsubscribe(id);
                }
            }
            return;
        }

        if (logger.isLoggable(Level.INFO)) {
            // TODO: printf usage wouldn't hurt :-)
            // logger.info("applying received updates on object " + id +
            // ";num.ops=" + ops.size() + ";tx="
            // + (ops.size() == 0 ? "-" :
            // ops.get(0).getTimestampMapping().getSelectedSystemTimestamp())
            // + ";clttx=" + (ops.size() == 0 ? "-" :
            // ops.get(0).getTimestamps()[0]) + ";vv=" + outputClock
            // + ";dep=" + dependencyClock);
        }

        for (final CRDTObjectUpdatesGroup<?> op : ops) {
            final boolean newUpdate = crdt.execute(op, CRDTOperationDependencyPolicy.IGNORE);
            final String updatesScoutId = op.getClientTimestamp().getIdentifier();
            if (!updatesScoutId.equals(scoutId)) {
                crdt.discardScoutClock(updatesScoutId);
            }
            if (!newUpdate) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("update " + op.getClientTimestamp() + " was already included in the state of object "
                            + id);
                }
                // Already applied update.
                continue;
            }
            if (sessionsSubs != null) {
                for (final UpdateSubscriptionWithListener subscription : sessionsSubs.values()) {
                    handleObjectUpdatesTryNotify(id, subscription, op.getTimestampMapping());
                }
            }
        }
        // FIXME: when to prune?
        // crdt.prune(update.getPruneClock(), true);
    }

    private synchronized void handleObjectUpdatesTryNotify(CRDTIdentifier id,
            UpdateSubscriptionWithListener subscription, TimestampMapping... timestampMappings) {
        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }
        // System.err.printf("My CLOCK:---->%s\n",
        // getGlobalCommittedVersion(false));
        // System.err.printf("Read: %s  --------------->%s\n",
        // subscription.readVersion, Arrays.asList(timestampMappings));
        // for (final TimestampMapping tm : timestampMappings) {
        // if (!tm.anyTimestampIncluded(subscription.readVersion)) {
        // System.err.println("---->" +
        // tm.anyTimestampIncluded(subscription.readVersion));
        // if (tm.anyTimestampIncluded(getGlobalCommittedVersion(false))
        // || tm.anyTimestampIncluded(lastLocallyCommittedTxnClock)) {
        // System.err.println("SwiftImpl.preNotify:" + id);
        // executorService.execute(subscription.generateNotificationAndDiscard(this,
        // id));
        // return;
        // }
        // }
        // }
        // if (true)
        // return;

        Map<TimestampMapping, CRDTIdentifier> uncommittedUpdates = new HashMap<TimestampMapping, CRDTIdentifier>();
        for (final TimestampMapping tm : timestampMappings) {
            if (!tm.anyTimestampIncluded(subscription.readVersion)) {
                if (tm.anyTimestampIncluded(getGlobalCommittedVersion(false))
                        || tm.anyTimestampIncluded(lastLocallyCommittedTxnClock)) {
                    executorService.execute(subscription.generateNotificationAndDiscard(this, id));
                    return;
                }
                uncommittedUpdates.put(tm, id);
            }
        }
        // There was no committed timestamp we could notify about, so put
        // them the queue of updates to notify when they are committed.
        for (final Entry<TimestampMapping, CRDTIdentifier> entry : uncommittedUpdates.entrySet()) {
            Set<CRDTIdentifier> ids = uncommittedUpdatesObjectsToNotify.get(entry.getKey());
            if (ids == null) {
                ids = new HashSet<CRDTIdentifier>();
                uncommittedUpdatesObjectsToNotify.put(entry.getKey().copy(), ids);
            } else {
                // FIXME: merge timestamp mappings for entry.getKey().
                // TRICKY! Should we also apply these mappings to objects, it
                // must be consistent!
            }
            ids.add(entry.getValue());
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Update on object " + id + " visible, but not committed, delaying notification");
            }
        }
    }

    private synchronized <V extends CRDT<V>> void handleObjectNewVersionTryNotify(CRDTIdentifier id,
            final UpdateSubscriptionWithListener subscription, final ManagedCRDT<V> newCrdtVersion) {
        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }

        final List<TimestampMapping> recentUpdates;
        try {
            recentUpdates = newCrdtVersion.getUpdatesTimestampMappingsSince(subscription.readVersion);
        } catch (IllegalArgumentException x) {
            // Object has been pruned since then, approximate by comparing old
            // and new txn views. This is a very bizzare case.
            logger.warning("Object has been pruned since notification was set up, needs to investigate the observable view");
            final CRDT<V> newView;
            try {
                final CausalityClock committedClock = getGlobalCommittedVersion(true);
                committedClock.merge(lastLocallyCommittedTxnClock);
                newView = newCrdtVersion.getVersion(committedClock, subscription.txn);
            } catch (IllegalStateException x2) {
                logger.warning("Object has been pruned since notification was set up, and investigating the observable view due to incompatible version");
                return;
            }
            if (!newView.getValue().equals(subscription.crdtView.getValue())) {
                executorService.execute(subscription.generateNotificationAndDiscard(this, id));
            }
            return;
        }
        handleObjectUpdatesTryNotify(id, subscription, recentUpdates.toArray(new TimestampMapping[0]));
    }

    private synchronized void addUpdateSubscriptionNoListener(final ManagedCRDT<?> crdt, boolean needsFetch) {
        if (!objectSessionsUpdateSubscriptions.containsKey(crdt.getUID())) {
            objectSessionsUpdateSubscriptions.put(crdt.getUID(), new HashMap<String, UpdateSubscriptionWithListener>());
            if (needsFetch && crdt.isRegisteredInStore()) {
                asyncFetchAndSubscribeObjectUpdates(crdt.getUID());
            }
            // else: newly created object, wait until untilcommitTxnGlobally()
            // with subscription.
        }
    }

    private synchronized UpdateSubscriptionWithListener addUpdateSubscriptionWithListener(final AbstractTxnHandle txn,
            final ManagedCRDT<?> crdt, final CRDT<?> localView, ObjectUpdatesListener listener, boolean needsFetch) {
        if (listener.isSubscriptionOnly()) {
            throw new IllegalArgumentException("Dummy listener subscribing udpates like a real listener for object "
                    + crdt.getUID());
        }

        Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(crdt.getUID());
        if (sessionsSubs == null) {
            addUpdateSubscriptionNoListener(crdt, needsFetch);
            sessionsSubs = objectSessionsUpdateSubscriptions.get(crdt.getUID());
        }

        final UpdateSubscriptionWithListener updateSubscription = new UpdateSubscriptionWithListener(txn, localView,
                listener);
        // Overwriting old session entry and even subscribing again is fine, the
        // interface specifies clearly that the latest get() matters.
        sessionsSubs.put(txn.getSessionId(), updateSubscription);
        return updateSubscription;
    }

    private synchronized void removeUpdateSubscriptionWithListener(CRDTIdentifier id, String sessionId,
            UpdateSubscriptionWithListener listener) {
        final Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(id);
        if (sessionsSubs != null && sessionsSubs.get(sessionId) == listener) {
            sessionsSubs.remove(sessionId);
        }
    }

    private void asyncFetchAndSubscribeObjectUpdates(final CRDTIdentifier id) {

        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                final CausalityClock version;
                synchronized (SwiftImpl.this) {
                    if (!objectSessionsUpdateSubscriptions.containsKey(id)) {
                        return;
                    }
                    version = getGlobalCommittedVersion(true);
                    version.merge(lastLocallyCommittedTxnClock);
                }
                try {
                    // FIXME: <V extends TxnLocalCRDT<V>> should be really part
                    // of ID, because if the object does not exist, we are
                    // grilled here with passing interface as V - it will crash
                    // TODO support queries
                    fetchObjectVersion(null, id, false, CRDT.class, version, new FullShardQuery(), false, true);
                } catch (SwiftException x) {
                    logger.warning("could not fetch the latest version of an object for notifications purposes: "
                            + x.getMessage());
                } catch (InterruptedException x) {
                    // Scout was closed.
                }
            }
        });
    }

    private synchronized void removeUpdateSubscriptionAsyncUnsubscribe(final CRDTIdentifier id) {
        objectSessionsUpdateSubscriptions.remove(id);

        // notificationsSubscriberExecutor.execute(new Runnable() {
        // @Override
        // public void run() {
        // if (objectSessionsUpdateSubscriptions.containsKey(id)) {
        // return;
        // }
        // if (localEndpoint.send(serverEndpoint, new
        // PubSubSubscriptionsUpdate(scoutId, id)).failed()) {
        // logger.info("failed to unsuscribe object updates");
        // }
        // }
        // });
    }

    @Override
    public synchronized void discardTxn(AbstractTxnHandle txn) {
        assertPendingTransaction(txn);
        removePendingTxn(txn);
        logger.info("local transaction " + txn.getTimestampMapping() + " rolled back");
        if (requiresGlobalCommit(txn)) {
            // Need to create and commit a dummy transaction, we cannot
            // returnLastTimestamp :-(
            final RepeatableReadsTxnHandle dummyTxn = new RepeatableReadsTxnHandle(this, txn.getSessionId(),
                    durableLog, CachePolicy.CACHED, txn.getTimestampMapping(), stats);
            dummyTxn.markLocallyCommitted();
            commitTxn(dummyTxn);
        } else {
            tryReuseTxnTimestamp(txn);
        }
    }

    // Txn is committed globally if it is not read-only, if it contains updates
    // and if it has not been cancelled
    private boolean requiresGlobalCommit(AbstractTxnHandle txn) {
        if (txn.isReadOnly()) {
            return false;
        }
        if (!concurrentOpenTransactions) {
            if (txn.getStatus() == TxnStatus.CANCELLED || txn.getAllUpdates().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void commitTxn(AbstractTxnHandle txn) {
        assertPendingTransaction(txn);
        assertRunning();
        txn.markLocallyCommitted();
        if (logger.isLoggable(Level.INFO)) {
            logger.info("transaction " + txn.getTimestampMapping() + " committed locally");
        }

        if (requiresGlobalCommit(txn)) {

            lastLocallyCommittedTxnClock.record(txn.getClientTimestamp());
            for (final CRDTObjectUpdatesGroup<?> opsGroup : txn.getAllUpdates()) {
                final CRDTIdentifier id = opsGroup.getTargetUID();
                applyLocalObjectUpdates(objectsCache.getWithoutTouch(id), txn);
                // Look if there is any other session to notify.
                final Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions
                        .get(id);
                if (sessionsSubs != null) {
                    for (final UpdateSubscriptionWithListener subscription : sessionsSubs.values()) {
                        if (subscription.txn == txn) {
                            // Add this update transaction timestamp to
                            // readVersion to exclude self-notifications.
                            for (final Timestamp ts : txn.getTimestampMapping().getTimestamps()) {
                                subscription.readVersion.record(ts);
                            }
                        }
                        handleObjectUpdatesTryNotify(id, subscription, opsGroup.getTimestampMapping());
                    }
                }
            }
            objectsCache.augmentAllWithScoutTimestampWithoutMappings(txn.getClientTimestamp());
            lastLocallyCommittedTxnClock.merge(txn.getUpdatesDependencyClock());

            // Transaction is queued up for global commit.
            // THIS MAY BLOCK in wait() if the queue is full!
            addLocallyCommittedTransactionBlocking(txn);

        } else {
            tryReuseTxnTimestamp(txn);
            txn.markGloballyCommitted(null);
            removeEvictionProtection(txn);
            if (logger.isLoggable(Level.INFO)) {
                logger.info("read-only transaction " + txn.getTimestampMapping() + " will not commit globally");
            }
        }
        removePendingTxn(txn);
    }

    public void removeEvictionProtection(AbstractTxnHandle txn) {
        objectsCache.removeProtection(txn.serial);
    }

    private void tryReuseTxnTimestamp(AbstractTxnHandle txn) {
        if (!txn.isReadOnly()) {
            returnLastTimestamp();
        }
    }

    /**
     * Stubborn commit procedure. Repeats until it succeeds.
     * 
     * @param txn
     *            locally committed transaction
     */
    private void commitTxnGlobally(final List<AbstractTxnHandle> transactionsToCommit) {
        final List<CommitUpdatesRequest> requests = new LinkedList<CommitUpdatesRequest>();

        // Preprocess transactions before sending them.
        final CausalityClock sharedDeps;
        if (USE_SHARED_DEPENDENCIES_IN_COMMIT_BATCH) {
            sharedDeps = transactionsToCommit.get(transactionsToCommit.size() - 1).getUpdatesDependencyClock().clone();
            final long prevId = transactionsToCommit.get(0).getClientTimestamp().getCounter() - 1;
            if (prevId > 0) {
                sharedDeps.recordAllUntil(new Timestamp(scoutId, prevId));
            }

        }
        for (final AbstractTxnHandle txn : transactionsToCommit) {
            txn.assertStatus(TxnStatus.COMMITTED_LOCAL);

            final CausalityClock newDeps;
            if (USE_SHARED_DEPENDENCIES_IN_COMMIT_BATCH) {
                // Alternative 1: coarse grained, but overapproximated.
                newDeps = sharedDeps;
            } else {
                // Alternative 2: fine grained, but costs more traffic.
                newDeps = txn.getUpdatesDependencyClock().clone();
            }
            final LinkedList<CRDTObjectUpdatesGroup<?>> operationsGroups = new LinkedList<CRDTObjectUpdatesGroup<?>>();
            for (final CRDTObjectUpdatesGroup<?> group : txn.getAllUpdates()) {
                operationsGroups.add(group.withDependencyClock(newDeps));
            }
            requests.add(new CommitUpdatesRequest(scoutId, disasterSafe, txn.getClientTimestamp(), newDeps,
                    operationsGroups));
        }
        for (CommitUpdatesRequest request : requests) {
            // LEGACY: internal dependency is implicit from the timestamp and
            // checked by the surrogate.
            request.dropInternalDependency();
        }

        // Send batched updates and wait for reply of server
        final BatchCommitUpdatesRequest commitRequest = new BatchCommitUpdatesRequest(scoutId, disasterSafe, requests);
        BatchCommitUpdatesReply batchReply = localEndpoint.request(serverEndpoint(), commitRequest);
        commitRequest.recordMetadataSample(metadataStatsCollector);

        // TODO Add here statistics for meta-data overhead?

        if (batchReply == null) {
            // FIXME with new RPC API null is just a timeout??
            throw new IllegalStateException("Fatal error: server returned null on commit");
        }
        batchReply.recordMetadataSample(metadataStatsCollector);
        if (batchReply != null && batchReply.getReplies().size() != requests.size()) {
            throw new IllegalStateException("Fatal error: server returned " + batchReply.getReplies().size() + " for "
                    + requests.size() + " commit requests!");
        }

        synchronized (this) {
            for (int i = 0; i < batchReply.getReplies().size(); i++) {
                final CommitUpdatesReply reply = batchReply.getReplies().get(i);
                final AbstractTxnHandle txn = transactionsToCommit.get(i);

                switch (reply.getStatus()) {
                case COMMITTED_WITH_KNOWN_TIMESTAMPS:
                    for (final Timestamp ts : reply.getCommitTimestamps()) {
                        txn.markGloballyCommitted(ts);
                        // lastGloballyCommittedTxnClock.record(ts);
                        // Commented out to avoid holes; useful for
                        // multi-session?
                        // committedVersion.record(ts);
                        // TODO: call updateCommittedVersion?
                    }
                    CausalityClock systemTxnClock = ClockFactory.newClock();
                    for (final Timestamp systemTimestamp : txn.getTimestampMapping().getSystemTimestamps()) {
                        systemTxnClock.record(systemTimestamp);
                    }
                    // Record new mappings for updated objects.
                    for (final CRDTObjectUpdatesGroup update : txn.getAllUpdates()) {
                        applyLocalObjectUpdates(objectsCache.getWithoutTouch(update.getTargetUID()), txn);
                    }
                    // Advance clock of all objects.
                    objectsCache.augmentAllWithDCCausalClockWithoutMappings(systemTxnClock);
                    break;
                case COMMITTED_WITH_KNOWN_CLOCK_RANGE:
                    // lastGloballyCommittedTxnClock.merge(reply.getImpreciseCommitClock());
                    txn.markGloballyCommitted(null);
                    // TODO: call updateCommittedVersion?
                    break;
                case INVALID_OPERATION:
                    throw new IllegalStateException("DC replied to commit request with INVALID_OPERATION");
                default:
                    throw new UnsupportedOperationException("unknown commit status: " + reply.getStatus());
                }
                // TODO: can it be done earlier, when it commits locally?
                removeEvictionProtection(txn);
                // lastGloballyCommittedTxnClock.merge(txn.getUpdatesDependencyClock());
                // lastLocallyCommittedTxnClock.merge(txn.getUpdatesDependencyClock());
                removeLocallyNowGloballyCommitedTxn(txn);
                globallyCommittedUnstableTxns.addLast(txn);

                if (logger.isLoggable(Level.INFO)) {
                    logger.info("transaction " + txn.getTimestampMapping() + " commited globally");
                }

                // Subscribe updates for newly created objects if they were
                // requested. It can be done only at this stage once the objects
                // are in the store.
                for (final CRDTObjectUpdatesGroup<?> opsGroup : txn.getAllUpdates()) {
                    final boolean subscriptionsExist = objectSessionsUpdateSubscriptions.containsKey(opsGroup
                            .getTargetUID());
                    if (subscriptionsExist && opsGroup.hasCreationState()) {
                        asyncFetchAndSubscribeObjectUpdates(opsGroup.getTargetUID());
                    }
                }
                tryPrune();
            }
        }
    }

    private synchronized void addLocallyCommittedTransactionBlocking(AbstractTxnHandle txn) {
        // Insert only if the queue size allows, or if tnx blocks other
        // transactions.
        while (locallyCommittedTxnsOrderedQueue.size() >= maxAsyncTransactionsQueued
                && locallyCommittedTxnsOrderedQueue.first().compareTo(txn) < 0) {
            logger.warning("Asynchronous commit queue is full - blocking the transaction commit");
            try {
                this.wait();
            } catch (InterruptedException e) {
                if (stopFlag && !stopGracefully) {
                    throw new IllegalStateException("Scout stopped in non-graceful manner, transaction not committed");
                }
            }
        }
        locallyCommittedTxnsOrderedQueue.add(txn);
        // Notify committer thread.
        this.notifyAll();
    }

    private synchronized void removeLocallyNowGloballyCommitedTxn(final AbstractTxnHandle txn) {
        locallyCommittedTxnsOrderedQueue.remove(txn);
        this.notifyAll();
    }

    /**
     * @return a batch of transactions ready to commit (within
     *         maxCommitBatchSize limit)
     */
    private synchronized List<AbstractTxnHandle> consumeLocallyCommitedTxnsQueue() {
        List<AbstractTxnHandle> result = new LinkedList<AbstractTxnHandle>();
        do {
            final Iterator<AbstractTxnHandle> queueIter = locallyCommittedTxnsOrderedQueue.iterator();
            for (int i = 0; i < maxCommitBatchSize && queueIter.hasNext(); i++) {
                final AbstractTxnHandle candidateTxn = queueIter.next();
                boolean validCandidate = true;
                if (concurrentOpenTransactions) {
                    // Check whether transactions with lower timestamps already
                    // committed. TODO: this is a quick HACK, do it better.
                    final long candidateCounter = locallyCommittedTxnsOrderedQueue.first().getTimestampMapping()
                            .getClientTimestamp().getCounter();
                    for (final AbstractTxnHandle txn : pendingTxns) {
                        if (!txn.isReadOnly() && txn.getClientTimestamp().getCounter() < candidateCounter) {
                            validCandidate = false;
                            break;
                        }
                    }
                }

                if (validCandidate) {
                    result.add(candidateTxn);
                } else {
                    break;
                }
            }

            if (result.size() > maxCommitBatchSize) {
                throw new IllegalStateException("Internal error, transaction batch size computed wrongly");
            }
            if (result.isEmpty() && !stopFlag)
                Threading.waitOn(this);
            else {
                return result;
            }
        } while (true);
    }

    private synchronized void addPendingTxn(final AbstractTxnHandle txn) {
        pendingTxns.add(txn);
    }

    private synchronized void removePendingTxn(final AbstractTxnHandle txn) {
        pendingTxns.remove(txn);
    }

    private synchronized void assertPendingTransaction(final AbstractTxnHandle expectedTxn) {
        if (!pendingTxns.contains(expectedTxn)) {
            throw new IllegalStateException(
                    "Corrupted state: unexpected transaction is bothering me, not the pending one");
        }
    }

    private void assertRunning() {
        if (stopFlag) {
            throw new IllegalStateException("scout is stopped");
        }
    }

    private void assertIsGlobalClock(CausalityClock version) {
        if (version.hasEventFrom(scoutId)) {
            throw new IllegalArgumentException("transaction requested visibility of local transaction");
        }
    }

    /**
     * Thread continuously committing locally committed transactions. The thread
     * takes the oldest locally committed transaction one by one, tries to
     * commit it to the store and update relevant clock information.
     */
    private class CommitterThread extends Thread {

        public CommitterThread() {
            super("SwiftTransactionCommitterThread");
        }

        @Override
        public void run() {
            while (true) {
                List<AbstractTxnHandle> transactionsToCommit = consumeLocallyCommitedTxnsQueue();
                batchSizeOnCommitStats.setValue(transactionsToCommit.size());
                if (stopFlag && (transactionsToCommit.isEmpty() || !stopGracefully)) {
                    if (!transactionsToCommit.isEmpty()) {
                        logger.warning("Scout ungraceful stop, some transactions may not have globally committed");
                    }
                    return;
                }
                commitTxnGlobally(transactionsToCommit);
            }
        }
    }

    /**
     * Scout representation of updates subscription with listener for a session.
     * The listener is awaiting for notification on update that occurred after
     * the readVersion.
     * 
     * @author mzawirski
     */
    private static class UpdateSubscriptionWithListener {
        private final AbstractTxnHandle txn;
        private final ObjectUpdatesListener listener;
        private final CRDT<?> crdtView;
        private final CausalityClock readVersion;
        private final AtomicBoolean fired;

        public UpdateSubscriptionWithListener(AbstractTxnHandle txn, CRDT<?> crdtView,
                final ObjectUpdatesListener listener) {
            this.txn = txn;
            this.crdtView = crdtView;
            this.listener = listener;
            this.readVersion = crdtView.getClock().clone();
            this.fired = new AtomicBoolean();
        }

        public Runnable generateNotificationAndDiscard(final SwiftImpl scout, final CRDTIdentifier id) {
            return new Runnable() {
                @Override
                public void run() {
                    if (fired.getAndSet(true)) {
                        return;
                    }
                    logger.info("Notifying on update on object " + id);
                    listener.onObjectUpdate(txn, id, crdtView);
                    // Mommy (well, daddy) tells you: clean up after yourself.
                    scout.removeUpdateSubscriptionWithListener(id, txn.getSessionId(),
                            UpdateSubscriptionWithListener.this);
                }
            };
        }
    }

    @Override
    public void onFailOver() {
        // TODO: replace with logging or Stats?
        System.out.println("SYS FAILOVER TO: " + serverEndpoint());
    }

    int serverIndex = 0;

    Endpoint serverEndpoint() {
        return serverEndpoints[serverIndex];
    }

    static Endpoint[] parseEndpoints(String serverList) {
        List<Endpoint> res = new ArrayList<Endpoint>();
        for (String i : serverList.split(","))
            res.add(Networking.resolve(i, DCConstants.SURROGATE_PORT));
        return res.toArray(new Endpoint[res.size()]);
    }

    class FailOverWatchDog extends Thread {
        FailOverWatchDog() {
            super.setDaemon(true);
            serverIndex = 0;
        }

        public void run() {
            for (;;) {
                Threading.sleep((30 + sys.Sys.Sys.rg.nextInt(10)) * 1000);
                serverIndex = (serverIndex + 1) % serverEndpoints.length;
                // TODO: replace with logging or stats?
                System.out.println("SYS FAILOVER TO INITIATED: " + serverEndpoint());
                getDCClockEstimates();
                getDCClockEstimates();
                getDCClockEstimates();
                System.out.println("SYS FAILOVER TO COMPLETED: " + serverEndpoint());
                onFailOver();
            }
        }
    }
}
