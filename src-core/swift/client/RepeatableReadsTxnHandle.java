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

import swift.clocks.TimestampMapping;
import swift.cprdt.core.CRDTShardQuery;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.TransactionsLog;
import sys.stats.Stats;

/**
 * Implementation of {@link IsolationLevel#REPEATABLE_READS} transaction, which
 * always read from a snapshot, possibly inconsistent and provides repeatable
 * reads.
 * <p>
 * It tries to offer the latest available object version, accessing the store
 * according to the {@link CachePolicy}.
 * 
 * @author mzawirski
 */
class RepeatableReadsTxnHandle extends AbstractTxnHandle {

    /**
     * Creates update transaction.
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
     *            update of this transaction
     */
    RepeatableReadsTxnHandle(final TxnManager manager, final String sessionId, final TransactionsLog durableLog,
            final CachePolicy cachePolicy, final TimestampMapping timestampMapping, Stats stats) {
        super(manager, sessionId, durableLog, IsolationLevel.REPEATABLE_READS, cachePolicy, timestampMapping, stats);
    }

    /**
     * Creates read-only transaction.
     * 
     * @param manager
     *            manager maintaining this transaction
     * @param sessionId
     *            id of the client session issuing this transaction
     * @param cachePolicy
     *            cache policy used by this transaction
     */
    RepeatableReadsTxnHandle(final TxnManager manager, final String sessionId, final CachePolicy cachePolicy,
            Stats stats) {
        super(manager, sessionId, IsolationLevel.REPEATABLE_READS, cachePolicy, stats);
    }

    @Override
    protected <V extends CRDT<V>> V getImpl(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener updatesListener, CRDTShardQuery<V> query, boolean createLocally) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        CRDT<V> localView = (CRDT<V>) objectViewsCache.get(id);
        if (localView != null) {
            if (!localView.getShard().isFull()) {
                // Need to fetch the queried parts
                CRDT<V> extendedLocalView = manager.getObjectVersionTxnView(this, id, localView.getClock(), create, classOfV, updatesListener, query);
                updateWithNotFullyAppliedOperations(id, (V)extendedLocalView);
                localView.mergeSameVersion((V) extendedLocalView);
                localView.setShard(localView.getShard().union(extendedLocalView.getShard()));
            } else {
                if (updatesListener != null) {
                    manager.getObjectVersionTxnView(this, id, localView.getClock(), create, classOfV, updatesListener, query);
                }
            }
        } else { // localView == null
            localView = manager.getObjectLatestVersionTxnView(this, id, cachePolicy, create, classOfV, updatesListener, null);
            objectViewsCache.put(id, localView);
            updateUpdatesDependencyClock(localView.getClock());
        }
        return (V) localView;
    }
}
