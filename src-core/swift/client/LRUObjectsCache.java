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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.cprdt.core.CRDTShardQuery;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;

/**
 * Local cache of CRDT objects with LRU eviction policy. Elements get evicted
 * when not used for a defined period of time or size of the cache is exceeded.
 * <p>
 * Thread unsafe (requires external synchronization).
 * 
 * @author smduarte, mzawirski
 */
class LRUObjectsCache {

    public static interface EvictionListener {
        void onEviction(CRDTIdentifier id);
    }

    private static Logger logger = Logger.getLogger(LRUObjectsCache.class.getName());

    private final int maxElements;
    private final long evictionTimeMillis;
    private Map<CRDTIdentifier, Entry> entries;
    // To get a cached object without reordering the entries
    private Map<CRDTIdentifier, Entry> shadowEntries;
    private Set<Long> evictionProtections;

    private EvictionListener evictionListener = new EvictionListener() {
        public void onEviction(CRDTIdentifier id) {
        }
    };

    synchronized public void removeProtection(long serial) {
        evictionProtections.remove(serial);
        evictExcess();
        evictOutdated();
    }

    /**
     * @param evictionTimeMillis
     *            maximum life-time for object entries (exclusive) in
     *            milliseconds
     */
    @SuppressWarnings("serial")
    public LRUObjectsCache(final long evictionTimeMillis, final int maxElements) {

        this.evictionTimeMillis = evictionTimeMillis;
        this.maxElements = maxElements;

        entries = new LinkedHashMap<CRDTIdentifier, Entry>(32, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<CRDTIdentifier, Entry> eldest) {
                Entry e = eldest.getValue();
                if (!evictionProtections.contains(e.id()) && size() > maxElements) {
                    shadowEntries.remove(eldest.getKey());
                    evictionListener.onEviction(eldest.getKey());

                    // System.err.println(eldest.getKey() +
                    // " evicted from the cache due to size limit, acesses:"
                    // + e.getNumberOfAccesses());

                    logger.info("Object evicted from the cache due to size limit, accesses:" + e.getNumberOfAccesses());
                    return true;
                } else
                    return false;
            }
        };
        shadowEntries = new HashMap<CRDTIdentifier, Entry>();
        evictionProtections = new HashSet<Long>();
    }

    void setEvictionListener(EvictionListener evictionListener) {
        this.evictionListener = evictionListener;
    }

    /**
     * Adds object to the cache, possibly overwriting old entry. May cause
     * evictoin due to size limit in the cache.
     * 
     * @param object
     *            object to add
     */
    synchronized public void add(final ManagedCRDT<?> object, CRDTShardQuery<?> query, long txnSerial) {
        if (txnSerial >= 0)
            evictionProtections.add(txnSerial);
        
        Entry e = shadowEntries.get(object.getUID());
        if (e == null) {
            e = new Entry(object, query, txnSerial);
        } else {
            e.add(object, query);
        }
        entries.put(object.getUID(), e);
        shadowEntries.put(object.getUID(), e);
    }

    /**
     * Returns object for given id and records access to the cache.
     * 
     * @param id
     *            object id
     * @return object or null if object is absent in the cache
     */
    synchronized public ManagedCRDT<?> getAndTouch(final CRDTIdentifier id, CRDTShardQuery<?> query) {
        final Entry entry = entries.get(id);
        if (entry == null) {
            return null;
        }
        ManagedCRDT<?> result = entry.getObject(query);
        if (result != null) {
            entry.touch();
        }
        return result;
    }

    /**
     * Returns object for given id without recording access to the cache (in
     * terms of eviction policy).
     * 
     * @param id
     *            object id
     * @return object or null if object is absent in the cache
     */
    synchronized public ManagedCRDT<?> getWithoutTouch(final CRDTIdentifier id, CRDTShardQuery<?> query) {
        final Entry entry = shadowEntries.get(id);
        return entry == null ? null : entry.getObject(query);
    }
    synchronized public List<ManagedCRDT<?>> getAllWithoutTouch(final CRDTIdentifier id) {
        final Entry entry = shadowEntries.get(id);
        return entry == null ? null : entry.getObjects();
    }

    /**
     * Evicts all objects that have not been accessed for over
     * evictionTimeMillis specified for this cache.
     */
    private void evictExcess() {
        int evictedObjects = 0;
        int excess = entries.size() - maxElements;
        for (Iterator<Map.Entry<CRDTIdentifier, Entry>> it = entries.entrySet().iterator(); it.hasNext();) {
            if (evictedObjects < excess) {
                Map.Entry<CRDTIdentifier, Entry> e = it.next();
                final Entry val = e.getValue();
                if (!evictionProtections.contains(val.id())) {
                    it.remove();
                    evictedObjects++;
                    shadowEntries.remove(e.getKey());
                    evictionListener.onEviction(e.getKey());
                    // System.err.println( e.getKey() +
                    // " evicted from the cache due to size limit, acesses:" +
                    // e.getValue().getNumberOfAccesses());
                }
            } else
                break;
        }
        if (evictedObjects > 0) {
            // System.err.printf("Objects evicted from the cache due to excessive size: %s / %s / %s\n",
            // evictedObjects,
            // entries.size(), maxElements);
            logger.info(evictedObjects + " objects evicted from the cache due to timeout");
        }
    }

    /**
     * Evicts all objects that have not been accessed for over
     * evictionTimeMillis specified for this cache.
     */
    private void evictOutdated() {
        long now = System.currentTimeMillis();
        final long evictionThreashold = now - evictionTimeMillis;

        int evictedObjects = 0;
        for (Iterator<Map.Entry<CRDTIdentifier, Entry>> it = entries.entrySet().iterator(); it.hasNext();) {
            Map.Entry<CRDTIdentifier, Entry> e = it.next();
            final Entry entry = e.getValue();
            if (entry.getLastAcccessTimeMillis() <= evictionThreashold) {
                it.remove();
                evictedObjects++;
                shadowEntries.remove(e.getKey());
                evictionListener.onEviction(e.getKey());
            } else {
                break;
            }
        }
        // if (evictedObjects > 0)
        // System.err.println(evictedObjects +
        // " objects evicted from the cache due to timeout");

        logger.info(evictedObjects + " objects evicted from the cache due to timeout");
    }

    synchronized void augmentAllWithDCCausalClockWithoutMappings(final CausalityClock causalClock) {
        for (final Entry entry : entries.values()) {
            for (ManagedCRDT<?> crdt: entry.getObjects()) {
                crdt.augmentWithDCClockWithoutMappings(causalClock);
            }
        }
    }

    synchronized void augmentAllWithScoutTimestampWithoutMappings(Timestamp clientTimestamp) {
        for (final Entry entry : entries.values()) {
            for (ManagedCRDT<?> crdt: entry.getObjects()) {
                crdt.augmentWithScoutTimestamp(clientTimestamp);
            }
        }
    }

    synchronized void printStats() {
        SortedSet<Entry> se = new TreeSet<Entry>(entries.values());
        for (Entry i : se)
            System.err.println(i.getUID() + "/" + i.accesses);
    }

    static AtomicLong g_serial = new AtomicLong();

    private final class Entry implements Comparable<Entry> {
        private ManagedCRDT<?> fullReplica;
        private final LinkedList<SubEntry> partialReplicas;
        private long lastAccessTimeMillis;
        private long accesses;
        private long txnId;
        private long serial = g_serial.incrementAndGet();

        public Entry(final ManagedCRDT<?> object, CRDTShardQuery<?> query, long txnId) {
            this.partialReplicas = new LinkedList<SubEntry>();
            this.fullReplica = null;
            if (query == null) {
                this.fullReplica = object;
            } else {
                this.partialReplicas.addFirst(new SubEntry(object, query));
            }
            this.txnId = txnId;
            touch();
        }

        public void add(ManagedCRDT<?> object, CRDTShardQuery<?> query) {
            if (query == null) {
                fullReplica = object;
                // If we have a full replica we don't need to keep the older partial replicas
                partialReplicas.clear();
            } else {
                partialReplicas.addFirst(new SubEntry(object, query));
            }
        }

        public long id() {
            return txnId;
        }

        public List<ManagedCRDT<?>> getObjects() {
            List<ManagedCRDT<?>> result = new LinkedList<ManagedCRDT<?>>();
            if (fullReplica != null) {
                result.add(fullReplica);
            }
            for (SubEntry cprdt: partialReplicas) {
                result.add(cprdt.getObject());
            }
            return result;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public ManagedCRDT<?> getObject(CRDTShardQuery<?> query) {
            if (fullReplica != null) {
                return fullReplica;
            }
            for (SubEntry cprdt: partialReplicas) {
                if (query.isSubqueryOf((CRDTShardQuery)cprdt.getQuery())) {
                    return cprdt.getObject();
                }
            }
            return null;
        }
        
        public CRDTIdentifier getUID() {
            if (fullReplica != null) {
                return fullReplica.getUID();
            }
            return partialReplicas.getFirst().getObject().getUID();
        }

        public long getLastAcccessTimeMillis() {
            return lastAccessTimeMillis;
        }

        public long getNumberOfAccesses() {
            return accesses;
        }

        public void touch() {
            accesses++;
            lastAccessTimeMillis = System.currentTimeMillis();
        }

        @Override
        public int compareTo(Entry other) {
            if (accesses == other.accesses)
                return serial < other.serial ? -1 : 1;
            else
                return accesses < other.accesses ? -1 : 1;
        }
        
        private final class SubEntry {
            private final ManagedCRDT<?> replica;
            private final CRDTShardQuery<?> query;
            
            public SubEntry(ManagedCRDT<?> replica, CRDTShardQuery<?> query) {
                this.replica = replica;
                this.query = query;
            }
            
            public ManagedCRDT<?> getObject() {
                return replica;
            }
            
            public CRDTShardQuery<?> getQuery() {
                return query;
            }
        }
    }
}
