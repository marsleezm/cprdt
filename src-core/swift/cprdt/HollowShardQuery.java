package swift.cprdt;

import java.util.Collections;

import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;
import swift.crdt.core.CRDT;

/**
 * Shard query to get a hollow replica of an object
 * 
 * @author Iwan Briquemont
 *
 * @param <V>
 */
public class HollowShardQuery<V extends CRDT<V>> implements CRDTShardQuery<V> {
    
    public HollowShardQuery() {
    }

    @Override
    public V executeAt(V crdtVersion, V crdtPruneVersion) {
        return crdtPruneVersion.copyFraction(Collections.emptySet());
    }

    @Override
    public boolean isAvailableIn(Shard shard) {
        return true;
    }
    
    @Override
    public boolean isSubqueryOf(CRDTShardQuery<V> other) {
        return true;
    }

    @Override
    public boolean isStateIndependent() {
        return true;
    }
    
    @Override
    public long allowedCacheTimeThreshold(long systemThreshold) {
        return -1;
    }
}
