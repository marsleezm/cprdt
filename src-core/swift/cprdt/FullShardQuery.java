package swift.cprdt;

import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;
import swift.crdt.core.CRDT;

public class FullShardQuery<V extends CRDT<V>> implements CRDTShardQuery<V> {
    
    public FullShardQuery() {
    }

    @Override
    public V executeAt(V crdtVersion, V crdtPruneVersion) {
        return crdtPruneVersion.copy();
    }

    @Override
    public boolean isAvailableIn(Shard shard) {
        return shard.isFull();
    }
    
    @Override
    public boolean isSubqueryOf(CRDTShardQuery<V> other) {
        if (!(other instanceof FullShardQuery)) {
            return false;
        }
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
