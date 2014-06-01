package swift.crdt;

import java.util.Set;

import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;
import swift.crdt.core.CRDT;

public class SetIntervalShardQuery<V extends Comparable<V>> implements CRDTShardQuery<AddWinsSortedSetCRDT<V>> {
    
    protected V fromNonInclusive;
    protected V toInclusive;
    
    // Kryo
    public SetIntervalShardQuery() {
    }
    
    public SetIntervalShardQuery(V fromNonInclusive, V toInclusive) {
        this.fromNonInclusive = fromNonInclusive;
        this.toInclusive = toInclusive;
    }

    @Override
    public AddWinsSortedSetCRDT<V> executeAt(AddWinsSortedSetCRDT<V> crdtVersion, AddWinsSortedSetCRDT<V> crdtPruneVersion) {
        return crdtPruneVersion.copyInterval(fromNonInclusive, toInclusive);
    }

    @Override
    public boolean isAvailableIn(Shard shard) {
        return shard.containsInterval(fromNonInclusive, toInclusive);
    }
    
    @Override
    public boolean isSubqueryOf(CRDTShardQuery<AddWinsSortedSetCRDT<V>> other) {
        return false;
    }

    @Override
    public boolean isStateIndependent() {
        return true;
    }
}
