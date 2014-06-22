package swift.application.reddit.cprdt;

import java.util.HashSet;
import java.util.Set;

import swift.application.reddit.Date;
import swift.application.reddit.SortingOrder;
import swift.application.reddit.Thing;
import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;

public class IndexedVoteableSetSortedShardQuery<V extends Date<V>, U> implements CRDTShardQuery<IndexedVoteableSetCPRDT<V, U>> {
    protected SortingOrder sort;
    protected V after;
    protected V before;
    protected int limit;
    
    // Kryo
    public IndexedVoteableSetSortedShardQuery() {
    }
    
    public IndexedVoteableSetSortedShardQuery(SortingOrder sort, V after, V before, int limit) {
        this.sort = sort;
        this.after = after;
        this.before = before;
        this.limit = limit;
    }

    @Override
    public IndexedVoteableSetCPRDT<V, U> executeAt(IndexedVoteableSetCPRDT<V, U> crdtVersion, IndexedVoteableSetCPRDT<V, U> crdtPrunedVersion) {
        Set<V> queryResult = new HashSet<V>(crdtVersion.applyFind(sort, after, before, limit));
        return crdtPrunedVersion.copyFraction(queryResult);
    }
    
    @Override
    public boolean isSubqueryOf(CRDTShardQuery<IndexedVoteableSetCPRDT<V, U>> crdtShardQuery) {
        if (!(crdtShardQuery instanceof IndexedVoteableSetSortedShardQuery)) {
            return false;
        }
        if (this == crdtShardQuery) {
            return true;
        }
        return ((IndexedVoteableSetSortedShardQuery<V,U>) crdtShardQuery).isSuperqueryOf(sort, after, before, limit);
    }

    public boolean isSuperqueryOf(SortingOrder sort, V after, V before, int limit) {
        return (this.sort.equals(sort)) && (this.after == after || (this.after != null && this.after.equals(after)))
                && (this.before == before || (this.before != null && this.before.equals(before)))
                && (this.limit >= limit);
    }

    @Override
    public boolean isStateIndependent() {
        return false;
    }

    @Override
    public boolean isAvailableIn(Shard shard) {
        return shard.isFull();
    }
}
