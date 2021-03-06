package swift.application.swiftlinks.cprdt;

import java.util.HashSet;
import java.util.Set;

import swift.application.swiftlinks.Dateable;
import swift.application.swiftlinks.SortingOrder;
import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;

/**
 * 
 * @author iwan Briquemont
 *
 * @param <V> Element of the set
 * @param <U> Voter
 */
public class IndexedVoteableSetSortedShardQuery<V extends Dateable<V>, U> implements CRDTShardQuery<IndexedVoteableSetCPRDT<V, U>> {
    protected SortingOrder sort;
    protected V after;
    protected V before;
    protected int limit;
    
    protected boolean strictMatching;
    
    // Kryo
    public IndexedVoteableSetSortedShardQuery() {
    }
    
    public IndexedVoteableSetSortedShardQuery(SortingOrder sort, V after, V before, int limit) {
        this(sort, after, before, limit, false);
    }
    
    public IndexedVoteableSetSortedShardQuery(SortingOrder sort, V after, V before, int limit, boolean strictMatching) {
        this.sort = sort;
        this.after = after;
        this.before = before;
        this.limit = limit;
        this.strictMatching = strictMatching;
    }

    @Override
    public IndexedVoteableSetCPRDT<V, U> executeAt(IndexedVoteableSetCPRDT<V, U> crdtVersion, IndexedVoteableSetCPRDT<V, U> crdtPrunedVersion) {
        Set<V> queryResult = new HashSet<V>(crdtVersion.applyFind(sort, after, before, limit));
        if (after != null) {
            queryResult.add(after);
        }
        if (before != null) {
            queryResult.add(before);
        }
        return crdtPrunedVersion.copyFraction(queryResult);
    }
    
    @SuppressWarnings("unchecked")
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
    
    @Override
    public long allowedCacheTimeThreshold(long systemThreshold) {
        if (strictMatching) {
            return -1;
        } else {
            return systemThreshold;
        }
    }
}
