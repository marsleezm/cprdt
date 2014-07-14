package swift.application.reddit.cprdt;

import java.util.HashSet;
import java.util.Set;

import swift.application.reddit.Date;
import swift.application.reddit.SortingOrder;
import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;

public class VoteableTreeSortedShardQuery<V extends Date<V>, U> implements CRDTShardQuery<VoteableTreeCPRDT<V, U>> {
    protected SortedNode<V> node;
    protected int context;
    protected SortingOrder sort;
    protected int limit;
    
    protected boolean strictMatching;
    
    // Kryo
    public VoteableTreeSortedShardQuery() {
    }
    
    public VoteableTreeSortedShardQuery(SortedNode<V> node, int context, SortingOrder sort, int limit) {
        this(node, context, sort, limit, false);
    }
    
    public VoteableTreeSortedShardQuery(SortedNode<V> node, int context, SortingOrder sort, int limit, boolean strictMatching) {
        this.node = node;
        this.context = context;
        this.sort = sort;
        this.limit = limit;
        this.strictMatching = strictMatching;
    }

    @Override
    public VoteableTreeCPRDT<V, U> executeAt(VoteableTreeCPRDT<V, U> crdtVersion, VoteableTreeCPRDT<V, U> crdtPrunedVersion) {
        Set<SortedNode<V>> queryResult = new HashSet<SortedNode<V>>(crdtVersion.applySortedSubtree(node, context, sort, limit));
        return crdtPrunedVersion.copyFraction(queryResult);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean isSubqueryOf(CRDTShardQuery<VoteableTreeCPRDT<V, U>> crdtShardQuery) {
        if (!(crdtShardQuery instanceof VoteableTreeSortedShardQuery)) {
            return false;
        }
        if (this == crdtShardQuery) {
            return true;
        }
        return ((VoteableTreeSortedShardQuery<V,U>) crdtShardQuery).isSuperqueryOf(node, context, sort, limit);
    }

    public boolean isSuperqueryOf(SortedNode<V> node, int context, SortingOrder sort, int limit) {
        return (this.sort.equals(sort)) && (this.node.equals(node))
                && (this.context >= context)
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
