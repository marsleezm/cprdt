package swift.application.reddit.cprdt;

import java.util.Set;

import swift.application.reddit.Date;
import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;

public class VoteableTreeChildrenShardQuery<V extends Date<V>, U> implements CRDTShardQuery<VoteableTreeCPRDT<V, U>> {
    protected SortedNode<V> node;
    
    // Kryo
    public VoteableTreeChildrenShardQuery() {
    }
    
    public VoteableTreeChildrenShardQuery(SortedNode<V> node) {
        this.node = node;
    }

    @Override
    public VoteableTreeCPRDT<V, U> executeAt(VoteableTreeCPRDT<V, U> crdtVersion, VoteableTreeCPRDT<V, U> crdtPrunedVersion) {
        Set<SortedNode<V>> queryResult = crdtVersion.applyAllChildrenOf(node);
        return crdtPrunedVersion.copyFraction(queryResult);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean isSubqueryOf(CRDTShardQuery<VoteableTreeCPRDT<V, U>> crdtShardQuery) {
        if (!(crdtShardQuery instanceof VoteableTreeChildrenShardQuery)) {
            return false;
        }
        if (this == crdtShardQuery) {
            return true;
        }
        return this.node.equals(((VoteableTreeChildrenShardQuery<V, U>) crdtShardQuery).node);
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
