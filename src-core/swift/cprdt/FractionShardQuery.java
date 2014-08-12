package swift.cprdt;

import java.util.Set;

import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;
import swift.crdt.core.CRDT;

/**
 * Basic shard query to request a set of particles
 * 
 * @author Iwan Briquemont
 *
 * @param <V>
 */
public class FractionShardQuery<V extends CRDT<V>> implements CRDTShardQuery<V> {
    
    protected Set<?> particles;
    
    // Kryo
    public FractionShardQuery() {
    }
    
    public FractionShardQuery(Set<?> particles) {
        this.particles = particles;
    }

    @Override
    public V executeAt(V crdtVersion, V crdtPruneVersion) {
        return crdtPruneVersion.copyFraction(particles);
    }

    @Override
    public boolean isAvailableIn(Shard shard) {
        return shard.containsAll(particles);
    }
    
    @Override
    public boolean isSubqueryOf(CRDTShardQuery<V> other) {
        if (!(other instanceof FractionShardQuery)) {
            return false;
        }
        return ((FractionShardQuery<V>) other).particles.containsAll(this.particles);
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
