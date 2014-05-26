/*****************************************************************************
 * Copyright 2013-2014 UCL
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
package swift.cprdt.core;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Definition of the particles present in a partial CRDT. Immutable
 * 
 * @author Iwan Briquemont
 */
public class Shard {
    private boolean isFull;
    private Set<Object> particles;
    
    // TODO add interval and properties support
    
    public static final Shard fullShard = new Shard(true);
    public static final Shard hollowShard = new Shard();
    
    public Shard() {
        this(false);
    }
    
    public Shard(boolean isFull) {
        this(isFull, (Set<Object>)Collections.EMPTY_SET);
    }
    
    public Shard(Set<Object> particles) {
        this(false, particles);
    }
    
    private Shard(boolean isFull, Set<Object> particles) {
        this.isFull = isFull;
        this.particles = particles;
    }
    
    public boolean isFull() {
        return isFull;
    }
    
    public boolean isHollow() {
        return !isFull && this.particles.size() == 0;
    }
    
    public boolean contains(Object particle) {
        if (isFull) {
            return true;
        }
        return particles.contains(particle);
    }
    
    public boolean containsAll(Set<?> particles) {
        if (isFull) {
            return true;
        }
        if (particles == null) {
            // null is considered to be the full set
            return false;
        }
        return particles.containsAll(particles);
    }
    
    public boolean containsAny(Set<?> particles) {
        if (isHollow()) {
            return false;
        }
        if (particles == null) {
            // null is considered to be the full set
            return true;
        }
        if (particles.size() != 0 && isFull()) {
            return true;
        }
        for (Object particle: particles) {
            if (contains(particle)) {
                return true;
            }
        }
        return false;
    }
    
    public Shard union(Shard other) {
        if (this.isFull() || other.isHollow()) {
            return this;
        }
        if (other.isFull() || this.isHollow()) {
            return other;
        }
        Set<Object> union = new HashSet<Object>(this.particles);
        union.addAll(other.particles);
        return new Shard(union);
    }
    
    /**
     * @return true if this shard is a (non strict) subset of the other
     * false if it is not or if they cannot be compared
     */
    public boolean isSubsetOf(Shard other) {
        if (other.isFull) {
            return true;
        }
        return other.containsAll(particles);
    }
}
