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

import java.util.Set;

import swift.crdt.core.CRDT;

/**
 * Shard defined by a set of particles
 * 
 * @author Iwan Briquemont
 */
public class ShardSet<V extends CRDT<V>> implements Shard<V> {
    protected Set<?> particles;
    
    public ShardSet(Set<?> particles) {
        this.particles = particles;
    }
    
    Set<?> getParticleSet() {
        return particles;
    }
    
    public boolean contains(Object particle) {
        return this.particles.contains(particle);
    }
    
    public boolean containsAll(Set<?> particles) {
        return this.particles.containsAll(particles);
    }
    
    public boolean isSubsetOf(Shard<V> other) {
        if (other instanceof ShardFull<?>) {
            return true;
        }
        if (other instanceof ShardSet<?>) {
            return ((ShardSet<V>) other).getParticleSet().containsAll(particles);
        }
        for (Object particle: particles) {
            if (!other.contains(particle)) {
                return false;
            }
        }
        return true;
    }
}
