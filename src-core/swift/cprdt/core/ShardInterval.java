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

import java.util.Comparator;
import java.util.Set;

import swift.crdt.core.CRDT;

/**
 * Shard defined by an interval on some property of the particles
 * 
 * @author Iwan Briquemont
 */
public class ShardInterval<V extends CRDT<V>> implements Shard<V> {
    protected Comparator<Object> comparator;
    protected Object from;
    protected boolean fromInclusive;
    protected Object to;
    protected boolean toInclusive;
    
    boolean comparatorEquals(Comparator<Object> otherComparator) {
        return comparator.equals(otherComparator);
    }
    
    boolean isSupersetOf(Object from, boolean fromInclusive, Object to, boolean toInclusive) {
        if (from == null && this.from != null) {
            return false;
        }
        if (to == null && this.to != null) {
            return false;
        }
        
        if (this.from != null && from != null) {
            if (comparator.compare(this.from, from) > 0) {
                return false;
            }
            if (comparator.compare(this.from, from) == 0 && !this.fromInclusive && fromInclusive) {
                return false;
            }
        }
        
        if (this.to != null && to != null) {
            if (comparator.compare(this.to, to) > 0) {
                return false;
            }
            if (comparator.compare(this.to, to) == 0 && !this.toInclusive && toInclusive) {
                return false;
            }
        }
        
        return true;
    }
    
    public ShardInterval(Comparator<Object> comparator, Object from, boolean fromInclusive, Object to, boolean toInclusive) {
        this.comparator = comparator;
        this.from = from;
        this.fromInclusive = fromInclusive;
        this.to = to;
        this.toInclusive = toInclusive;
    }
    
    public boolean contains(Object particle) {
        if (from != null) {
            if (comparator.compare(from, particle) > 0) {
                return false;
            }
            if (comparator.compare(from, particle) == 0 && !fromInclusive) {
                return false;
            }
        }
        if (to != null) {
            if (comparator.compare(particle, to) > 0) {
                return false;
            }
            if (comparator.compare(particle, to) == 0 && !toInclusive) {
                return false;
            }
        }
        return true;
    }
    
    public boolean containsAll(Set<?> particles) {
        for (Object particle: particles) {
            if (!contains(particle)) {
                return false;
            }
        }
        return true;
    }
    
    public boolean isSubsetOf(Shard<V> other) {
        if (other instanceof ShardFull<?>) {
            return true;
        }
        if (other instanceof ShardInterval<?>) {
            ShardInterval<V> otherInterval = (ShardInterval<V>)other;
            return otherInterval.isSupersetOf(from, fromInclusive, otherInterval, toInclusive);
        }
        return false;
    }
}
