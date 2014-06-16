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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Definition of the particles present in a partial CRDT. Immutable
 * 
 * @author Iwan Briquemont
 */
public class Shard {
    private boolean isFull;
    private Set<Object> particles;

    // TODO add properties support, maybe mutiple intervals with different
    // comparators
    private IntervalSet interval;

    public static final Shard full = new Shard(true);
    public static final Shard hollow = new Shard();

    public Shard() {
        this(false);
    }

    public Shard(boolean isFull) {
        this(isFull, Collections.emptySet(), IntervalSet.empty);
    }

    public Shard(Set<?> particles) {
        this(false, particles, IntervalSet.empty);
    }

    public Shard(Comparable<?> fromNonInclusive, Comparable<?> toInclusive) {
        this(false, Collections.emptySet(), new IntervalSet(fromNonInclusive, toInclusive));
    }

    private Shard(boolean isFull, Set<?> particles, IntervalSet interval) {
        this.isFull = isFull;
        this.particles = (Set<Object>) particles;
        this.interval = interval;
    }
    
    public String toString() {
        return interval.toString();
    }

    public boolean isFull() {
        return isFull;
    }

    public boolean isHollow() {
        return !isFull && this.particles.size() == 0 && interval.isEmpty();
    }

    public boolean contains(Object particle) {
        if (isFull) {
            return true;
        }
        if (this.particles.contains(particle)) {
            return true;
        }
        if (particle instanceof Comparable && interval.contains((Comparable) particle)) {
            return true;
        }
        return false;
    }

    public boolean containsInterval(Comparable<?> fromNonInclusive, Comparable<?> toInclusive) {
        return interval.containsInterval(fromNonInclusive, toInclusive);
    }

    public boolean containsAll(Set<?> particles) {
        if (isFull) {
            return true;
        }
        if (particles == null) {
            // null is considered to be the full set
            return false;
        }
        for (Object particle : particles) {
            if (!contains(particle)) {
                return false;
            }
        }
        return true;
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
        for (Object particle : particles) {
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
        return new Shard(false, union, this.interval.union(other.interval));
    }

    static class IntervalSet {
        // Ordered set of intervals, without any overlapping
        // nor consecutive intervals (e.g. (1,3) and (3,5))
        List<Interval> intervals;
        static IntervalSet empty = new IntervalSet();

        IntervalSet() {
            this.intervals = Collections.emptyList();
        }

        IntervalSet(Comparable<?> fromNonInclusive, Comparable<?> toInclusive) {
            this.intervals = Collections.singletonList(new Interval(fromNonInclusive, toInclusive));
        }

        private IntervalSet(List<Interval> intervals) {
            this.intervals = intervals;
        }

        boolean isEmpty() {
            return this.intervals.size() == 0;
        }

        boolean contains(Comparable<?> element) {
            // TODO smarter search
            for (Interval interval : intervals) {
                if (interval.contains(element)) {
                    return true;
                } else {
                    if (interval.from.compareTo(element) > 0) {
                        // element is before the interval
                        // Since the intervals are in order => element not there
                        return false;
                    }
                }
            }
            return false;
        }

        boolean containsInterval(Comparable<?> from, Comparable<?> to) {
            for (Interval interval : intervals) {
                if (interval.containsInterval(from, to)) {
                    return true;
                } else {
                    if (interval.to.compareTo(to) >= 0) {
                        // end of asked interval is before the end of this
                        // interval
                        // Since the intervals are in order => interval not
                        // there
                        return false;
                    }
                }
            }
            return false;
        }

        IntervalSet union(IntervalSet other) {
            if (other.isEmpty()) {
                return this;
            }
            if (this.isEmpty()) {
                return other;
            }
            List<Interval> union = new ArrayList<Interval>(this.intervals.size() + other.intervals.size() + 1);
            int indexThis = 0;
            int indexOther = 0;
            int i = 0;
            Interval next;
            Interval currentInterval = null;
            while (indexThis < this.intervals.size() || indexOther < other.intervals.size()) {
                // Loop on both this and other intervals in order
                if (indexThis == this.intervals.size()
                        || (indexOther != other.intervals.size() && other.intervals.get(indexOther).compareTo(
                                this.intervals.get(indexThis)) < 0)) {
                    next = other.intervals.get(indexOther);
                    indexOther++;
                } else {
                    next = this.intervals.get(indexThis);
                    indexThis++;
                }
                if (currentInterval == null) {
                    currentInterval = next;
                    continue;
                }
                if (currentInterval.mergeableWith(next)) {
                    currentInterval = currentInterval.union(next);
                } else {
                    union.add(currentInterval);
                    i++;
                    currentInterval = next;
                }
            }
            if (i == union.size()) {
                union.add(currentInterval);
            }

            return new IntervalSet(union);
        }
        
        public String toString() {
            StringBuilder b = new StringBuilder();
            for (Interval i: intervals) {
                b.append(i.from);
                b.append("-");
                b.append(i.to);
                b.append(", ");
            }
            return b.toString();
        }

        class Interval implements Comparable<Interval> {
            // TODO support fromInclusive, toInclusive
            // For the moment from is not inclusive, and to is inclusive
            Comparable<Object> from;
            Comparable<Object> to;
            
            // TODO support open intervals

            Interval(Comparable<?> from, Comparable<?> to) {
                this.from = (Comparable<Object>) from;
                this.to = (Comparable<Object>) to;
            }

            boolean contains(Comparable<?> element) {
                return this.from.compareTo(element) < 0 && this.to.compareTo(element) >= 0;
            }

            boolean containsInterval(Comparable<?> otherFrom, Comparable<?> otherTo) {
                return this.from.compareTo(otherFrom) <= 0 && this.to.compareTo(otherTo) >= 0;
            }

            /**
             * this intersects with other 
             * Or they are next to each other 
             * (e.g.: ]1, 2] and ]2, 3] => ]1, 3])
             * 
             * @param other
             * @return
             */
            boolean mergeableWith(Interval other) {
                if (this.to.compareTo(other.from) < 0) {
                    return false;
                }
                if (other.to.compareTo(this.from) < 0) {
                    return false;
                }
                return true;
            }

            Interval union(Interval other) {
                assert (this.mergeableWith(other));
                Comparable<Object> newFrom = this.from.compareTo(other.from) < 0 ? this.from : other.from;
                Comparable<Object> newTo = this.to.compareTo(other.to) > 0 ? this.to : other.to;
                return new Interval(newFrom, newTo);
            }

            @Override
            public int compareTo(Interval other) {
                return this.from.compareTo(other.from);
            }
        }
    }
}
