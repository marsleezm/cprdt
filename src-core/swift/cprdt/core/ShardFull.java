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
 * For full replicas of a CRDT
 * 
 * @author Iwan Briquemont
 */
public class ShardFull<V extends CRDT<V>> implements Shard<V> {
    // Kryo
    public ShardFull() {
    }
    
    public boolean contains(Object particle) {
        return true;
    }
    
    public boolean containsAll(Set<?> particles) {
        return true;
    }
    
    public boolean isSubsetOf(Shard<V> other) {
        return (other instanceof ShardFull<?>);
    }
}
