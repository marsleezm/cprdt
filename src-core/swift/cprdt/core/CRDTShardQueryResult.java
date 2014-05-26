/*****************************************************************************
 * Copyright 2014 UCL
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

import swift.crdt.core.CRDT;

public class CRDTShardQueryResult<V extends CRDT<V>> {
    protected Shard shard;
    protected V crdt;
    
    public CRDTShardQueryResult(Shard shard, V crdt) {
        this.shard = shard;
        this.crdt = crdt;
    }
    
    public Shard getShard() {
        return shard;
    }
    
    public V getCrdt() {
        return crdt;
    }
}
