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

public interface CRDTShardQuery<V extends CRDT<V>> {
    
    /**
     * 
     * @param crdtVersion The version the query should run against
     * @param crdtPruneVersion Can be an older version than the one the query should run against
     * @return A copy of the crdtPruneVersion CRDT restricted to the query
     */
    CRDTShardQueryResult<V> executeAt(V crdtVersion, V crdtPruneVersion);

    /**
     * 
     * @return True if this query always results in the same shard, regardless
     *         of the version of the CRDT (i.e. update(query(crdt)) == query(update(crdt)))
     */
    //boolean isStateIndependent();

    /**
     * 
     * @param crdtShardQuery
     * @return True if this is more specific than the other False if it is not
     *         or if it unknown
     */
    boolean isSubqueryOf(Shard<V> shard, CRDTShardQuery<V> crdtShardQuery);
}
