/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
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
package swift.crdt.core;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.cprdt.core.CRDTShardQuery;
import swift.cprdt.core.Shard;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

// WISHME: separate client and system interface
// TODO: Extend interface to offer optional buffering of transaction's updates (e.g. to aggregate increments, assignments etc.)
// with explicit transaction close.
/**
 * Operation-based CRDT object. An object can be optionally associated with a
 * transaction handle {@link TxnHandle} and clock {@link CausalityClock}. It
 * accepts three kind of operations:
 * <ol>
 * <li>application queries with no side-effects,</li>
 * <li>application updates with side-effects (only if associated with a handle),
 * </li>
 * <li>downstream updates with side-effects (only if not associated with a
 * handle).</li>
 * </ol>
 * Application update generates a downstream update {@link CRDTUpdate} using a
 * timestamp provided by a handle and registered in the handle. Downstream
 * update is eventually delivered exactly-once to other object instances in
 * causal order. Since the older of delivery of concurrent downstream updates
 * may be different on each replica, all concurrent updates must generate
 * donwstream updates that commute.
 * <p>
 * See {@link BaseCRDT} for more information on how to implement an object.
 * 
 * @author mzawirsk
 * @param <V>
 *            target CRDT type (subclass)
 */
public interface CRDT<V extends CRDT<V>> extends Copyable {
    /**
     * @return observable content of the object (without metadata)
     */
    Object getValue();

    /**
     * @return unique id of this object
     */
    CRDTIdentifier getUID();

    /**
     * @return associated transaction handle (if there is one; otherwise null)
     *         where updates are registered and timestamps generated
     */
    TxnHandle getTxnHandle();

    /**
     * <b>INTERNAL, SYSTEM USE.</b>
     * 
     * @see #getTxnHandle()
     * @return clock with the set of updates included in this object if the
     *         object is associated with a transaction handle; otherwise null
     */
    CausalityClock getClock();
    
    Shard getShard();
    void setShard(Shard shard);
    
    /**
     * <b>INTERNAL, SYSTEM USE.</b>
     * 
     * Merge this part of CRDT with another part on the same version
     * This replica can have non-committed changes so in case of overlapping
     * the parts of this must be chosen over the parts of other
     * Only used when at least one of the CRDTs is not a full replica
     * 
     * @param other
     */
    void mergeSameVersion(V other);
    
    public void fetch(Set<?> particles) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;
    public void fetch(CRDTShardQuery<V> query) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;
    
    /**
     * Make a partial copy of the CRDT
     * 
     * @param particles
     * @return
     */
    public V copyFraction(Set<?> particles);

    @Override
    public V copy();

    /**
     * <b>INTERNAL, SYSTEM USE.</b>
     * 
     * @param txnHandle
     * @param clock
     * @return a copy of the object with provided txnHandle and clock installed
     */
    public V copyWith(TxnHandle txnHandle, CausalityClock clock);
}
