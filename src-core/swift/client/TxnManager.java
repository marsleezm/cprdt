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
package swift.client;

import swift.clocks.CausalityClock;
import swift.cprdt.core.CRDTShardQuery;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.CRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * TODO: document
 * 
 * @author mzawirski
 */
public interface TxnManager {

    <V extends CRDT<V>> CRDT<V> getObjectLatestVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CachePolicy cachePolicy, boolean create, Class<V> classOfV, final ObjectUpdatesListener updatesListener, CRDTShardQuery<V> query)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

    <V extends CRDT<V>> CRDT<V> getObjectVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CausalityClock version, boolean create, Class<V> classOfV, ObjectUpdatesListener updatesListener, CRDTShardQuery<V> query)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

    void discardTxn(AbstractTxnHandle txn);

    void commitTxn(AbstractTxnHandle txn);
}
