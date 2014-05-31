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
package swift.proto;

import swift.clocks.CausalityClock;
import swift.cprdt.core.CRDTShardQuery;
import swift.crdt.core.CRDTIdentifier;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * Client request to fetch a particular version of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectVersionRequest extends ClientRequest implements MetadataSamplable {
    protected CRDTIdentifier uid;
    // TODO: could be derived from client's session?
    protected CausalityClock version;
    
    protected CRDTShardQuery query;
    
    // FIXME: make these things optional? Used only by evaluation.
    // protected CausalityClock clock;
    // protected CausalityClock disasterDurableClock;
    protected boolean strictUnprunedVersion;
    protected boolean subscribe;
    protected boolean sendDCVector;

    public long timestamp = sys.Sys.Sys.timeMillis(); // FOR EVALUATION, TO BE
                                                      // REMOVED...

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    FetchObjectVersionRequest() {
    }

    public FetchObjectVersionRequest(String clientId, boolean disasterSafe, CRDTIdentifier uid, CausalityClock version, CRDTShardQuery<?> query,
            final boolean strictUnprunedVersion, boolean subscribe, boolean sendDCVersion) {
        super(clientId, disasterSafe);
        this.uid = uid;
        this.version = version;
        this.query = query;
        this.subscribe = subscribe;
        this.strictUnprunedVersion = strictUnprunedVersion;
        this.sendDCVector = sendDCVersion;
    }

    // public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid,
    // CausalityClock version,
    // final boolean strictUnprunedVersion, CausalityClock clock, CausalityClock
    // disasterDurableClock,
    // boolean subscribe) {
    // super(clientId);
    // this.uid = uid;
    // this.clock = clock;
    // this.version = version;
    // this.subscribe = subscribe;
    // this.strictUnprunedVersion = strictUnprunedVersion;
    // this.disasterDurableClock = disasterDurableClock;
    // }

    public boolean isSendDCVector() {
        return sendDCVector;
    }

    public boolean hasSubscription() {
        return subscribe;
    }

    /**
     * @return id of the requested object
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    /**
     * @return minimum version requested
     */
    public CausalityClock getVersion() {
        return version;
    }
    
    public CRDTShardQuery getQuery() {
        return query;
    }

    /**
     * @return true strictly this (unpruned) version needs to be available in
     *         the reply; otherwise a more recent version is acceptable
     */
    public boolean isStrictAvailableVersion() {
        return strictUnprunedVersion;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate
     */
    public CausalityClock getClock() {
        return null;
        // return clock;
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate, durable in even in case of disaster affecting fragment
     *         of the store
     */
    public CausalityClock getDistasterDurableClock() {
        return null;
        // return disasterDurableClock;
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        final Kryo kryo = collector.getFreshKryo();
        final Output buffer = collector.getFreshKryoBuffer();

        // TODO: capture from the wire, rather than recompute here
        kryo.writeObject(buffer, this);
        collector.recordStats(this, buffer.position(), 0, 0, 1, version.getExceptionsNumber());
    }
}
