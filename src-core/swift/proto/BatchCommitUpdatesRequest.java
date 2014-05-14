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

import java.util.LinkedList;
import java.util.List;

import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTUpdate;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.impl.KryoLib;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;

/**
 * Client batch request to commit a a sequence of transactions. Transactions
 * should be committed sequentially and a depedency clock of <em>i+1</em>-th
 * should be updated to include system timestamp of <em>i</em>-th transaction.
 * 
 * @author mzawirski
 */
public class BatchCommitUpdatesRequest extends ClientRequest implements MetadataSamplable {
    protected LinkedList<CommitUpdatesRequest> commitRequests;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    BatchCommitUpdatesRequest() {
    }

    /**
     * Creates a batch commit request made of provided list of requests.
     * 
     * @param clientId
     *            client id
     * @param commitRequests
     *            sequence of commit requests from the client
     */
    public BatchCommitUpdatesRequest(String clientId, List<CommitUpdatesRequest> commitRequests) {
        super(clientId);
        this.commitRequests = new LinkedList<CommitUpdatesRequest>(commitRequests);
    }

    /**
     * @return sequence of commit updates requests, one possibly dependent on
     *         another
     */
    // TODO: Weaken the dependencies?
    public LinkedList<CommitUpdatesRequest> getCommitRequests() {
        return commitRequests;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        final Kryo kryo = collector.getKryo();
        final ByteBufferOutput buffer = collector.getKryoBuffer();

        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();
        buffer.clear();

        int updatesSize = 0;
        for (final CommitUpdatesRequest req : commitRequests) {
            for (final CRDTObjectUpdatesGroup<?> group : req.getObjectUpdateGroups()) {
                group.getCreationState();
                for (final CRDTUpdate<?> op : group.getOperations()) {
                    kryo.writeObject(buffer, op);
                }
            }
        }
        updatesSize = buffer.position();
        buffer.clear();

        int valuesSize = 0;
        for (final CommitUpdatesRequest req : commitRequests) {
            for (final CRDTObjectUpdatesGroup<?> group : req.getObjectUpdateGroups()) {
                group.getCreationState();
                for (final CRDTUpdate<?> op : group.getOperations()) {
                    kryo.writeObject(buffer, op.getValueWithoutMetadata());
                }
            }
        }
        valuesSize = buffer.position();
        collector.recordStats(getClass().getSimpleName(), totalSize, updatesSize, valuesSize);
    }
}
