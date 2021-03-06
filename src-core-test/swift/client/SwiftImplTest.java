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

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.same;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import swift.client.proto.CommitUpdatesReplyHandler;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReplyHandler;
import swift.client.proto.FastRecentUpdatesRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import swift.client.proto.FetchObjectVersionReplyHandler;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampReplyHandler;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampSource;
import swift.crdt.IntegerCRDT;
import swift.crdt.IntegerVersioned;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * SwiftImpl test using mock endpoint of a server, to drive test with
 * preprepared messages.
 * 
 * @author mzawirski
 */
// FIXME: adapt to 1PC
public class SwiftImplTest extends EasyMockSupport {
    private RpcEndpoint mockLocalEndpoint;
    private Endpoint mockServerEndpoint;
    private SwiftImpl swiftImpl;
    private CausalityClock serverClock;
    private TimestampSource<Timestamp> serverTimestampGen;

    private CRDTIdentifier idCrdtA;
    private CRDTIdentifier idCrdtB;
    private SwiftOptions options;

    @Before
    public void setUp() {
        mockLocalEndpoint = createMock(RpcEndpoint.class);
        mockServerEndpoint = createMock(Endpoint.class);
        serverClock = ClockFactory.newClock();
        serverTimestampGen = new IncrementalTimestampGenerator("server");
        options = new SwiftOptions("dummy", 0);
    }

    private SwiftImpl createSwift() {
        return new SwiftImpl(mockLocalEndpoint, mockServerEndpoint, new LRUObjectsCache(120 * 1000, 1000), options);
    }

    @After
    public void tearDown() {
        if (swiftImpl != null) {
            swiftImpl.stop(true);
            swiftImpl = null;
        }
    }

    @Ignore
    @Test
    public void testSingleSITxnCreateObject() throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        // Specify communication with the server mock.
        mockLocalEndpoint.send(same(mockServerEndpoint), isA(LatestKnownClockRequest.class),
                isA(LatestKnownClockReplyHandler.class), eq(options.getTimeoutMillis()));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
                ((LatestKnownClockReplyHandler) replyHandler).onReceive(null, new LatestKnownClockReply(serverClock,
                        serverClock));
                return null;
            }
        });
        mockLocalEndpoint.send(same(mockServerEndpoint), isA(FetchObjectVersionRequest.class),
                isA(FetchObjectVersionReplyHandler.class), eq(options.getTimeoutMillis()));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
                final FetchObjectVersionReply fetchReply = new FetchObjectVersionReply(FetchStatus.OBJECT_NOT_FOUND,
                        null, serverClock, ClockFactory.newClock(), serverClock, serverClock, -1, -1, -1);
                ((FetchObjectVersionReplyHandler) replyHandler).onReceive(null, fetchReply);
                return null;
            }
        });
        final Timestamp txn1Timestamp = serverTimestampGen.generateNew();
        mockLocalEndpoint.send(same(mockServerEndpoint), isA(GenerateTimestampRequest.class),
                isA(GenerateTimestampReplyHandler.class), eq(options.getTimeoutMillis()));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
                final GenerateTimestampRequest request = (GenerateTimestampRequest) m;
                assertFalse(request.getClientId().isEmpty());
                assertNull(request.getPreviousTimestamp());
                assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(request.getDominatedClock()));
                ((GenerateTimestampReplyHandler) replyHandler).onReceive(null, new GenerateTimestampReply(
                        txn1Timestamp, 1000));
                return null;
            }
        });

        mockLocalEndpoint.send(same(mockServerEndpoint), isA(CommitUpdatesRequest.class),
                isA(CommitUpdatesReplyHandler.class), eq(options.getTimeoutMillis()));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
                final CommitUpdatesRequest request = (CommitUpdatesRequest) m;
                assertFalse(request.getClientId().isEmpty());
                assertEquals(txn1Timestamp, request.getClientTimestamp());
                assertEquals(1, request.getObjectUpdateGroups().size());
                // Verify message integrity.
                assertEquals(request.getClientTimestamp(), request.getObjectUpdateGroups().get(0).getClientTimestamp());

                // FIXME: adapt to 1PC
                // ((CommitUpdatesReplyHandler) replyHandler).onReceive(null,
                // new CommitUpdatesReply(request.getClientTimestamp()));
                return null;
            }
        });

        mockLocalEndpoint.send(same(mockServerEndpoint), isA(FastRecentUpdatesRequest.class),
                isA(FastRecentUpdatesReplyHandler.class), eq(options.getTimeoutMillis()));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
                final FastRecentUpdatesRequest request = (FastRecentUpdatesRequest) m;
                assertFalse(request.getClientId().isEmpty());
                assertTrue(request.getMaxBlockingTimeMillis() > 0
                        && request.getMaxBlockingTimeMillis() <= options.getNotificationTimeoutMillis());

                // ((FastRecentUpdatesReplyHandler) replyHandler).onReceive(
                // null,
                // new FastRecentUpdatesReply(SubscriptionStatus.ACTIVE,
                // Collections
                // .<ObjectSubscriptionInfo> emptyList(),
                // ClockFactory.newClock(), ClockFactory.newClock()),
                // );
                return null;
            }
        }).anyTimes();
        replayAll();

        // Actual test: execute 1 transaction creating and updating object A.
        swiftImpl = createSwift();
        final AbstractTxnHandle txn = swiftImpl.beginTxn("session", IsolationLevel.SNAPSHOT_ISOLATION,
                CachePolicy.MOST_RECENT, true);
        final IntegerCRDT crdtA = txn.get(idCrdtA, true, IntegerVersioned.class);
        assertEquals(new Integer(0), crdtA.getValue());
        crdtA.add(5);
        txn.commit();

        verifyAll();
    }

    @Ignore
    @Test
    public void testSingleTxnRetrievePreexistingObject() throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException {
        // TODO
    }

    @Ignore
    @Test
    public void testTwoTxnsRefreshObject() {
        // TODO
    }

    @Ignore
    @Test
    public void testSingleTxnCacheObject() {
        // TODO
    }

    @Ignore
    @Test
    public void testConsistency() {
        // TODO
    }

    @Ignore
    @Test
    public void testQueuingUpLocalTransaction() {
        // TODO
    }

    private class DummyRpcEndpoint implements RpcEndpoint {
        @Override
        public Endpoint localEndpoint() {
            return null;
        }

        @Override
        public RpcHandle send(Endpoint dst, RpcMessage m) {
            return null;
        }

        @Override
        public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler) {
            return null;
        }

        @Override
        public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
            return null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends RpcEndpoint> T setHandler(final RpcHandler handler) {
            return (T) this;
        }

        @Override
        public RpcFactory getFactory() {
            return null;
        }

        @Override
        public void setDefaultTimeout(int ms) {
            // TODO Auto-generated method stub
        }

        @Override
        public int getDefaultTimeout() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public <T extends RpcMessage> T request(Endpoint dst, RpcMessage m) {
            // TODO Auto-generated method stub
            return null;
        }
    }
}
