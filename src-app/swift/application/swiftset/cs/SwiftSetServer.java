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
package swift.application.swiftset.cs;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import swift.application.swiftdoc.TextLine;
import swift.application.swiftdoc.cs.msgs.AppRpcHandler;
import swift.application.swiftdoc.cs.msgs.BeginTransaction;
import swift.application.swiftdoc.cs.msgs.BulkTransaction;
import swift.application.swiftdoc.cs.msgs.CommitTransaction;
import swift.application.swiftdoc.cs.msgs.InitScoutServer;
import swift.application.swiftdoc.cs.msgs.InsertAtom;
import swift.application.swiftdoc.cs.msgs.RemoveAtom;
import swift.application.swiftdoc.cs.msgs.ServerACK;
import swift.application.swiftdoc.cs.msgs.ServerReply;
import swift.application.swiftdoc.cs.msgs.SwiftDocRpc;
import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.SequenceCRDT;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

/**
 * 
 * @author smduarte
 * 
 */
public class SwiftSetServer extends Thread {
    public static int PORT1 = 11111, PORT2 = 11112;

    static String dcName = "localhost";
    private static String sequencerName = "localhost";

    static boolean synchronousOps = false;

    static boolean notifications = true;
    static long cacheEvictionTimeMillis = 60000;
    static CachePolicy cachePolicy = CachePolicy.CACHED;
    static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;

    static CRDTIdentifier j1id = new CRDTIdentifier("swiftdoc1", "1");
    static CRDTIdentifier j2id = new CRDTIdentifier("swiftdoc2", "1");

    public static void main(String[] args) {
        System.out.println("SwiftDoc Server start!");
        Sys.init();

        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        // start DC server
        DCServer.main(new String[] { dcName });

        Threading.sleep(5000);
        System.out.println("SwiftDoc Launching scouts...!");

        Threading.newThread("scoutServer1", true, new Runnable() {
            public void run() {
                runScoutServer1();
            }
        }).start();

        Threading.newThread("scoutServer2", true, new Runnable() {
            public void run() {
                runScoutServer2();
            }
        }).start();
    }

    static void runScoutServer1() {
        scoutServerCommonCode(PORT1, j1id, j2id);
    }

    static void runScoutServer2() {
        scoutServerCommonCode(PORT2, j2id, j1id);
    }

    public void run() {
        notifyClient();
    }

    static void scoutServerCommonCode(final int port, final CRDTIdentifier d1, final CRDTIdentifier d2) {
        try {

            Networking.rpcBind(port, TransportProvider.DEFAULT).toService(0, new AppRpcHandler() {

                public void onReceive(RpcHandle client, final InitScoutServer r) {
                    client.enableDeferredReplies(Integer.MAX_VALUE);
                    getSession(client.remoteEndpoint(), client, d1, d2);
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final BeginTransaction r) {
                    getSession(client.remoteEndpoint()).swiftdoc.begin();
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final CommitTransaction r) {
                    getSession(client.remoteEndpoint()).swiftdoc.commit();
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final InsertAtom r) {
                    getSession(client.remoteEndpoint()).swiftdoc.add(r.pos, r.atom);
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final RemoveAtom r) {
                    getSession(client.remoteEndpoint()).swiftdoc.remove(r.pos);
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final BulkTransaction r) {
                    long t0 = System.currentTimeMillis();
                    Session s = getSession(client.remoteEndpoint());
                    s.swiftdoc.begin();
                    for (SwiftDocRpc i : r.ops)
                        if (i instanceof InsertAtom) {
                            InsertAtom j = (InsertAtom) i;
                            s.swiftdoc.add(j.pos, j.atom);
                        } else if (i instanceof RemoveAtom) {
                            RemoveAtom j = (RemoveAtom) i;
                            s.swiftdoc.remove(j.pos);
                        }
                    long t1 = System.currentTimeMillis();
                    s.swiftdoc.commit();
                    long now = System.currentTimeMillis();
                    // System.err.printf(
                    // "Bulk Ops: %s total time:%s ms commit time: %s ms\n",
                    // r.ops.size(), (now - t0), (now - t1));
                    client.reply(new ServerACK(r));
                }

            });
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    TxnHandle handle = null;
    CRDTIdentifier j1 = null, j2 = null;
    SequenceCRDT<TextLine> doc = null;

    RpcHandle clientHandle = null;
    SwiftSession swift1 = null;

    SwiftSession swift2 = null;

    SwiftSetServer(SwiftSession swift12, SwiftSession swift22, RpcHandle client, CRDTIdentifier j1, CRDTIdentifier j2) {
        this.j1 = j1;
        this.j2 = j2;
        this.swift1 = swift12;
        this.swift2 = swift22;
        this.clientHandle = client.enableDeferredReplies(Integer.MAX_VALUE);
    }

    public void begin() {
        try {
            handle = swift1.beginTxn(isolationLevel, cachePolicy, false);
            doc = handle.get(j1, true, swift.crdt.SequenceCRDT.class, null);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void add(int pos, TextLine atom) {
        if (pos < 0)
            pos = doc.size();

        doc.insertAt(pos, atom);
    }

    public TextLine remove(int pos) {
        TextLine res = doc.removeAt(pos);
        return res;
    }

    public void commit() {
        handle.commit();
        handle = null;
        doc = null;
    }

    void notifyClient() {
        try {
            final Set<Long> serials = new HashSet<Long>();
            for (int k = 0; true; k++) {

                final Object barrier = new Object();
                final TxnHandle handle = swift2.beginTxn(isolationLevel, k == 0 ? CachePolicy.MOST_RECENT
                        : CachePolicy.CACHED, true);

                SequenceCRDT<TextLine> doc = handle.get(j2, true, swift.crdt.SequenceCRDT.class,
                        new AbstractObjectUpdatesListener() {
                            @Override
                            public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, CRDT<?> previousValue) {

                                Threading.synchronizedNotifyAllOn(barrier);
                                // System.err.println("Triggered Reader get():"
                                // + j2 + "  "+ previousValue.getValue());
                            }
                        });

                List<TextLine> newAtoms = new ArrayList<TextLine>();
                for (TextLine i : doc.getValue())
                    if (serials.add(i.serial())) {
                        newAtoms.add(i);
                    }
                if (newAtoms.size() > 0)
                    clientHandle.reply(new ServerReply(newAtoms));

                handle.commit();

                Threading.synchronizedWaitOn(barrier, 10);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static Session getSession(Object sessionId) {
        return sessions.get(sessionId);
    }

    static Session getSession(Object sessionId, RpcHandle client, CRDTIdentifier j1, CRDTIdentifier j2) {
        Session res = sessions.get(sessionId);
        if (res == null) {
            sessions.put(sessionId, res = new Session(client, j1, j2));
        }
        return res;
    }

    static Map<Object, Session> sessions = new HashMap<Object, Session>();

    static class Session {
        SwiftSession swift1;
        final SwiftSession swift2;
        final RpcHandle client;
        final SwiftSetServer swiftdoc;

        Session(RpcHandle client, CRDTIdentifier j1, CRDTIdentifier j2) {
            this.client = client;

            final SwiftOptions options = new SwiftOptions(dcName, DCConstants.SURROGATE_PORT);
            options.setCacheEvictionTimeMillis(cacheEvictionTimeMillis);
            options.setCacheSize(Integer.MAX_VALUE);
            options.setDisasterSafe(false);
            this.swift1 = SwiftImpl.newSingleSessionInstance(options);
            this.swift2 = SwiftImpl.newSingleSessionInstance(options);

            swiftdoc = new SwiftSetServer(swift1, swift2, client, j1, j2);
            swiftdoc.start();
        }
    }

}
