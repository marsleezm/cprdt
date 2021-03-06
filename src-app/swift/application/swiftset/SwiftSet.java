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
package swift.application.swiftset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.AddWinsSortedSetCRDT;
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
import sys.utils.Threading;

/**
 * 
 * @author smduarte, annettebieniusa
 * 
 */
public class SwiftSet {
    private static String sequencerName = "localhost";
    private static String dcName = "localhost";
    static int iterations = 10;
    static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;
    static CachePolicy cachePolicy = CachePolicy.CACHED;
    static boolean notifications = true;
    static CRDTIdentifier j1 = new CRDTIdentifier("doc", "1");
    static CRDTIdentifier j2 = new CRDTIdentifier("doc", "2");

    public static void main(String[] args) {
        System.out.println("SwiftSet start!");
        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        // start DC server
        DCServer.main(new String[] { dcName });

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                Sys.init();
                SwiftSession swift1 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                        DCConstants.SURROGATE_PORT));
                SwiftSession swift2 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                        DCConstants.SURROGATE_PORT));
                runClient1(swift1, swift2);
            }
        }).start();

        Threading.sleep(1000);

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                Sys.init();
                SwiftSession swift1 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                        DCConstants.SURROGATE_PORT));
                SwiftSession swift2 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                        DCConstants.SURROGATE_PORT));
                runClient2(swift1, swift2);
            }
        }).start();
    }

    static void runClient1(SwiftSession swift1, SwiftSession swift2) {
        client1code(swift1, swift2);
    }

    static void runClient2(SwiftSession swift1, SwiftSession swift2) {
        client2code(swift1, swift2);
    }

    static void client1code(final SwiftSession swift1, final SwiftSession swift2) {
        try {
            final AtomicBoolean done = new AtomicBoolean(false);
            final Map<Long, TextLine> samples = new HashMap<Long, TextLine>();

            Threading.newThread(true, new Runnable() {
                public void run() {
                    try {
                        for (int k = 0; !done.get(); k++) {
                            final Object barrier = new Object();
                            final TxnHandle handle = swift2.beginTxn(isolationLevel, k == 0 ? CachePolicy.MOST_RECENT
                                    : CachePolicy.CACHED, true);
                            AddWinsSortedSetCRDT<TextLine> doc = handle.get(j2, true,
                                    swift.crdt.AddWinsSortedSetCRDT.class, new AbstractObjectUpdatesListener() {
                                        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id,
                                                CRDT<?> previousValue) {
                                            Threading.synchronizedNotifyAllOn(barrier);
                                            // System.err.println("Triggered Reader get():"
                                            // + previousValue.getValue());
                                        }
                                    });
                            for (TextLine i : doc.getValue()) {
                                if (!samples.containsKey(i.serial())) {
                                    samples.put(i.serial(), i);
                                }
                            }
                            Threading.synchronizedWaitOn(barrier, 1000);
                            handle.commit();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            SwiftSetPatchReplay<TextLine> player = new SwiftSetPatchReplay<TextLine>();

            player.parseFiles(new SwiftSetOps<TextLine>() {
                TxnHandle handle = null;
                AddWinsSortedSetCRDT<TextLine> doc = null;

                @Override
                public void begin() {
                    try {
                        handle = swift1.beginTxn(isolationLevel, cachePolicy, false);
                        doc = handle.get(j1, true, swift.crdt.AddWinsSortedSetCRDT.class, null);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        System.exit(0);
                    }
                }

                public void remove(TextLine v) {
                    doc.remove(v);
                }

                @Override
                public void add(TextLine atom) {
                    doc.add(atom);
                }

                @Override
                public int size() {
                    return doc.size();
                }

                @Override
                public void commit() {
                    handle.commit();
                    Threading.sleep(100);
                }

                @Override
                public TextLine gen(String s) {
                    return new TextLine(s);
                }
            });
            done.set(true);

            for (TextLine i : new ArrayList<TextLine>(samples.values()))
                System.out.printf("%s\n", i.latency());

            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void client2code(final SwiftSession swift1, final SwiftSession swift2) {
        try {

            final Set<Long> serials = new HashSet<Long>();

            for (int k = 0;; k++) {
                final Object barrier = new Object();
                final TxnHandle handle = swift1.beginTxn(isolationLevel, k == 0 ? CachePolicy.MOST_RECENT
                        : CachePolicy.CACHED, false);
                AddWinsSortedSetCRDT<TextLine> doc = handle.get(j1, true, swift.crdt.AddWinsSortedSetCRDT.class,
                        new AbstractObjectUpdatesListener() {
                            public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, CRDT<?> previousValue) {
                                Threading.synchronizedNotifyAllOn(barrier);
                                // System.err.println("previous:" +
                                // previousValue.getValue());
                            }
                        });
                handle.commit();

                // SortedSetTxnLocal<TextLine> doc = handle.get(j1, true,
                // swift.crdt.SortedSetVersioned.class, null);

                final Collection<TextLine> newAtoms = new ArrayList<TextLine>();
                for (TextLine i : doc.getValue()) {
                    if (serials.add(i.serial())) {
                        newAtoms.add(i);
                    }
                }
                // Wait for the notification, before reading the new value of
                // the sequence...
                Threading.synchronizedWaitOn(barrier, 1000);
                // System.err.println("Triggered Reader get():" +
                // doc.getValue());

                // Determine the new atoms this update brought...

                // Write the atoms into the other sequence to measure RTT...
                Threading.newThread(true, new Runnable() {
                    public void run() {
                        synchronized (serials) {// wait for the previous
                                                // transaction to complete...
                            try {
                                final TxnHandle handle = swift2.beginTxn(isolationLevel, CachePolicy.CACHED, false);
                                AddWinsSortedSetCRDT<TextLine> doc2 = handle.get(j2, true,
                                        swift.crdt.AddWinsSortedSetCRDT.class, null);
                                for (TextLine i : newAtoms)
                                    doc2.add(i);
                                handle.commit();
                            } catch (Exception x) {
                                x.printStackTrace();
                            }
                        }
                    }
                }).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
