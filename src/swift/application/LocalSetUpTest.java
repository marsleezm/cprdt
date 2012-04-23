package swift.application;

import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class LocalSetUpTest {
    static String sequencerName = "localhost";

    public static void main(String[] args) {
        Thread sequencer = new Thread() {
            public void run() {
                DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
                sequencer.start();
            }
        };
        sequencer.start();

        Thread server = new Thread() {
            public void run() {
                DCServer server = new DCServer(sequencerName);
                server.startSurrogServer();
            }
        };
        server.start();

        for (int i = 0; i < 5; i++) {
            final int portId = i + 2000;
            Thread client = new Thread("client" + i) {
                public void run() {
                    Sys.init();
                    SwiftImpl clientServer = SwiftImpl.newInstance(portId, "localhost", DCConstants.SURROGATE_PORT);
                    clientCode(clientServer);
                    clientServer.stop(true);
                }

            };
            client.start();
        }
    }

    private static void clientCode(SwiftImpl server) {
        try {
            TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
            System.out.println("(e,1) = " + i1.getValue());
            IntegerTxnLocal i2 = handle.get(new CRDTIdentifier("e", "2"), false, swift.crdt.IntegerVersioned.class);
            System.out.println("(e,2) = " + i2.getValue());
            i1.add(1);
            System.out.println("(e,1).add(1)");
            System.out.println("(e,1) = " + i1.getValue());
            handle.commit();
            System.out.println("commit");

            System.out.println(Thread.currentThread().getName() + " finished successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
