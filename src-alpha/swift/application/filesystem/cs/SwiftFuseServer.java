package swift.application.filesystem.cs;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import fuse.FuseMount;
import swift.application.filesystem.FilesystemBasic;
import swift.application.filesystem.cs.proto.RemoteFuseOperationHandler;
import swift.application.filesystem.fuse.FilesystemFuse;
import swift.application.swiftdoc.cs.msgs.AppRpcHandler;
import swift.client.SwiftImpl;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcMessage;

public class SwiftFuseServer extends RemoteFuseOperationHandler {
    public static final int PORT = 10001;
    
    private static final Log log = LogFactory.getLog(SwiftFuseServer.class);

    RpcEndpoint endpoint;
    
    SwiftFuseServer() {        
    }
    
    void init( String[] args ) {
        DCSequencerServer.main(new String[] { "-name", "localhost" });
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }
        DCServer.main(new String[] { "localhost" });
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }
        Sys.init();

        
        log.info("setting up servers");
        endpoint = Networking.rpcBind(PORT, TransportProvider.DEFAULT).toService(0, this);
        
        try {
            server = SwiftImpl.newInstance("localhost", DCConstants.SURROGATE_PORT);

            log.info("getting root directory");
            TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);

            // create a root directory
            // FIXME make this part of arguments
            fs = new FilesystemBasic(txn, ROOT, "DIR");
            txn.commit();


            System.out.println("SwiftFuseServer accepting requests...");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("exiting");
        }
    }
    
    public static void main(String[] args) {
        
        new SwiftFuseServer().init( args ) ;
        
    }

    
    public static void disposeFh( Object cfh ) {
        Object sfh = c2s_fh(cfh);
        c2s_fileHandles.remove( cfh);
        s2c_fileHandles.remove( sfh);
    }
    
   public static  Object s2c_fh( Object fh ) {
        Object res = s2c_fileHandles.get( fh ) ;
        if( res == null ) {
            res = g_fh.getAndIncrement();
            s2c_fileHandles.put( fh, res) ;
            c2s_fileHandles.put( res, fh) ;
        }
        return res;
    }
    
    public static Object c2s_fh( Object fh ) {
        return c2s_fileHandles.get( fh ) ;
    }
    
    static Map<Object, Object> c2s_fileHandles = new HashMap<Object, Object>();
    static Map<Object, Object> s2c_fileHandles = new HashMap<Object, Object >();
    
    static AtomicInteger g_fh = new AtomicInteger(1001);
}
