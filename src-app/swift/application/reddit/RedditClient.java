
package swift.application.reddit;

import static sys.net.api.Networking.Networking;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.SwiftException;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.Sys;
import swift.application.social.Message;

public class RedditClient {
    
    private static String generateClientId() {
        final UUID uuid = UUID.randomUUID();
        final byte[] uuidBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE * 2).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
        return DatatypeConverter.printBase64Binary(uuidBytes);
    }
    
    public static void main(String[] args) {
        String dcName = "localhost";
        
        Sys.init();
        
        SwiftSession clientServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                DCConstants.SURROGATE_PORT));
        
        IsolationLevel isolationLevel = IsolationLevel.SNAPSHOT_ISOLATION;
        //CachePolicy cachePolicy = CachePolicy.MOST_RECENT;
        CachePolicy cachePolicy = CachePolicy.CACHED;
        
        RedditFullReplicas client = new RedditFullReplicas(clientServer, isolationLevel, cachePolicy, generateClientId());
    }
}
