
package swift.application.reddit;

import static sys.net.api.Networking.Networking;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.SwiftException;
import sys.Sys;

public class RedditServer {
    public static void main(String[] args) {
        String dcName = "localhost";
        DCSequencerServer.main(new String[] { "-name", dcName });
        
        DCServer.main(new String[] { dcName });
        
        //Sys.init();
    }
}
