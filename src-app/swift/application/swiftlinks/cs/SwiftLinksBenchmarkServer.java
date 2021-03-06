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
package swift.application.swiftlinks.cs;

import static sys.net.api.Networking.Networking;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import swift.application.social.cs.Reply;
import swift.application.social.cs.Request;
import swift.application.social.cs.RequestHandler;
import swift.application.swiftlinks.SwiftLinksApp;
import swift.application.swiftlinks.SwiftLinksBenchmark;
import swift.application.swiftlinks.SwiftLinks;
import sys.ec2.ClosestDomain;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Args;
import sys.utils.IP;

/**
 * Receives operations via RPC
 * to run them at the server side
 */
public class SwiftLinksBenchmarkServer extends SwiftLinksBenchmark {
    public static int SCOUT_PORT = 26667;

    public static void main(String[] args) {
        sys.Sys.init();

        String partitions = Args.valueOf(args, "-partitions", "1/1");
        int site = Integer.valueOf(partitions.split("/")[0]);

        List<String> servers = Args.subList(args, "-servers", "localhost");

        String server = ClosestDomain.closest2Domain(servers, site);

        System.err.println(IP.localHostAddress() + " connecting to: " + server);

        final SwiftLinksApp app = new SwiftLinksApp();
        app.init(new String[] { "-servers", server });

        app.populateWorkloadFromConfig(false); // Populate properties...
        app.getProps().setProperty("swift.computeMetadataStatistics", "false");

        int instance = Args.valueOf(args, "-instance", 0);
        Networking.rpcBind(SCOUT_PORT + instance, TransportProvider.DEFAULT).toService(0, new RequestHandler() {

            @Override
            public void onReceive(final RpcHandle handle, final Request m) {
                String cmdLine = m.getPayload();
                String sessionId = handle.remoteEndpoint().toString();
                SwiftLinks client = getSession(sessionId, app);
                try {
                    app.runCommandLine(client, cmdLine);
                    handle.reply(new Reply("OK"));
                } catch (Exception x) {
                    handle.reply(new Request("ERROR"));
                    x.printStackTrace();
                }
            }
        });

        System.err.println("SwiftSocial Server Ready...");
    }

    static SwiftLinks getSession(String sessionId, SwiftLinksApp app) {
        SwiftLinks res = sessions.get(sessionId);
        if (res == null) {
            res = app.getReddit(sessionId, true);
            sessions.put(sessionId, res);
        }

        return res;
    }

    static Map<String, SwiftLinks> sessions = new ConcurrentHashMap<String, SwiftLinks>();
}
