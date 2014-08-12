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

import static java.lang.System.exit;
import static sys.net.api.Networking.Networking;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import swift.application.social.cs.Reply;
import swift.application.social.cs.Request;
import swift.application.swiftlinks.Commands;
import swift.application.swiftlinks.SwiftLinksBenchmark;
import swift.application.swiftlinks.SwiftLinks;
import swift.application.swiftlinks.SwiftLinksAPI;
import swift.application.swiftlinks.Workload;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.utils.Args;
import sys.utils.Threading;

/**
 * Benchmark of SwiftLinks
 * Runs operations remotely using RPC
 */
public class SwiftLinksBenchmarkClient extends SwiftLinksBenchmark {

    Endpoint server;

    public void init(String[] args) {
        int port = SwiftLinksBenchmarkServer.SCOUT_PORT + Args.valueOf(args, "-instance", 0);
        server = Networking.resolve(Args.valueOf(args, "-servers", "localhost"), port);
    }
    
    @Override
    protected void runClientSession(final String sessionId, final Workload commands, boolean loop4Ever, AtomicBoolean running) {
        totalCommands.addAndGet(commands.size());
        final long sessionStartTime = System.currentTimeMillis();
        final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);
        bufferedOutput.println(initSessionLog);

        do
            for (String cmdLine : commands) {
                if (!running.get()) {
                    loop4Ever = false;
                    break;
                }
                long txnStartTime = System.currentTimeMillis();
                Commands cmd = runCommandLine(sessionId, cmdLine);
                long txnEndTime = System.currentTimeMillis();
                final long txnExecTime = txnEndTime - txnStartTime;
                final String log = String.format("%s,%s,%d,%d", sessionId, cmd, txnExecTime, txnEndTime);
                bufferedOutput.println(log);

                Threading.sleep(thinkTime);
                commandsDone.incrementAndGet();
            }
        while (loop4Ever);

        final long now = System.currentTimeMillis();
        final long sessionExecTime = now - sessionStartTime;
        bufferedOutput.println(String.format("%s,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
        bufferedOutput.flush();
    }
    
    public Commands runCommandLine(String sessionId, String cmdLine) {
        String[] toks = cmdLine.split(";");
        final Commands cmd = Commands.valueOf(toks[0].toUpperCase());

        Reply reply = endpointFor(sessionId).request(server, new Request(cmdLine));

        return cmd;
    }

    Map<String, RpcEndpoint> endpoints = new ConcurrentHashMap<String, RpcEndpoint>();

    RpcEndpoint endpointFor(String sessionId) {
        RpcEndpoint res = endpoints.get(sessionId);
        if (res == null)
            endpoints.put(sessionId, res = Networking.rpcConnect().toDefaultService());
        return res;
    }

    public static void main(String[] args) {
        sys.Sys.init();

        SwiftLinksBenchmarkClient client = new SwiftLinksBenchmarkClient();
        if (args.length == 0) {

            DCSequencerServer.main(new String[] { "-name", "X0" });

            args = new String[] { "-servers", "localhost", "-threads", "1" };

            DCServer.main(args);
            SwiftLinksBenchmarkServer.main(args);

            client.init(args);
            client.initDB(args);
            client.doBenchmark(args);
            exit(0);
        }

        if (args[0].equals("init")) {
            client.init(args);
            client.initDB(args);
            exit(0);
        }

        if (args[0].equals("run")) {
            client.init(args);
            client.doBenchmark(args);
            exit(0);
        }
    }
}
