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
package swift.application.reddit;

import java.io.PrintStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.exceptions.SwiftException;
import swift.proto.MetadataStatsCollectorImpl;
import sys.utils.Args;
import sys.utils.Progress;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Executing SwiftSocial operations, based on data model of WaltSocial prototype
 * [Sovran et al. SOSP 2011].
 * <p>
 * Runs SwiftSocial workload that is generated on the fly.
 */

public class RedditApp {
    protected String server;
    protected IsolationLevel isolationLevel;
    protected CachePolicy cachePolicy;
    protected boolean subscribeUpdates;
    protected boolean asyncCommit;

    protected int thinkTime;
    protected int numUsers;
    protected int userFriends;
    protected int biasedOps;
    protected int randomOps;
    protected int opGroups;

    protected PrintStream bufferedOutput;

    protected AtomicInteger commandsDone = new AtomicInteger(0);
    protected AtomicInteger totalCommands = new AtomicInteger(0);
    private Properties props;

    public void init(String[] args) {
        server = Args.valueOf(args, "-servers", "localhost");
    }

    public void populateWorkloadFromConfig() {

        props = Props.parseFile("reddit", bufferedOutput, "reddit-test.props");
        isolationLevel = IsolationLevel.valueOf(Props.get(props, "swift.isolationLevel"));
        cachePolicy = CachePolicy.valueOf(Props.get(props, "swift.cachePolicy"));
        subscribeUpdates = Props.boolValue(props, "swift.notifications", false);
        asyncCommit = Props.boolValue(props, "swift.asyncCommit", true);

        // TODO: read reddit's data
    }
    
    public Workload getWorkloadFromConfig(int site, int numberOfSites) {
        if (props == null)
            populateWorkloadFromConfig();
        return Workload.doMixed(site, userFriends, biasedOps, randomOps, opGroups, numberOfSites);
    }

    public RedditAPI getReddit(final String sessionId) {
        final SwiftOptions options = new SwiftOptions(server, DCConstants.SURROGATE_PORT, props);
        if (options.hasMetadataStatsCollector()) {
            options.setMetadataStatsCollector(new MetadataStatsCollectorImpl(sessionId, bufferedOutput));
        }
        SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(options);
        RedditAPI redditClient = new RedditPartialReplicas(swiftClient, isolationLevel, cachePolicy,
                sessionId);
        return redditClient;
    }

    void runClientSession(final String sessionId, final Workload commands, boolean loop4Ever) {
        final RedditAPI redditClient = getReddit(sessionId);

        totalCommands.addAndGet(commands.size());
        final long sessionStartTime = System.currentTimeMillis();
        final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);
        bufferedOutput.println(initSessionLog);

        do
            for (String cmdLine : commands) {
                long txnStartTime = System.currentTimeMillis();
                Commands cmd = runCommandLine(redditClient, cmdLine);
                long txnEndTime = System.currentTimeMillis();
                final long txnExecTime = txnEndTime - txnStartTime;
                final String log = String.format("%s,%s,%d,%d", sessionId, cmd, txnExecTime, txnEndTime);
                bufferedOutput.println(log);

                Threading.sleep(thinkTime);
                commandsDone.incrementAndGet();
            }
        while (loop4Ever);

        redditClient.getSwift().stopScout(true);

        final long now = System.currentTimeMillis();
        final long sessionExecTime = now - sessionStartTime;
        bufferedOutput.println(String.format("%s,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
        bufferedOutput.flush();
    }

    public Commands runCommandLine(RedditAPI redditClient, String cmdLine) {
        String[] toks = cmdLine.split(";");
        final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
        switch (cmd) {
        case LOGIN:
            if (toks.length == 3) {
                while (!redditClient.login(toks[1], toks[2]))
                    Threading.sleep(1000);
                break;
            }
        // TODO
        default:
            System.err.println("Can't parse command line :" + cmdLine);
            System.err.println("Exiting...");
            System.exit(1);
        }
        return cmd;
    }
    
    public <V> void initData(SwiftOptions swiftOptions, final Workload.DataInit<V> processor, final List<V> elements, AtomicInteger counter, int total) {
        try {
            SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(swiftOptions);
            RedditPartialReplicas client = new RedditPartialReplicas(swiftClient, isolationLevel, cachePolicy, swiftClient.getSessionId());

            TxnHandle txn = swiftClient.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            int txnSize = 0;
            // Initialize user data
            for (V element : elements) {
                // Divide into smaller transactions.
                if (txnSize >= 100) {
                    txn.commit();
                    txn = swiftClient.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
                    txnSize = 0;
                } else {
                    txnSize++;
                }
                
                processor.init(client, txn, element);
                
                System.out.printf("\rDone: %s", Progress.percentage(counter.incrementAndGet(), total));
            }
            // Commit the last batch
            if (!txn.getStatus().isTerminated()) {
                txn.commit();
            }
            swiftClient.stopScout(true);
        } catch (SwiftException e) {
            e.printStackTrace();
        }
    }
}
