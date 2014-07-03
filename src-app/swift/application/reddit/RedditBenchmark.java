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

import static java.lang.System.exit;
import static sys.Sys.Sys;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.ec2.ClosestDomain;
import sys.herd.Shepard;
import sys.scheduler.PeriodicTask;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Progress;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class RedditBenchmark extends RedditApp {

    private static String shepard;

    public void initDB(String[] args) {

        final String servers = Args.valueOf(args, "-servers", "localhost");

        Properties properties = Props.parseFile("swiftlinks", System.out, "swiftlinks-test.props");

        System.err.println("Populating db with users...");

        generateInitialDataFromConfig(properties);

        Workload.DataInit toInitData[] = { Workload.getUsers(), Workload.getSubreddits(), Workload.getLinks(),
                Workload.getLinkVotes(), Workload.getComments(), Workload.getCommentVotes() };
        int size = 0;
        for (final Workload.DataInit data : toInitData) {
            size += data.size();
        }

        final int fullSize = size;

        final AtomicInteger counter = new AtomicInteger(0);

        int k = 0;

        for (final Workload.DataInit data : toInitData) {
            final int PARTITION_SIZE = 1000;
            int partitions = data.size() / PARTITION_SIZE + (data.size() % PARTITION_SIZE > 0 ? 1 : 0);

            ExecutorService pool = Executors.newFixedThreadPool(8);
            for (int i = 0; i < partitions; i++) {
                int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
                final List partition = data.getData().subList(lo, Math.min(hi, data.size()));
                pool.execute(new Runnable() {
                    public void run() {
                        SwiftOptions options = new SwiftOptions(servers, DCConstants.SURROGATE_PORT);
                        initData(options, data, partition, counter, fullSize);
                    }
                });
            }

            Threading.awaitTermination(pool, Integer.MAX_VALUE);

            k++;

            System.out.println(k + "/" + toInitData.length);
        }
        Threading.sleep(5000);
        System.out.println("\nFinished populating db with users.");
    }

    public void doBenchmark(String[] args) {
        // IO.redirect("stdout.txt", "stderr.txt");

        System.err.println(IP.localHostname() + "/ starting...");

        int concurrentSessions = Args.valueOf(args, "-threads", 1);
        String partitions = Args.valueOf(args, "-partition", "0/1");
        int site = Integer.valueOf(partitions.split("/")[0]);
        int numberOfSites = Integer.valueOf(partitions.split("/")[1]);
        // ASSUMPTION: concurrentSessions is the same at all sites
        int numberOfVirtualSites = numberOfSites * concurrentSessions;

        // 1/userFraction sessions are logged in
        int userFraction = Args.valueOf(args, "-userfraction", 2);

        List<String> candidates = Args.subList(args, "-servers");
        server = ClosestDomain.closest2Domain(candidates, site);
        shepard = Args.valueOf(args, "-shepard", "");

        System.err.println(IP.localHostAddress() + " connecting to: " + server);

        bufferedOutput = new PrintStream(System.out, false);

        super.populateWorkloadFromConfig();

        System.err.println(Workload.getSubreddits().getData());

        bufferedOutput.printf(";\n;\targs=%s\n", Arrays.asList(args));
        bufferedOutput.printf(";\tsite=%s\n", site);
        bufferedOutput.printf(";\tnumberOfSites=%s\n", numberOfSites);
        bufferedOutput.printf(";\tthreads=%s\n;\n", concurrentSessions);
        bufferedOutput.printf(";\tnumberOfVirtualSites=%s\n", numberOfVirtualSites);
        bufferedOutput.printf(";\tSurrogate=%s\n", server);
        bufferedOutput.printf(";\tShepard=%s\n", shepard);

        if (!shepard.isEmpty())
            Shepard.sheepJoinHerd(shepard);

        // Kick off all sessions, throughput is limited by
        // concurrentSessions.
        final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentSessions, Threading.factory("App"));

        System.err.println("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = site * concurrentSessions + i;
            final Workload commands = getWorkloadFromConfig(sessionId, numberOfVirtualSites, (i % userFraction) == 0);
            threadPool.execute(new Runnable() {
                public void run() {
                    // Randomize startup to avoid clients running all at the
                    // same time; causes problems akin to DDOS symptoms.
                    Threading.sleep(Sys.rg.nextInt(1000));
                    RedditBenchmark.super.runClientSession(Integer.toString(sessionId), commands, false);
                }
            });
        }

        // report client progress every 1 seconds...
        new PeriodicTask(0.0, 1.0) {
            public void run() {
                System.err.printf("Done: %s", Progress.percentage(commandsDone.get(), totalCommands.get()));
            }
        };

        // Wait for all sessions.
        threadPool.shutdown();
        Threading.awaitTermination(threadPool, Integer.MAX_VALUE);

        System.err.println("Session threads completed.");
        System.exit(0);
    }

    public void test() {
        SwiftSession clientServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions("localhost",
                DCConstants.SURROGATE_PORT));
        RedditPartialReplicas client = new RedditPartialReplicas(clientServer, IsolationLevel.SNAPSHOT_ISOLATION,
                CachePolicy.CACHED, clientServer.getSessionId(), true);
        System.out.println(client.links(Workload.getSubreddits().getData().get(0), SortingOrder.HOT, null, null, 100));
    }

    public static void main(String[] args) {
        sys.Sys.init();

        RedditBenchmark instance = new RedditBenchmark();
        if (args.length == 0) {

            DCSequencerServer.main(new String[] { "-name", "X0" });
            DCServer.main(new String[] { "-servers", "localhost" });

            args = new String[] { "-servers", "localhost", "-threads", "10" };

            instance.initDB(args);
            //instance.test();
            instance.doBenchmark(args);
            exit(0);
        }

        if (args[0].equals("init")) {
            instance.initDB(args);
            exit(0);
        }
        if (args[0].equals("run")) {
            instance.doBenchmark(args);
            exit(0);
        }
    }
}

// protected static void exitWithUsage() {
// System.err.println("Usage 1: init <number_of_users>");
// System.err.println("Usage 2: run <surrogate addr> <concurrent_sessions>");
// System.exit(1);
// }

