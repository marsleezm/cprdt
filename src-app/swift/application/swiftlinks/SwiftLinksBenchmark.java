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
package swift.application.swiftlinks;

import static java.lang.System.exit;
import static sys.Sys.Sys;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * Benchmark of SwiftLinks
 * 
 * @author Iwan Briquemont
 * 
 */
public class SwiftLinksBenchmark extends SwiftLinksApp {

    private static String shepard;
    
    /**
     * Fills the database with preexisting users, links, comments and votes
     * 
     * @param args
     */
    public void initDB(String[] args) {

        final String servers = Args.valueOf(args, "-servers", "localhost");

        Properties properties = Props.parseFile("swiftlinks", System.out, "swiftlinks-test.props");

        System.err.println("Populating db with data...");

        generateInitialDataFromConfig(properties, true);

        Workload.DataInit toInitData[] = { Workload.getUsers(), Workload.getSubreddits(), Workload.getLinks(),
                Workload.getLinkVotes(), Workload.getComments(), Workload.getCommentVotes() };
        int size = 0;
        for (final Workload.DataInit data : toInitData) {
            size += data.size();
        }

        final int fullSize = size;

        final AtomicInteger counter = new AtomicInteger(0);

        int k = 0;

        final SwiftOptions options = new SwiftOptions(servers, DCConstants.SURROGATE_PORT);
        // To avoid growth of the cache and local application of updates
        options.setCacheSize(0);

        final int PARTITION_SIZE = 8000;

        for (final Workload.DataInit data : toInitData) {

            ExecutorService pool = Executors.newFixedThreadPool(8);

            System.err.println("\nSize: " + data.size() + "\n");
            int partitions = data.size() / PARTITION_SIZE + (data.size() % PARTITION_SIZE > 0 ? 1 : 0);

            for (int i = 0; i < partitions; i++) {
                int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
                final List partition = data.getData().subList(lo, Math.min(hi, data.size()));
                pool.execute(new Runnable() {
                    public void run() {
                        initData(options, data, partition, counter, fullSize);
                    }
                });
            }

            Threading.awaitTermination(pool, Integer.MAX_VALUE);

            k++;

            System.err.println("\n" + k + "/" + toInitData.length);
        }
        Threading.sleep(5000);
        System.err.println("\nFinished populating db with data.");
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

        super.populateWorkloadFromConfig(false);

        bufferedOutput.printf(";\n;\targs=%s\n", Arrays.asList(args));
        bufferedOutput.printf(";\tsite=%s\n", site);
        bufferedOutput.printf(";\tnumberOfSites=%s\n", numberOfSites);
        bufferedOutput.printf(";\tthreads=%s\n;\n", concurrentSessions);
        bufferedOutput.printf(";\tnumberOfVirtualSites=%s\n", numberOfVirtualSites);
        bufferedOutput.printf(";\tSurrogate=%s\n", server);
        bufferedOutput.printf(";\tShepard=%s\n", shepard);

        final Collection<AtomicBoolean> runningClients = new ConcurrentLinkedQueue<AtomicBoolean>();
        final Semaphore scoutResources = new Semaphore(0);

        if (!shepard.isEmpty()) {
            Shepard.sheepJoinHerd(shepard, new Runnable() {
                public void run() {
                    System.err.println("Stopping scouts");
                    for (AtomicBoolean running : runningClients) {
                        running.set(false);
                    }
                    try {
                        scoutResources.acquire(runningClients.size());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.err.println("Scouts stopped");
                }
            });
        }

        // Kick off all sessions, throughput is limited by
        // concurrentSessions.
        final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentSessions, Threading.factory("App"));

        System.err.println("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = site * concurrentSessions + i;
            final Workload commands = getWorkloadFromConfig(sessionId, numberOfVirtualSites, true);
            threadPool.execute(new Runnable() {
                public void run() {
                    // Randomize startup to avoid clients running all at the
                    // same time; causes problems akin to DDOS symptoms.
                    Threading.sleep(Sys.rg.nextInt(1000));
                    AtomicBoolean running = new AtomicBoolean(true);
                    runningClients.add(running);
                    runClientSession(Integer.toString(sessionId), commands, false, running);
                    scoutResources.release();
                }
            });
        }

        // report client progress every 1 seconds...
        new PeriodicTask(0.0, 1.0) {
            public void run() {
                System.err.printf("Done: %s\n", Progress.percentage(commandsDone.get(), totalCommands.get()));
            }
        };

        // Wait for all sessions.
        threadPool.shutdown();
        Threading.awaitTermination(threadPool, Integer.MAX_VALUE);

        System.err.println("Session threads completed.");
        System.exit(0);
    }
    
    void test(String[] args) {
        final String servers = Args.valueOf(args, "-servers", "localhost");
        
        super.populateWorkloadFromConfig(false);
        
        SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(new SwiftOptions(servers, DCConstants.SURROGATE_PORT));
        SwiftLinks client = new SwiftLinks(swiftClient, isolationLevel, cachePolicy,
                swiftClient.getSessionId(), true);
        
        String sub = Workload.getSubreddits().getData().get(0);
        
        System.out.println("Links of sub: " + sub);
        
        
        System.out.println(client.links(sub, SortingOrder.NEW, null, null, 10));
    }

    public static void main(String[] args) {
        sys.Sys.init();
        SwiftLinksBenchmark instance = new SwiftLinksBenchmark();

        if (args.length == 0 || args[0].equals("test")) {

            DCSequencerServer.main(new String[] { "-name", "X0" });
            DCServer.main(new String[] { "-servers", "localhost" });
            if (args.length == 0) {
                args = new String[] { "-servers", "localhost", "-threads", "10" };
            }
            long time = System.currentTimeMillis();

            instance.initDB(args);

            long period = System.currentTimeMillis() - time;

            System.out.println("Initialised DB in " + period / 1000 + " sec");
            Threading.sleep(5000);
            
            //instance.test(args);
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

