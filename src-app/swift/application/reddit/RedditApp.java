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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import swift.application.reddit.cprdt.SortedNode;
import swift.application.reddit.crdt.VoteDirection;
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

    protected int linksPerQuery;
    protected int commentsPerQuery;

    protected int thinkTime;
    protected int numUsers;
    protected int biasedOps;
    protected int randomOps;
    protected int opGroups;
    
    long startingDate = 1404294482851L;

    protected PrintStream bufferedOutput;

    protected AtomicInteger commandsDone = new AtomicInteger(0);
    protected AtomicInteger totalCommands = new AtomicInteger(0);
    private Properties props;

    public void init(String[] args) {
        server = Args.valueOf(args, "-servers", "localhost");
    }

    public void generateInitialDataFromConfig(Properties properties) {
        final int numUsers = Props.intValue(properties, "swiftlinks.numUsers", 50);
        final int numSubreddits = Props.intValue(properties, "swiftlinks.numSubreddits", 5);
        final int numLinks = Props.intValue(properties, "swiftlinks.numLinks", 200);
        final int avgCommentsPerLink = Props.intValue(properties, "swiftlinks.avgCommentsPerLink", 10);
        final int avgVotesPerLink = Props.intValue(properties, "swiftlinks.avgVotesPerLink", 10);
        final int downToUpLinkRatio = Props.intValue(properties, "swiftlinks.downToUpLinkRatio", 75);
        final int avgVotesPerComment = Props.intValue(properties, "swiftlinks.avgVotesPerComment", 5);
        final int downToUpCommentRatio = Props.intValue(properties, "swiftlinks.downToUpCommentRatio", 75);

        Random rg = new Random(6L);

        Workload.generateData(rg, numUsers, numSubreddits, numLinks, avgCommentsPerLink, avgVotesPerLink,
                downToUpLinkRatio, avgVotesPerComment, downToUpCommentRatio);
    }

    public void populateWorkloadFromConfig() {

        props = Props.parseFile("reddit", bufferedOutput, "swiftlinks-test.props");
        isolationLevel = IsolationLevel.valueOf(Props.get(props, "swift.isolationLevel"));
        cachePolicy = CachePolicy.valueOf(Props.get(props, "swift.cachePolicy"));
        subscribeUpdates = Props.boolValue(props, "swift.notifications", false);
        asyncCommit = Props.boolValue(props, "swift.asyncCommit", true);

        linksPerQuery = Props.intValue(props, "swiftlinks.linksPerQuery", 25);
        commentsPerQuery = Props.intValue(props, "swiftlinks.commentsPerQuery", 100);
        
        numUsers = Props.intValue(props, "swiftlinks.numUsers", 500);
        biasedOps = Props.intValue(props, "swiftlinks.biasedOps", 9);
        randomOps = Props.intValue(props, "swiftlinks.randomOps", 1);
        opGroups = Props.intValue(props, "swiftlinks.opGroups", 50);
        thinkTime = Props.intValue(props, "swiftlinks.thinkTime", 1000);

        generateInitialDataFromConfig(props);
    }

    public Workload getWorkloadFromConfig(int site, int numberOfSites, boolean loggedIn) {
        if (props == null)
            populateWorkloadFromConfig();
        return Workload.doMixed(site, loggedIn, biasedOps, randomOps, opGroups, numberOfSites, startingDate);
    }

    public RedditPartialReplicas getReddit(final String sessionId) {
        final SwiftOptions options = new SwiftOptions(server, DCConstants.SURROGATE_PORT, props);
        if (options.hasMetadataStatsCollector()) {
            options.setMetadataStatsCollector(new MetadataStatsCollectorImpl(sessionId, bufferedOutput));
        }
        SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(options);
        RedditPartialReplicas redditClient = new RedditPartialReplicas(swiftClient, isolationLevel, cachePolicy,
                sessionId);
        return redditClient;
    }

    void runClientSession(final String sessionId, final Workload commands, boolean loop4Ever) {
        final RedditPartialReplicas redditClient = getReddit(sessionId);

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

    public Commands runCommandLine(RedditPartialReplicas redditClient, String cmdLine) {
        String[] toks = cmdLine.split(";", -1);
        final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
        switch (cmd) {
        case LOGIN:
            if (toks.length == 3) {
                while (!redditClient.login(toks[1], toks[2]))
                    Threading.sleep(1000);
                break;
            }
        case LOGOUT:
            if (toks.length == 1) {
                redditClient.logout();
                break;
            }
        case READ_LINKS:
            if (toks.length == 4) {
                Link after = null;
                if (!toks[3].isEmpty()) {
                    // Read 'older' links
                    int linkIndex = Integer.parseInt(toks[3]);
                    after = Workload.getLinks().getData().get(linkIndex);
                }

                redditClient.links(toks[1], SortingOrder.valueOf(toks[2]), null, after, linksPerQuery);
                break;
            }
        case READ_COMMENTS:
            if (toks.length == 4) {
                String linkId = toks[1];
                if (linkId.isEmpty()) {
                    List<Link> lastLinks = redditClient.getLastLinks();
                    if (lastLinks.isEmpty()) {
                        System.err.println("No link to read comments from");
                        break;
                    }
                    linkId = lastLinks.get(Integer.valueOf(toks[3]) % lastLinks.size()).getId();
                }
                redditClient
                        .comments(linkId, null, Integer.MAX_VALUE, SortingOrder.valueOf(toks[2]), commentsPerQuery);
                break;
            }
        case POST_LINK:
            if (toks.length == 7) {
                if (toks[1].isEmpty()) {
                    redditClient.submit(null, toks[2], toks[3], Long.parseLong(toks[4]), toks[5], toks[6]);
                } else {
                    redditClient.submit(toks[1], toks[2], toks[3], Long.parseLong(toks[4]), toks[5], toks[6]);
                }
                break;
            }
        case POST_COMMENT:
            if (toks.length == 6) {
                String linkId = toks[1];
                SortedNode<Comment> comment = null;
                if (linkId.isEmpty()) {
                    linkId = redditClient.getLastCommentsLinkId();
                    if (linkId.isEmpty()) {
                        System.err.println("No link to post comment to");
                        break;
                    }
                    List<SortedNode<Comment>> comments = redditClient.getLastComments();
                    
                    if (!toks[5].isEmpty() && !comments.isEmpty()) {
                        int random = Integer.valueOf(toks[5]);
                        comment = comments.get(random % comments.size());
                    }
                } else {
                    if (!toks[2].isEmpty()) {
                        int index = Integer.parseInt(toks[2]);
                        comment = Workload.getComments().getData().get(index);
                    }
                }
                redditClient.comment(linkId, comment, Long.parseLong(toks[3]), toks[4]);
                break;
            }
        case VOTE_LINK:
            if (toks.length == 4) {
                Link link;
                if (toks[1].isEmpty()) {
                    List<Link> lastLinks = redditClient.getLastLinks();
                    if (lastLinks.isEmpty()) {
                        System.err.println("No link to read comments from");
                        break;
                    }
                    link = lastLinks.get(Integer.valueOf(toks[3]) % lastLinks.size());
                } else {
                    int index = Integer.parseInt(toks[1]);
                    link = Workload.getLinks().getData().get(index);
                }
                redditClient.voteLink(link, VoteDirection.valueOf(toks[2]));
                break;
            }
        case VOTE_COMMENT:
            if (toks.length == 5) {
                SortedNode<Comment> comment;
                if (toks[1].isEmpty()) {
                    List<SortedNode<Comment>> comments = redditClient.getCommentList();
                    
                    if (comments.isEmpty()) {
                        System.err.println("No comment to vote");
                        break;
                    }
                    
                    int random = Integer.valueOf(toks[3]);
                    comment = comments.get(random % comments.size());
                } else {
                    int index = Integer.parseInt(toks[1]);
                    comment = Workload.getComments().getData().get(index);
                }
                redditClient.voteComment(comment, VoteDirection.valueOf(toks[2]));
                break;
            }
        case CREATE_SUBREDDIT:
            if (toks.length == 2) {
                redditClient.createSubreddit(toks[1]);
            }
        default:
            System.err.println("Can't parse command line :" + cmdLine);
            for (String tok: toks) {
                System.err.println(tok);
            }
            System.err.println("Exiting...");
            System.exit(1);
        }
        return cmd;
    }

    public <V> void initData(SwiftOptions swiftOptions, final Workload.DataInit<V> processor, final List<V> elements,
            AtomicInteger counter, int total) {
        try {
            SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(swiftOptions);
            RedditPartialReplicas client = new RedditPartialReplicas(swiftClient, isolationLevel, cachePolicy,
                    swiftClient.getSessionId());

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
