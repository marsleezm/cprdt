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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import swift.application.swiftlinks.cprdt.SortedNode;
import swift.application.swiftlinks.crdt.VoteDirection;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

import com.thoughtworks.xstream.core.util.Base64Encoder;

/**
 * Workload generation for SwiftLinks
 * 
 * @author Iwan Briquemont
 *
 */
abstract public class Workload implements Iterable<String>, Iterator<String> {

    private static final int MAX_SITES = 1321;
    private static boolean generated = false;
    private static boolean generatedSmall = false;
    /** List of user names */
    private static List<String> users = new ArrayList<String>();
    /** List of subreddits */
    private static List<String> subreddits = new ArrayList<String>();
    /** List of links */
    private static List<Link> links = new ArrayList<Link>();
    /** Votes of links */
    private static List<Vote<Link, String>> linkVotes = new ArrayList<Vote<Link, String>>();
    /** List of comments */
    private static List<SortedNode<Comment>> comments = new ArrayList<SortedNode<Comment>>();
    /** Votes of comments */
    private static List<Vote<SortedNode<Comment>, String>> commentVotes = new ArrayList<Vote<SortedNode<Comment>, String>>();

    /** Size of workload, i.e. number of operations */
    abstract public int size();

    private static long currentDate = 140370504;

    public static class Vote<K, V> {
        K element;
        V voter;
        VoteDirection vote;

        public Vote(K element, V voter, VoteDirection vote) {
            this.element = element;
            this.voter = voter;
            this.vote = vote;
        }

        public K getElement() {
            return element;
        }

        public V getVoter() {
            return voter;
        }

        public VoteDirection getDirection() {
            return vote;
        }
    }

    public static abstract class DataInit<V> {
        private List<V> data;

        public DataInit(List<V> data) {
            this.data = data;
        }

        public List<V> getData() {
            return data;
        }

        public int size() {
            return data.size();
        }

        public abstract void init(SwiftLinks client, TxnHandle txn, V element) throws WrongTypeException,
                NoSuchObjectException, VersionNotFoundException, NetworkException;
    }

    public static void generateDataForDB(Random rg1, Random rg2, int numUsers, int numSubreddits, int numLinks, int avgCommentsPerLink,
            int avgUpvotesPerLink, int avgDownvotesPerLink, int avgUpvotesPerComment, int avgDownvotesPerComment) {
        if (generated) {
            return;
        }
        generateDataForClient(rg1, numUsers, numSubreddits, numLinks, avgCommentsPerLink, Integer.MAX_VALUE);
        
        generateLinkVotes(rg2, avgUpvotesPerLink, avgDownvotesPerLink);
        generateCommentVotes(rg2, avgUpvotesPerComment, avgDownvotesPerComment);
        generated = true;
    }
    
    private static int getExponentiallyDistributed(Random rg, int avg) {
        double rand = 1. - rg.nextDouble();
        Double result = -Math.log(rand)*avg;
        return Math.min(result.intValue(), 50*avg);
    }
    
    public static void generateDataForClient(Random rg, int numUsers, int numSubreddits, int numLinks, int avgCommentsPerLink, int commentsToKeep) {
        if (generated || generatedSmall) {
            return;
        }
        generateUsers(rg, numUsers);
        generateSubreddits(rg, numSubreddits);
        generateLinks(rg, numLinks);
        generateComments(rg, avgCommentsPerLink, commentsToKeep);
        generatedSmall = true;
    }

    public static DataInit<String> getUsers() {
        return new DataInit<String>(users) {
            @Override
            public void init(SwiftLinks client, TxnHandle txn, String username) throws WrongTypeException,
                    NoSuchObjectException, VersionNotFoundException, NetworkException {
                client.register(txn, username, "passwd", username + "@user.com");
            }
        };
    }

    public static DataInit<String> getSubreddits() {
        return new DataInit<String>(subreddits) {
            @Override
            public void init(SwiftLinks client, TxnHandle txn, String sub) throws WrongTypeException,
                    NoSuchObjectException, VersionNotFoundException, NetworkException {
                client.createSubreddit(txn, sub, "admin");
            }
        };
    }

    public static DataInit<Link> getLinks() {
        return new DataInit<Link>(links) {
            @Override
            public void init(SwiftLinks client, TxnHandle txn, Link link) throws WrongTypeException,
                    NoSuchObjectException, VersionNotFoundException, NetworkException {
                client.submit(txn, link);
            }
        };
    }

    public static DataInit<Vote<Link, String>> getLinkVotes() {
        return new DataInit<Vote<Link, String>>(linkVotes) {
            @Override
            public void init(SwiftLinks client, TxnHandle txn, Vote<Link, String> vote)
                    throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
                client.voteLinkBlind(txn, vote.getElement(), vote.getVoter(), vote.getDirection());
            }
        };
    }

    public static DataInit<SortedNode<Comment>> getComments() {
        return new DataInit<SortedNode<Comment>>(comments) {
            @Override
            public void init(SwiftLinks client, TxnHandle txn, SortedNode<Comment> comment)
                    throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
                client.comment(txn, comment);
            }
        };
    }

    public static DataInit<Vote<SortedNode<Comment>, String>> getCommentVotes() {
        return new DataInit<Vote<SortedNode<Comment>, String>>(commentVotes) {
            @Override
            public void init(SwiftLinks client, TxnHandle txn, Vote<SortedNode<Comment>, String> vote)
                    throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
                client.voteCommentBlind(txn, vote.getElement(), vote.getVoter(), vote.getDirection());
            }
        };
    }

    /**
     * Generates random user names and other (dummy, not semantically used)
     * attributes such as password and email
     */
    private static void generateUsers(Random rg, int numUsers) {
        System.out.println(";Generating users...");
        for (int i = 0; i < numUsers; i++) {
            byte[] tmp = new byte[6];
            rg.nextBytes(tmp);
            Base64Encoder enc = new Base64Encoder();

            String user = enc.encode(tmp);
            users.add(user);
        }
    }

    private static void generateSubreddits(Random rg, int numSubs) {
        System.out.println(";Generating subreddits...");
        for (int i = 0; i < numSubs; i++) {
            byte[] tmp = new byte[6];
            rg.nextBytes(tmp);
            Base64Encoder enc = new Base64Encoder();

            String sub = enc.encode(tmp);
            subreddits.add(sub);
        }
    }

    private static void fillSubreddits(String[] subs) {
        for (String sub : subs) {
            subreddits.add(sub);
        }
    }

    /**
     * Generates random links
     */
    private static void generateLinks(Random rg, int numLinks) {
        System.out.println(";Generating links...");

        for (int i = 0; i < numLinks; i++) {
            String linkId = String.valueOf(i);
            String author = users.get(rg.nextInt(users.size()));
            String subreddit = subreddits.get(rg.nextInt(subreddits.size()));
            long date = currentDate;
            currentDate += rg.nextInt(20000);

            links.add(new Link(linkId, author, subreddit, "Link title", date, false, "http://www.test.com/", ""));
        }
    }

    /**
     * Generates random comments
     */
    private static void generateComments(Random rg, int avgPerLink, int commentsToKeep) {
        System.out.println(";Generating comments...");

        List<SortedNode<Comment>> linkComments = new ArrayList<SortedNode<Comment>>(avgPerLink*2);

        int i = 0;
        for (Link link : links) {
            long date = link.getDate();

            linkComments.clear();

            int numberComments = getExponentiallyDistributed(rg, avgPerLink) + 1;
            for (int j = 0; j < numberComments; j++) {
                date += rg.nextInt(20000);

                String author = users.get(rg.nextInt(users.size()));

                Comment comment = new Comment(link.getId(), String.valueOf(i), author, date, "Comment content");
                SortedNode<Comment> parent = SortedNode.getRoot();
                if (linkComments.size() != 0 && rg.nextBoolean()) {
                    parent = linkComments.get(rg.nextInt(linkComments.size()));
                }

                SortedNode<Comment> node = new SortedNode<Comment>(parent, comment);

                linkComments.add(node);
                if (j < commentsToKeep) {
                    comments.add(node);
                }

                i++;
            }
        }
    }

    /**
     * Generates random link votes
     */
    private static void generateLinkVotes(Random rg, int avgUpvotesPerLink, int avgDownvotesPerLink) {
        System.out.println(";Generating link votes...");
        
        for (Link link : links) {
            generateVotes(rg, link, linkVotes, avgUpvotesPerLink, avgDownvotesPerLink);
        }
    }

    /**
     * Generates random comment votes
     */
    private static void generateCommentVotes(Random rg, int avgUpvotesPerComment, int avgDownvotesPerComment) {
        System.out.println(";Generating comment votes...");

        for (SortedNode<Comment> comment : comments) {
            generateVotes(rg, comment, commentVotes, avgUpvotesPerComment, avgDownvotesPerComment);
        }
    }

    private static <K> void generateVotes(Random rg, K element, List<Vote<K, String>> votes, int avgUpvotes,
            int avgDownvotes) {
        int upvotes = getExponentiallyDistributed(rg, avgUpvotes);
        int downvotes = getExponentiallyDistributed(rg, avgDownvotes);
        for (int i = 0; i < downvotes + upvotes; i++) {
            VoteDirection direction;
            if (i < downvotes) {
                direction = VoteDirection.DOWN;
            } else {
                direction = VoteDirection.UP;
            }
            String voter = String.valueOf(i);

            votes.add(new Vote<K, String>(element, voter, direction));
        }
    }

    /**
     * Represents an abstract command the user performs. Each command has a
     * frequency/probability and needs to be formatted into a command line.
     */
    static abstract class Operation {
        private int frequency;

        Operation freq(int f) {
            this.frequency = f;
            return this;
        }

        abstract String doLine(Random rg, String user, boolean biased, long date, List<String> candidates,
                List<SortingOrder> orderings);

        public String toString() {
            return getClass().getSimpleName();
        }
    }

    static class ReadLinks extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            int index = rg.nextInt(subreddits.size());
            String sub = subreddits.get(index);
            SortingOrder order = orderings.get(rg.nextInt(orderings.size()));
            String after = "";
            if (!biased) {
                // Get an old link
                after = links.get(rg.nextInt(links.size())).getId();
            }
            return String.format("read_links;%s;%s;%s", sub, order.toString(), after);
        }
    }

    static class ReadComments extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            SortingOrder order = orderings.get(rg.nextInt(orderings.size()));
            String linkId = "";
            if (!biased) {
                // Get an old link
                linkId = links.get(rg.nextInt(links.size())).getId();
            }
            return String.format("read_comments;%s;%s;%d", linkId, order.toString(), rg.nextInt(1024));
        }
    }

    /**
     * Posts a link. Target is a randomly chosen subreddit from a list of
     * candidates
     */
    static class PostLink extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            int index = rg.nextInt(subreddits.size());
            String sub = subreddits.get(index);
            return String.format("post_link;;%s;Link title;%d;http://www.test.com/;", sub, date);
        }
    }

    static class PostComment extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            String linkId = "";
            String commentIndex = "";
            if (!biased) {
                int index = rg.nextInt(comments.size());
                commentIndex = String.valueOf(index);
                linkId = comments.get(index).getValue().getLinkId();
            }
            return String.format("post_comment;%s;%s;%d;Comment content;%d", linkId, commentIndex, date,
                    rg.nextInt(1024));
        }
    }

    static class VoteLink extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            String linkIndex = "";
            if (!biased) {
                linkIndex = String.valueOf(rg.nextInt(links.size()));
            }
            VoteDirection direction = (rg.nextBoolean()) ? VoteDirection.UP : VoteDirection.DOWN;
            return String.format("vote_link;%s;%s;%d", linkIndex, direction.toString(), rg.nextInt(1024));
        }
    }

    static class VoteComment extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            String commentIndex = "";
            if (!biased) {
                commentIndex = String.valueOf(rg.nextInt(comments.size()));
            }
            VoteDirection direction = (rg.nextBoolean()) ? VoteDirection.UP : VoteDirection.DOWN;
            return String.format("vote_comment;%s;%s;%d", commentIndex, direction.toString(), rg.nextInt(1024));
        }
    }

    static class CreateSubreddit extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            int index = rg.nextInt(subreddits.size());
            String sub = subreddits.get(index);
            return String.format("create_subreddit;%s", sub);
        }
    }

    /**
     * Login. Signals start of session.
     */
    static class Login extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            return String.format("login;%s;passwd", user);
        }
    }

    /**
     * Logout. Signals end of session.
     */
    static class Logout extends Operation {
        @Override
        public String doLine(Random rg, String user, boolean biased, long date, List<String> subreddits,
                List<SortingOrder> orderings) {
            return String.format("logout");
        }
    }

    /**
     * Defines the set of available operations and their frequency. Frequencies
     * need to add up to 100.
     */
    private static Operation[] readOps = new Operation[] { new ReadLinks().freq(60), new ReadComments().freq(40) };
    private static Operation[] writeOps = new Operation[] { new PostLink().freq(2), new PostComment().freq(10),
            new VoteLink().freq(38), new VoteComment().freq(50) };
    
    // There is a write every readToWriteRatio reads
    int readToWriteRatio = 5;

    private static AtomicInteger doMixedCounter = new AtomicInteger(7);

    /**
     * Generates a workload with a mixture of operations. The workload
     * represents a session for some randomly chosen user. Each session starts
     * with a login and ends with a logout operations for this user.
     * 
     * @param site
     *            from which to chose the user from, randomly chosen if site < 0
     * @param friends_per_user
     *            number of friends per user
     * @param ops_biased
     *            number of ops chosen with a bias to increase data locality
     * @param ops_random
     *            number of randomly chosen ops
     * @param ops_groups
     *            number of operation groups to generate
     * @param number_of_sites
     *            number of user partitions
     * @return workload represented as an iterable collection of operations
     */
    static public Workload doMixed(int site, final boolean isLoggedIn, final int ops_biased, final int ops_random,
            final int ops_groups, int number_of_sites, final long startingDate) {
        // Each workload has its own seed...
        final Random rg = new Random(doMixedCounter.addAndGet(MAX_SITES + site));
        
        final List<SortingOrder> orders = new ArrayList<SortingOrder>();
        orders.add(SortingOrder.NEW);
        orders.add(SortingOrder.HOT);

        // Pick a user at random from this site's user partition (for non
        // read-only workloads)
        site = site < 0 ? rg.nextInt(number_of_sites) : site; // fix site
        int partitionSize = users.size() / number_of_sites;
        final String user;
        if (isLoggedIn) {
            user = users.get(rg.nextInt(partitionSize) + partitionSize * site);
        } else {
            user = "";
        }

        // Generate 100 biased operations, according to their frequency
        final List<Operation> mixRead = new ArrayList<Operation>();
        for (Operation i : readOps)
            for (int j = 0; j < i.frequency; j++)
                mixRead.add(i);

        if (mixRead.size() != 100) {
            System.err.println("Workload generation bug");
            System.exit(0);
        }
        
        final List<Operation> mixWrite = new ArrayList<Operation>();
        if (isLoggedIn) {
            for (Operation i : writeOps)
                for (int j = 0; j < i.frequency; j++)
                    mixWrite.add(i);
        }

        return new Workload() {
            int groupCounter = 0;
            int biasedCounter = 0;
            int randomCounter = 0;
            Iterator<String> it = null;
            long date = startingDate + rg.nextInt(1000);

            // operation groups are generated on the fly
            void refill() {
                ArrayList<String> group = new ArrayList<String>();

                if (groupCounter == 0) {
                    if (isLoggedIn) {
                        // first group starts with login...
                        group.add(new Login().doLine(rg, user, false, 0L, null, null));
                    }
                    // Make sure the client has read some links and comments
                    group.add(new ReadLinks().doLine(rg, user, true, date, subreddits, orders));
                    group.add(new ReadLinks().doLine(rg, user, true, date, subreddits, orders));
                    group.add(new ReadComments().doLine(rg, user, true, date, subreddits, orders));
                    group.add(new ReadComments().doLine(rg, user, true, date, subreddits, orders));
                }

                if (groupCounter < ops_groups) {

                    // append biased operations
                    for (int i = 0; i < ops_biased; i++) {
                        group.add(mixRead.get(rg.nextInt(mixRead.size())).doLine(rg, user, true, date, subreddits, orders));
                        biasedCounter++;
                        if (isLoggedIn && (biasedCounter % readToWriteRatio) == 0) {
                            group.add(mixWrite.get(rg.nextInt(mixWrite.size())).doLine(rg, user, true, date, subreddits, orders));
                        }
                    }
                    
                    for (int i = 0; i < ops_random; i++)
                        group.add(mixRead.get(rg.nextInt(mixRead.size())).doLine(rg, user, false, date, subreddits, orders));
                        randomCounter++;
                        if (isLoggedIn && (randomCounter % readToWriteRatio) == 0) {
                            group.add(mixWrite.get(rg.nextInt(mixWrite.size())).doLine(rg, user, false, date, subreddits, orders));
                        }
                }
                
                date += 1000;

                groupCounter++;

                // last group ends with logout
                if (isLoggedIn && groupCounter == ops_groups) {
                    group.add(new Logout().doLine(rg, user, false, 0L, null, null));
                }

                it = group.iterator();
            }

            @Override
            public boolean hasNext() {
                if (it == null || !it.hasNext())
                    refill();

                return it.hasNext();
            }

            @Override
            public String next() {
                return it.next();
            }

            @Override
            public void remove() {
                throw new RuntimeException("On demand workload generation; remove is not supported...");
            }

            @Override
            public int size() {
                return 2 + ops_groups * (ops_biased + ops_random);
            }

        };
    }

    public Iterator<String> iterator() {
        return this;
    }

    public static void main(String[] args) throws Exception {
        Random rg = new Random(17);
        Workload.generateDataForDB(rg, new Random(13), 20, 3, 200, 10, 20, 70, 5, 70);
        Workload res = doMixed(0, true, 9, 2,
                10, 2, System.currentTimeMillis());
        
        System.out.println("Generated " + res.size() + " operations");
        for (String i : res) System.out.println(i);
    }
}
