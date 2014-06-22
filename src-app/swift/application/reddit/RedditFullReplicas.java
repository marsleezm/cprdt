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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.clocks.IncrementalTimestampGenerator;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.application.reddit.crdt.DecoratedNode;
import swift.application.reddit.crdt.Node;
import swift.application.reddit.crdt.TombstoneTreeCRDT;
import swift.application.reddit.crdt.VoteCounterCRDT;
import swift.application.reddit.crdt.VoteDirection;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
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
import sys.stats.Tally;

// Trying to replicate (partially) the API: http://www.reddit.com/dev/api

public class RedditFullReplicas implements RedditAPI {

    private static Logger logger = Logger.getLogger("swift.social");

    private IncrementalTimestampGenerator idGenerator;

    private User currentUser;

    private SwiftSession server;
    private final IsolationLevel isolationLevel;
    private final CachePolicy cachePolicy;

    private boolean asyncCommit;

    protected String generateId() {
        return idGenerator.generateNew().toString();
    }

    // http://www.reddit.com/r/defaults/comments/1u4oso/list_of_default_subreddits_jan_1_2014/
    private static final String[] defaultSubredditsArray = { "AdviceAnimals", "AskReddit", "AskScience", "Aww", "BestOf",
            "Books", "EarthPorn", "ExplainLikeImFive", "Funny", "Gaming", "Gifs", "IAmA", "Movies", "Music", "News",
            "Pics", "Science", "Sports", "Technology", "Television", "TodayILearned", "Videos", "WorldNews" };
    public static final Set<String> defaultSubreddits = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(defaultSubredditsArray)));
    
    private Set<String> currentSubreddits() {
        if (currentUser == null) {
            return defaultSubreddits;
        } else {
            TxnHandle txn = null;
            Set<String> subreddits = null;
            
            try {
                txn = server.beginTxn(isolationLevel, cachePolicy, true);
                
                AddWinsSetCRDT<String> subredditsSet = (AddWinsSetCRDT<String>) get(txn, NamingScheme.forSubredditsOfUser(currentUser.getUsername()), false, AddWinsSetCRDT.class);
                
                subreddits = subredditsSet.getValue();
                
                commitTxn(txn);
            } catch (SwiftException e) {
                logger.warning(e.getMessage());
            } finally {
                if (txn != null && !txn.getStatus().isTerminated()) {
                    txn.rollback();
                }
            }
            return subreddits;
        }
    }
    
    public RedditFullReplicas(SwiftSession clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy, String clientId) {
        this.server = clientServer;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.asyncCommit = true;
        this.idGenerator = new IncrementalTimestampGenerator(clientId);
    }

    private void commitTxn(final TxnHandle txn) {
        if (asyncCommit) {
            txn.commitAsync(null);
        } else {
            txn.commit();
        }
    }

    public User register(String username, String password, String email) {
        logger.info("Got registration request for " + username);
        // FIXME How do we guarantee unique login names?

        TxnHandle txn = null;
        User newUser = null;
        try {
            txn = server.beginTxn(isolationLevel, CachePolicy.STRICTLY_MOST_RECENT, false);
            newUser = register(txn, username, password, email);
            logger.info("Registered user: " + newUser);
            // Here only synchronous commit, as otherwise the following tests
            // might fail.
            txn.commit();
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }

        return newUser;
    }

    public User register(final TxnHandle txn, String username, String password, String email)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        // FIXME How do we guarantee unique login names?

        LWWRegisterCRDT<User> reg = (LWWRegisterCRDT<User>) txn.get(NamingScheme.forUser(username), true,
                LWWRegisterCRDT.class, null);

        User newUser = new User(username, password, email);
        reg.set((User) newUser.copy());

        // Construct the associated sets with links, comments
        txn.get(newUser.linkList, true, AddWinsSetCRDT.class, null);
        txn.get(newUser.commentList, true, AddWinsSetCRDT.class, null);
        AddWinsSetCRDT<String> subreddits = txn.get(newUser.subredditList, true, AddWinsSetCRDT.class, null);
        for (String sr : defaultSubreddits) {
            subreddits.add(sr);
        }

        return newUser;
    }

    public boolean login(String username, String passwd) {
        logger.info("Got login request from user " + username);

        // Check if user is already logged in
        if (currentUser != null) {
            if (username.equals(currentUser)) {
                logger.info(username + " is already logged in");
                return true;
            } else {
                logger.info("Need to log out user " + currentUser.username + " first!");
                return false;
            }
        }

        TxnHandle txn = null;
        try {
            // Check if user is known at all
            // FIXME Is login possible in offline mode?

            final CachePolicy loginCachePolicy;
            if (isolationLevel == IsolationLevel.SNAPSHOT_ISOLATION && cachePolicy == CachePolicy.CACHED) {
                loginCachePolicy = CachePolicy.MOST_RECENT;
            } else {
                loginCachePolicy = cachePolicy;
            }
            txn = server.beginTxn(isolationLevel, loginCachePolicy, true);
            @SuppressWarnings("unchecked")
            User user = (User) (txn.get(NamingScheme.forUser(username), false, LWWRegisterCRDT.class, null)).getValue();

            // Check password
            // FIXME We actually need an external authentification mechanism, as
            // clients cannot be trusted.
            if (user != null) {
                if (user.password.equals(passwd)) {
                    currentUser = user;
                    logger.info(username + " successfully logged in");
                    commitTxn(txn);
                    return true;
                } else {
                    logger.info("Wrong password for " + username);
                }
            } else {
                logger.severe("User has not been registered " + username);
            }
        } catch (NoSuchObjectException e) {
            logger.severe("User " + username + " is not known");
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }

        return false;
    }

    @Override
    public User me() {
        return currentUser;
    }

    @Override
    public void update(String currentPassword, String newPassword, String newEmail) {
        if (currentUser == null) {
            logger.info("You must be logged in to change your info");
            return;
        }
        // Should be real authentication
        if (!currentUser.password.equals(currentPassword)) {
            logger.info("Wrong password for " + currentUser.getUsername());
        }
        
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            @SuppressWarnings("unchecked")
            LWWRegisterCRDT<User> userReg = (LWWRegisterCRDT<User>) txn.get(NamingScheme.forUser(currentUser.getUsername()), false, LWWRegisterCRDT.class, null);
            
            User newUser = new User(currentUser.getUsername(), newPassword, newEmail);
            
            userReg.set(newUser);
            currentUser = newUser;
        } catch (NoSuchObjectException e) {
            logger.severe("User " + currentUser.getUsername() + " is not known");
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public Link submit(String kind, String subreddit, String title, long date, String url,
            String text) {
        if (currentUser == null) {
            logger.warning("User must be logged in to submit a link");
            return null;
        }
        TxnHandle txn = null;
        Link link = null;
        
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            // Check that the subreddit exists
            try {
                get(txn, NamingScheme.forSubreddit(subreddit), false,
                    LWWRegisterCRDT.class);
            } catch (NoSuchObjectException e) {
                logger.warning("You must post to an existing subreddit");
                txn.rollback();
                return null;
            }
        
            String linkId = generateId();
            if (kind.equals("link")) {
                link = new Link(linkId, currentUser.getUsername(), subreddit, title, date, false, url, null);
            } else {
                link = new Link(linkId, currentUser.getUsername(), subreddit, title, date, false, null, text);
            }
            
            AddWinsSetCRDT<Link> subredditLinks = (AddWinsSetCRDT<Link>) get(txn, NamingScheme.forLinksOfSubreddit(subreddit), true, AddWinsSetCRDT.class);
            
            subredditLinks.add(link);
            
            // Create vote counter
            get(txn, NamingScheme.forLinkVotes(linkId), true, VoteCounterCRDT.class);
            // Vote my own link up
            voteThing(txn, link, VoteDirection.UP);
            
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
                return null;
            }
        }
        return link;
    }
    
    public void voteLink(Link link, VoteDirection direction) {
        vote(link, direction);
    }
    
    public void voteComment(Comment comment, VoteDirection direction) {
        vote(comment, direction);
    }
    
    public void vote(Thing<?> thing, VoteDirection direction) {
        if (currentUser == null) {
            logger.warning("You must be logged in to vote");
            return;
        }
        TxnHandle txn = null;
        
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            
            try {
                // Vote on the link
                voteThing(txn, thing, direction);
            } catch (NoSuchObjectException e) {
                logger.warning("You must vote on an existing link");
                txn.rollback();
                return;
            }
            
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }
    
    private void voteThing(TxnHandle txn, Thing<?> thing, VoteDirection direction) throws SwiftException {
        VoteCounterCRDT<String> voteCounter = (VoteCounterCRDT<String>) get(txn, thing.getVoteCounterIdentifier(), false, VoteCounterCRDT.class);
        voteCounter.vote(currentUser.getUsername(), direction);
    }
    
    public Vote voteOfLink(Link link) {
        return voteOf(link);
    }
    
    public Vote voteOfComment(Comment comment) {
        return voteOf(comment);
    }
    
    public Vote voteOf(Thing<?> thing) {
        TxnHandle txn = null;
        
        Vote vote = null;
        
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);
            
            VoteCounterCRDT<String> voteCounter = (VoteCounterCRDT<String>) get(txn, thing.getVoteCounterIdentifier(), false, VoteCounterCRDT.class);
            VoteDirection myVote = VoteDirection.MIDDLE;
            if (currentUser != null) {
                myVote = voteCounter.getVoteOf(currentUser.getUsername());
            }
            
            vote = new Vote(voteCounter.getUpvotes(), voteCounter.getDownvotes(), myVote);
            
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        
        return vote;
    }
    
    public boolean deleteLink(Link link) {
        if (currentUser == null) {
            logger.warning("User must be logged in to delete a link");
            return false;
        }
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            
            AddWinsSetCRDT<Link> subredditLinks = (AddWinsSetCRDT<Link>) get(txn, NamingScheme.forLinksOfSubreddit(link.getSubreddit()), false, AddWinsSetCRDT.class);
            
            subredditLinks.remove(link);
            
            commitTxn(txn);
            return true;
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return false;
    }
    
    private Node<Comment> getCommentNode(TombstoneTreeCRDT<Comment> commentTree, Comment comment) {
        Node<Comment> commentNode = null;
        for (Node<Comment> n: commentTree.getNodesByValue(comment)) {
            commentNode = n;
        }
        return commentNode;
    }
    
    public Comment comment(Link link, Comment parentComment, long date, String text) {
        if (currentUser == null) {
            logger.warning("User must be logged in to submit a comment");
            return null;
        }
        TxnHandle txn = null;
        Comment comment = null;
        
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            
            TombstoneTreeCRDT<Comment> comments = (TombstoneTreeCRDT<Comment>) get(txn, NamingScheme.forCommentTree(link.getId()), false, TombstoneTreeCRDT.class);
            
            Node<Comment> parentCommentNode;
            
            if (parentComment == null) {
                parentCommentNode = comments.getRoot();
            } else  {
                parentCommentNode = getCommentNode(comments, parentComment);
                if (parentCommentNode == null) {
                    logger.warning("You must reply to an existing comment");
                    txn.rollback();
                    return null;
                }
            }
            
            String commentId = generateId();
            
            comment = new Comment(link.getId(), commentId, currentUser.getUsername(), date, text);
            
            comments.add(parentCommentNode, comment);
            
            // Vote my own comment up
            voteThing(txn, comment, VoteDirection.UP);
            
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
                return null;
            }
        }
        return comment;
    }
    
    public boolean deleteComment(Link link, Comment comment) {
        if (currentUser == null) {
            logger.warning("User must be logged in to delete a comment");
            return false;
        }
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            
            TombstoneTreeCRDT<Comment> linkComments = (TombstoneTreeCRDT<Comment>) get(txn, NamingScheme.forCommentTree(comment.commentId), true, TombstoneTreeCRDT.class);
            
            for (Node<Comment> nodeComment: linkComments.getNodesByValue(comment)) {
                linkComments.remove(nodeComment);
            }
            
            commitTxn(txn);
            return true;
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return false;
    }
    
    /*
     * Only one subreddit at a time for the moment
     * TODO: support multiple subreddits
     */
    public List<Link> links(String subreddit, SortingOrder sort, Link before,
            Link after, int limit) {
        if (before != null && after != null) {
            logger.severe("Cannot use both before and after");
            return null;
        }
        TxnHandle txn = null;
        List<Link> links = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);
            
            // TODO: sort the CRDT directly
            
            AddWinsSetCRDT<Link> subredditLinks = (AddWinsSetCRDT<Link>) get(txn, NamingScheme.forLinksOfSubreddit(subreddit), false, AddWinsSetCRDT.class);
            if ((before != null && !subredditLinks.lookup(before)) || (after != null && !subredditLinks.lookup(after))) {
                logger.warning("Provided before or after link not found");
                txn.rollback();
                return null;
            }
            
            Map<Link,VoteableThing> voteFinder = new HashMap<Link,VoteableThing> ();
            
            for (Link link: subredditLinks.getValue()) {
                VoteCounterCRDT<String> linkVotes = (VoteCounterCRDT<String>) get(txn, NamingScheme.forLinkVotes(link.getId()), false, VoteCounterCRDT.class);
                VoteableThing thing = new VoteableThing(linkVotes.getUpvotes(), linkVotes.getDownvotes(), link.getDate());
                voteFinder.put(link, thing);
            }
            
            TreeSet<Link> sortedLinks = new TreeSet<Link>(sort.getComparator(voteFinder));
            sortedLinks.addAll(subredditLinks.getValue());
            
            SortedSet<Link> allLinks = sortedLinks;
            
            if (before != null) {
                allLinks = sortedLinks.headSet(before, false);
            }
            if (after != null) {
                allLinks = sortedLinks.tailSet(after, false);
            }
            
            // Limit the number of links returned
            links = new LinkedList<Link>();
            int i = 0;
            for (Link link: allLinks) {
                if (i == limit) {
                    break;
                }
                links.add(link);
            }
            
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            links = null;
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return links;
    }

    @Override
    public SortedTree<DecoratedNode<Comment>> comments(Link link, int depth, int limit, SortingOrder sort) {
        // TODO
        return null;
    }
    
    Map<CRDTIdentifier, CRDT<?>> bulkRes = new HashMap<CRDTIdentifier, CRDT<?>>();

    void bulkGet(TxnHandle txn, CRDTIdentifier... ids) {
        txn.bulkGet(ids);
    }

    void bulkGet(TxnHandle txn, Set<CRDTIdentifier> ids) {
        txn.bulkGet(ids.toArray(new CRDTIdentifier[ids.size()]));
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> V get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        V res = (V) bulkRes.remove(id);
        if (res == null)
            res = txn.get(id, create, classOfV, updatesListener);
        return res;
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>, T extends CRDT<V>> T get(TxnHandle txn, CRDTIdentifier id, boolean create,
            Class<V> classOfT) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {

        T res = (T) bulkRes.remove(id);
        if (res == null)
            res = (T) txn.get(id, create, classOfT);

        return res;
    }
}
