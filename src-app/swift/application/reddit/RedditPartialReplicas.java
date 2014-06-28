package swift.application.reddit;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.logging.Logger;

import swift.clocks.IncrementalTimestampGenerator;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.application.reddit.cprdt.IndexedVoteableSetCPRDT;
import swift.application.reddit.cprdt.SortedNode;
import swift.application.reddit.crdt.DecoratedNode;
import swift.application.reddit.cprdt.VoteableTreeCPRDT;
import swift.application.reddit.crdt.VoteCounter;
import swift.application.reddit.crdt.VoteDirection;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.exceptions.SwiftException;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

// Trying to replicate (partially) the API: http://www.reddit.com/dev/api

public class RedditPartialReplicas implements RedditAPI {

    private static Logger logger = Logger.getLogger("swift.social");

    private IncrementalTimestampGenerator idGenerator;

    private User currentUser;

    private SwiftSession server;
    private final IsolationLevel isolationLevel;
    private final CachePolicy cachePolicy;

    private boolean asyncCommit;
    
    private boolean lazy;

    protected String generateId() {
        return idGenerator.generateNew().toString();
    }

    public SwiftSession getSwift() {
        return server;
    }

    // http://www.reddit.com/r/defaults/comments/1u4oso/list_of_default_subreddits_jan_1_2014/
    private static final String[] defaultSubredditsArray = { "AdviceAnimals", "AskReddit", "AskScience", "Aww",
            "BestOf", "Books", "EarthPorn", "ExplainLikeImFive", "Funny", "Gaming", "Gifs", "IAmA", "Movies", "Music",
            "News", "Pics", "Science", "Sports", "Technology", "Television", "TodayILearned", "Videos", "WorldNews" };
    public static final Set<String> defaultSubreddits = Collections.unmodifiableSet(new HashSet<String>(Arrays
            .asList(defaultSubredditsArray)));

    private Set<String> currentSubreddits() {
        if (currentUser == null) {
            return defaultSubreddits;
        } else {
            TxnHandle txn = null;
            Set<String> subreddits = null;

            try {
                txn = server.beginTxn(isolationLevel, cachePolicy, true);

                AddWinsSetCRDT<String> subredditsSet = (AddWinsSetCRDT<String>) get(txn,
                        NamingScheme.forSubredditsOfUser(currentUser.getUsername()), false, AddWinsSetCRDT.class);

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

    public RedditPartialReplicas(SwiftSession clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            String clientId) {
        this.server = clientServer;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.asyncCommit = false;
        this.idGenerator = new IncrementalTimestampGenerator(clientId);
        this.lazy = true;
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
        txn.get(newUser.linkList, true, IndexedVoteableSetCPRDT.class, null);
        txn.get(newUser.commentList, true, AddWinsSetCRDT.class, null);
        AddWinsSetCRDT<String> subreddits = txn.get(newUser.subredditList, true, AddWinsSetCRDT.class, true);
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

    public void logout() {
        currentUser = null;
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
            LWWRegisterCRDT<User> userReg = (LWWRegisterCRDT<User>) txn.get(
                    NamingScheme.forUser(currentUser.getUsername()), false, LWWRegisterCRDT.class, null);

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

    public void createSubreddit(String name) {
        if (currentUser == null) {
            logger.warning("User must be logged in to create a subreddit");
            return;
        }
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            
            createSubreddit(txn, name, currentUser.getUsername());

            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }
    
    void createSubreddit(TxnHandle txn, String name, String author) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        Subreddit sub = new Subreddit(name, author);

        LWWRegisterCRDT<Subreddit> subredditReg = (LWWRegisterCRDT<Subreddit>) get(txn,
                NamingScheme.forSubreddit(name), true, LWWRegisterCRDT.class);
        subredditReg.set(sub);
        /*
        AddWinsSetCRDT<Subreddit> subredditSet = (AddWinsSetCRDT<Subreddit>) get(txn,
                NamingScheme.forSubredditSet(), true, LWWRegisterCRDT.class, lazy);
        subredditSet.add(sub);*/
    }

    public Link submit(String kind, String subreddit, String title, long date, String url, String text) {
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
                get(txn, NamingScheme.forSubreddit(subreddit), false, LWWRegisterCRDT.class);
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
            
            submit(txn, link);
            
            commitTxn(txn);
        } catch (SwiftException e) {
            e.printStackTrace();
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
                return null;
            }
        }
        return link;
    }
    
    void submit(TxnHandle txn, Link link) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        IndexedVoteableSetCPRDT<Link, String> subredditLinks = (IndexedVoteableSetCPRDT<Link, String>) get(txn,
                NamingScheme.forLinksOfSubreddit(link.getSubreddit()), true, IndexedVoteableSetCPRDT.class,
                lazy);

        subredditLinks.add(link);

        // Vote my own link up
        voteLink(txn, link, link.getPosterUsername(), VoteDirection.UP);
        
        // Create the comment tree
        get(txn, NamingScheme.forCommentTree(link.getId()), true, VoteableTreeCPRDT.class,
                lazy);
    }

    public void voteLink(Link link, VoteDirection direction) {
        if (currentUser == null) {
            logger.warning("You must be logged in to vote");
            return;
        }
        TxnHandle txn = null;

        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);

            try {
                // Vote on the link
                voteLink(txn, link, currentUser.getUsername(), direction);
            } catch (NoSuchObjectException e) {
                logger.warning("You must vote on an existing link");
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

    void voteLink(TxnHandle txn, Link link, String user, VoteDirection direction) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        IndexedVoteableSetCPRDT<Link, String> voteCounter = (IndexedVoteableSetCPRDT<Link, String>) get(txn,
                NamingScheme.forLinksOfSubreddit(link.getSubreddit()), false, IndexedVoteableSetCPRDT.class,
                lazy);
        voteCounter.vote(link, user, direction);
    }

    public Vote voteOfLink(Link link) {
        TxnHandle txn = null;

        Vote vote = null;

        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);
            
            IndexedVoteableSetCPRDT<Link, String> voteCounterSet = (IndexedVoteableSetCPRDT<Link, String>) get(txn,
                    link.getVoteCounterSetIdentifier(), false, IndexedVoteableSetCPRDT.class,
                    lazy);
            VoteCounter<String> votes = voteCounterSet.voteCounterOf(link);
            VoteDirection myVote = VoteDirection.MIDDLE;
            if (currentUser != null) {
                myVote = votes.getVoteOf(currentUser.getUsername());
            }

            vote = new Vote(votes.getUpvotes(), votes.getDownvotes(), myVote);

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
        if (!currentUser.getUsername().equals(link.getPosterUsername())) {
            logger.warning("You can only remove your own links");
            return false;
        }
        
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);

            IndexedVoteableSetCPRDT<Link, String> subredditLinks = (IndexedVoteableSetCPRDT<Link, String>) get(txn,
                    NamingScheme.forLinksOfSubreddit(link.getSubreddit()), false, IndexedVoteableSetCPRDT.class,
                    lazy);

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

    private SortedNode<Comment> getCommentNode(VoteableTreeCPRDT<Comment,String> commentTree, Comment comment) {
        SortedNode<Comment> commentNode = null;
        for (SortedNode<Comment> n : commentTree.getNodesByValue(comment)) {
            commentNode = n;
        }
        return commentNode;
    }

    public Comment comment(Link link, SortedNode<Comment> parentComment, long date, String text) {
        if (currentUser == null) {
            logger.warning("User must be logged in to submit a comment");
            return null;
        }
        TxnHandle txn = null;
        Comment comment = null;

        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            
            if (parentComment == null) {
                parentComment = SortedNode.getRoot();
            }

            String commentId = generateId();

            comment = new Comment(link.getId(), commentId, currentUser.getUsername(), date, text);
            
            SortedNode<Comment> commentNode = new SortedNode<Comment>(parentComment, comment);
            
            comment(txn, commentNode);

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
    
    void comment(TxnHandle txn, SortedNode<Comment> commentNode) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        VoteableTreeCPRDT<Comment,String> comments = (VoteableTreeCPRDT<Comment, String>) get(txn,
                NamingScheme.forCommentTree(commentNode.getValue().getLinkId()), false, VoteableTreeCPRDT.class, lazy);

        comments.add(commentNode);

        // Vote my own comment up
        comments.vote(commentNode, commentNode.getValue().getUsername(), VoteDirection.UP);
    }

    @Override
    public Vote voteOfComment(SortedNode<Comment> comment) {
        TxnHandle txn = null;

        Vote vote = null;

        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);

            vote = voteOfComment(txn, (currentUser != null) ? currentUser.getUsername() : null, comment);

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
    
    Vote voteOfComment(TxnHandle txn, String myUsername, SortedNode<Comment> comment) throws VersionNotFoundException, NetworkException, WrongTypeException, NoSuchObjectException {
        VoteableTreeCPRDT<Comment,String> comments = (VoteableTreeCPRDT<Comment, String>) get(txn,
                NamingScheme.forCommentTree(comment.getValue().getLinkId()), false, VoteableTreeCPRDT.class, lazy);
        VoteCounter<String> votes = comments.voteCounterOf(comment);
        VoteDirection myVote = VoteDirection.MIDDLE;
        if (myUsername != null) {
            myVote = votes.getVoteOf(myUsername);
        }

        return new Vote(votes.getUpvotes(), votes.getDownvotes(), myVote);
    }

    @Override
    public void voteComment(SortedNode<Comment> comment, VoteDirection direction) {
        if (currentUser == null) {
            logger.warning("User must be logged in to vote a comment");
            return;
        }
        
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            
            voteComment(txn, comment, currentUser.getUsername(), direction);

            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }
    
    void voteComment(TxnHandle txn, SortedNode<Comment> comment, String username, VoteDirection direction) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        VoteableTreeCPRDT<Comment,String> linkComments = (VoteableTreeCPRDT<Comment,String>) get(txn,
                NamingScheme.forCommentTree(comment.getValue().linkId), true, VoteableTreeCPRDT.class, true);
        
        linkComments.vote(comment, username, direction);
    }

    public boolean deleteComment(Link link, SortedNode<Comment> comment) {
        if (currentUser == null) {
            logger.warning("User must be logged in to delete a comment");
            return false;
        }
        if (!currentUser.getUsername().equals(comment.getValue().getUsername())) {
            logger.warning("You can only remove your own comments");
            return false;
        }
        
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);

            VoteableTreeCPRDT<Comment,String> linkComments = (VoteableTreeCPRDT<Comment,String>) get(txn,
                    NamingScheme.forCommentTree(comment.getValue().linkId), true, VoteableTreeCPRDT.class, lazy);
            
            linkComments.remove(comment);

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
    public List<Link> links(String subreddit, SortingOrder sort, Link before, Link after, int limit) {
        if (before != null && after != null) {
            logger.severe("Cannot use both before and after");
            return null;
        }
        TxnHandle txn = null;
        List<Link> links = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);

            IndexedVoteableSetCPRDT<Link, String> subredditLinks = (IndexedVoteableSetCPRDT<Link, String>) get(txn,
                    NamingScheme.forLinksOfSubreddit(subreddit), false, IndexedVoteableSetCPRDT.class,
                    lazy);

            links = subredditLinks.find(sort, after, before, limit);

            commitTxn(txn);
        } catch (NoSuchObjectException e) {
            links = Collections.emptyList();
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

    public SortedTree<DecoratedNode<SortedNode<Comment>,Comment>> comments(Link link, SortedNode<Comment> from, int context, SortingOrder sort, int limit) {
        
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);

            VoteableTreeCPRDT<Comment,String> linkComments = (VoteableTreeCPRDT<Comment,String>) get(txn,
                    NamingScheme.forCommentTree(link.getId()), false, VoteableTreeCPRDT.class, lazy);
            
            List<SortedNode<Comment>> comments = linkComments.sortedSubtree(from, context, sort, limit);
            // TODO transform into tree (or do it directly in the CRDT)

            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
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
    <V extends CRDT<V>, T extends CRDT<V>> T get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

        T res = (T) bulkRes.remove(id);
        if (res == null)
            res = (T) txn.get(id, create, classOfT);

        return res;
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>, T extends CRDT<V>> T get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfT,
            boolean lazy) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        return (T) txn.get(id, create, classOfT, lazy);
    }
}
