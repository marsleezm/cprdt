package swift.application.reddit;

import swift.crdt.core.CRDTIdentifier;


public class NamingScheme {

   /**
     * Generates a CRDT identifier for the user from the username.
     * 
     * @param username
     * @return CRDT identifier for user
     */
    public static CRDTIdentifier forUser(String username) {
        return new CRDTIdentifier("account", username);
    }
    
    /**
     * Generates a CRDT identifier for the comment from the comment id.
     * 
     * @param id
     * @return CRDT identifier for comment
     */
    public static CRDTIdentifier forComment(String commentid) {
        return new CRDTIdentifier("comment", commentid);
    }
    
    public static CRDTIdentifier forCommentVotes(String commentid) {
        return new CRDTIdentifier("commentvotecounter", commentid);
    }
    
    public static CRDTIdentifier forCommentTree(String linkid) {
        return new CRDTIdentifier("commenttree", linkid);
    }
    
    public static CRDTIdentifier forCommentsOfUser(String username) {
        return new CRDTIdentifier("usercommentset", username);
    }
    
    /**
     * Generates a CRDT identifier for the link from the link id.
     * 
     * @param id
     * @return CRDT identifier for link
     */
    public static CRDTIdentifier forLink(String linkid) {
        return new CRDTIdentifier("link", linkid);
    }
    
    public static CRDTIdentifier forLinkVotes(String linkid) {
        return new CRDTIdentifier("linkvotecounter", linkid);
    }
    
    public static CRDTIdentifier forLinksOfUser(String username) {
        return new CRDTIdentifier("userlinkset", username);
    }
    
    public static CRDTIdentifier forLinksOfSubreddit(String subreddit) {
        return new CRDTIdentifier("subredditlinkset", subreddit);
    }
    
    /**
     * Generates a CRDT identifier for the subreddit from the subreddit name.
     * 
     * @param id
     * @return CRDT identifier for link
     */
    public static CRDTIdentifier forSubreddit(String subreddit) {
        return new CRDTIdentifier("subreddit", subreddit);
    }
    
    public static CRDTIdentifier forSubredditsOfUser(String username) {
        return new CRDTIdentifier("usersubredditset", username);
    }
}
