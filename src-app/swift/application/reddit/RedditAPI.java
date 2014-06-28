package swift.application.reddit;

import java.util.List;

import swift.application.reddit.cprdt.SortedNode;
import swift.application.reddit.crdt.DecoratedNode;
import swift.application.reddit.crdt.VoteDirection;
import swift.crdt.core.SwiftSession;

/*
 * Trying to replicate (partially) the API: http://www.reddit.com/dev/api
 * Without the web related parameters
 */

public interface RedditAPI {
    
    public SwiftSession getSwift();
    
    // Account

    /*
     * http://www.reddit.com/dev/api#POST_api_delete_user
     */
    //public void deleteUser(String username, String password);

    /*
     * http://www.reddit.com/dev/api#POST_api_login
     */
    public boolean login(String username, String password);
    
    public void logout();

    /*
     * http://www.reddit.com/dev/api#GET_api_me.json
     */
    public User me();

    /*
     * http://www.reddit.com/dev/api#POST_api_register
     */
    public User register(String username, String password, String email);

    /*
     * http://www.reddit.com/dev/api#POST_api_update
     */
    public void update(String currentPassword, String newPassword, String newEmail);
    
    // Subreddits
    public void createSubreddit(String name);

    // Links and comments

    /*
     * http://www.reddit.com/dev/api#POST_api_comment
     * @param parentComment Parent comment, null if root comment
     */
    public Comment comment(Link link, SortedNode<Comment> parentComment, long date, String text);

    /*
     * http://www.reddit.com/dev/api#POST_api_del
     */
    public boolean deleteLink(Link link);
    public boolean deleteComment(Link link, SortedNode<Comment> comment);

    /**
     * http://www.reddit.com/dev/api#POST_api_submit
     * 
     * @param kind "link" or "self"
     */
    public Link submit(String kind, String subreddit, String title, long date, String url,
            String text);

    /*
     * http://www.reddit.com/dev/api#POST_api_vote
     */
    public void voteLink(Link link, VoteDirection direction);
    public void voteComment(SortedNode<Comment> comment, VoteDirection direction);
    
    public Vote voteOfLink(Link link);
    public Vote voteOfComment(SortedNode<Comment> comment);

    // Listings

    /*
     */
    public List<Link> links(String subreddit, SortingOrder sort, Link before,
            Link after, int limit);

    /*
     * http://www.reddit.com/dev/api#GET_comments_{article}
     */
    public SortedTree<DecoratedNode<SortedNode<Comment>,Comment>> comments(Link link, SortedNode<Comment> from, int context, SortingOrder sort, int limit);
}
