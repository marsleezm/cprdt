package swift.application.swiftlinks;

import java.util.List;

import swift.application.swiftlinks.cprdt.SortedNode;
import swift.application.swiftlinks.crdt.DecoratedNode;
import swift.application.swiftlinks.crdt.VoteDirection;
import swift.crdt.core.SwiftSession;

/**
 * Trying to replicate (partially) the API: http://www.reddit.com/dev/api
 * Without the web related parameters
 * 
 * @author Iwan Briquemont
 */
public interface SwiftLinksAPI {

    public SwiftSession getSwift();

    // Account

    /**
     * http://www.reddit.com/dev/api#POST_api_delete_user
     */
    // public void deleteUser(String username, String password);

    /**
     * http://www.reddit.com/dev/api#POST_api_login
     */
    public boolean login(String username, String password);

    public void logout();

    /**
     * http://www.reddit.com/dev/api#GET_api_me.json
     */
    public User me();

    /**
     * http://www.reddit.com/dev/api#POST_api_register
     */
    public User register(String username, String password, String email);

    /**
     * http://www.reddit.com/dev/api#POST_api_update
     */
    public void update(String currentPassword, String newPassword, String newEmail);

    // Subreddits/forums
    public void createSubreddit(String name);

    // Links and comments

    /**
     * http://www.reddit.com/dev/api#POST_api_comment
     * 
     * @param parentComment Parent comment, null if root comment
     */
    public SortedNode<Comment> comment(String linkId, SortedNode<Comment> parentComment, long date, String text);

    /**
     * http://www.reddit.com/dev/api#POST_api_del
     */
    public boolean deleteLink(Link link);

    public boolean deleteComment(Link link, SortedNode<Comment> comment);

    /**
     * http://www.reddit.com/dev/api#POST_api_submit
     * 
     * @param linkId
     *            is optional
     */
    public Link submit(String linkId, String subreddit, String title, long date, String url, String text);

    /*
     * http://www.reddit.com/dev/api#POST_api_vote
     */
    public void voteLink(Link link, VoteDirection direction);

    public void voteComment(SortedNode<Comment> comment, VoteDirection direction);

    public Vote voteOfLink(Link link);

    public Vote voteOfComment(SortedNode<Comment> comment);

    // Listings

    /**
     * List of links
     * before and after cannot both be set at the same time
     */
    public List<Link> links(String subreddit, SortingOrder sort, Link before, Link after, int limit);

    /**
     * Tree of comments
     * 
     * http://www.reddit.com/dev/api#GET_comments_{article}
     */
    public List<SortedNode<Comment>> comments(String linkId, SortedNode<Comment> from,
            int context, SortingOrder sort, int limit);
}
