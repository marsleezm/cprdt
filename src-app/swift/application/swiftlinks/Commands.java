package swift.application.swiftlinks;


/**
 * Commands for the benchmark
 * 
 * @author Iwan Briquemont
 *
 */
public enum Commands {
    LOGIN, // ;username;password
    LOGOUT,
    READ_LINKS, // ;subreddit;order;after Read links of a subreddit (in a certain order) after (index of a link in the workload) is optional
    READ_COMMENTS, // ;link_id;order;random
    POST_LINK, // ;link_id;subreddit;title;date;url;selftext (link_id is optional)
    POST_COMMENT, // ;link_id;parent_comment_index;date;content;random_comment_index
    VOTE_LINK, // ;link_index;vote_direction;random
    VOTE_COMMENT, // ;comment_index;vote_direction;random
    CREATE_SUBREDDIT // ;subreddit
}
