package swift.application.reddit;

import swift.crdt.core.CRDTIdentifier;

public class Comment implements Thing<Comment> {
    String linkId;
    String commentId;
    String username;
    long date;
    String text;
    
    // For Kryo
    public Comment() {
    }
    
    public Comment(String linkId, String commentId, String username, long date, String text) {
        this.linkId = linkId;
        this.commentId = commentId;
        this.username = username;
        this.date = date;
        this.text = text;
    }
    
    public String getId() {
        return commentId;
    }
    
    public String getUsername() {
        return this.username;
    }
    
    public long getDate() {
        return date;
    }
    
    public CRDTIdentifier getVoteCounterIdentifier() {
        return NamingScheme.forCommentVotes(commentId);
    }
    
    public CRDTIdentifier getVoteCounterSetIdentifier() {
        return NamingScheme.forCommentTree(linkId);
    }
    
    public Comment copy() {
        return new Comment(linkId, commentId, username, date, text);
    }
    
    public int hashCode() {
        return commentId.hashCode();
    }
    
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof Comment))
            return false;
        Comment other = (Comment) obj;
        return commentId.equals(other.getId());
    }
    
    @Override
    public int compareTo(Comment o) {
        if (this.getDate() != o.getDate()) {
            if (this.getDate() > o.getDate()) {
                return -1;
            } else {
                return 1;
            }
        }
        
        return this.commentId.compareTo(o.getId());
    }

    public String getLinkId() {
        return this.linkId;
    }
}
