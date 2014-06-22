package swift.application.reddit;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.Copyable;

public class Link implements Thing<Link>, Copyable, java.io.Serializable {
    private static final long serialVersionUID = 1L;
    
    String linkId;
    String user;
    String subreddit;
    String title;
    long date;
    boolean selfpost;
    String url;
    String text;

    /** DO NOT USE: Empty constructor needed for Kryo */
    public Link() {
    }

    public Link(String linkId, String user, String subreddit, String title,
            long date, boolean selfpost, String url, String text) {
        this.linkId = linkId;
        this.user = user;
        this.subreddit = subreddit;
        this.title = title;
        this.date = date;
        this.selfpost = selfpost;
        this.url = url;
        this.text = text;
    }
    
    public String getId() {
        return linkId;
    }
    
    public String getPosterUsername() {
        return this.user;
    }
    
    public String getSubreddit() {
        return this.subreddit;
    }
    
    public long getDate() {
        return this.date;
    }
    
    public CRDTIdentifier getVoteCounterIdentifier() {
        return NamingScheme.forLinkVotes(linkId);
    }
    
    public CRDTIdentifier getVoteCounterSetIdentifier() {
        return NamingScheme.forLinksOfSubreddit(subreddit);
    }

    @Override
    public Object copy() {
        Link copyObj = new Link(linkId, user, subreddit, title, date, selfpost, url, text);
        return copyObj;
    }

    @Override
    public String toString() {
        return title;
    }
    
    public int hashCode() {
        return linkId.hashCode();
    }
    
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof Link))
            return false;
        Link other = (Link) obj;
        return this.linkId.equals(other.getId());
    }
    
    @Override
    public int compareTo(Link o) {
        if (this.getDate() != o.getDate()) {
            if (this.getDate() > o.getDate()) {
                return -1;
            } else {
                return 1;
            }
        }
        
        return this.getId().compareTo(o.getId());
    }
}
