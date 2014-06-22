package swift.application.reddit;

import swift.application.reddit.crdt.VoteDirection;

public class Vote {
    int upvotes;
    int downvotes;
    VoteDirection direction;
    
    public Vote(int upvotes, int downvotes, VoteDirection direction) {
        this.upvotes = upvotes;
        this.downvotes = downvotes;
        this.direction = direction;
    }
    
    public int getUpvotes() {
        return upvotes;
    }
    
    public int getDownvotes() {
        return downvotes;
    }
    
    public int getScore() {
        return upvotes - downvotes;
    }
    
    public VoteDirection myVote() {
        return direction;
    }
}
