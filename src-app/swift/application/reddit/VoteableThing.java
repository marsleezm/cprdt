package swift.application.reddit;

public class VoteableThing {
    private int upvotes;
    private int downvotes;
    private long date;
    
    public VoteableThing(int upvotes, int downvotes, long date) {
        this.upvotes = upvotes;
        this.downvotes = downvotes;
        this.date = date;
    }

    public int getUpvotes() {
        return this.upvotes;
    }

    public int getDownvotes() {
        return this.downvotes;
    }
    
    public int getScore() {
        return getUpvotes() - getDownvotes();
    }
    
    public long getDate() {
        return this.date;
    }
    
    // See http://amix.dk/blog/post/19588
    public double getConfidence() {
        return VoteUtils.confidence(getUpvotes(), getDownvotes());
    }
    
    public double getHotness() {
        return VoteUtils.hotness(getDate(), getUpvotes(), getDownvotes());
    }
    
    public double getControversy() {
        return VoteUtils.controversy(getUpvotes(), getDownvotes());
    }
}
