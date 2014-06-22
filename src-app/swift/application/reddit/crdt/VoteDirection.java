package swift.application.reddit.crdt;

public enum VoteDirection {
    UP (1),
    MIDDLE (0),
    DOWN (-1);
    
    public final int direction;
    
    private VoteDirection(int direction) {
        this.direction = direction;
    }
}
