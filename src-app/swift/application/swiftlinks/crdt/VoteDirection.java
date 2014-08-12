package swift.application.swiftlinks.crdt;

/**
 * 
 * @author Iwan Briquemont
 *
 */
public enum VoteDirection {
    UP (1),
    MIDDLE (0),
    DOWN (-1);
    
    public final int direction;
    
    private VoteDirection(int direction) {
        this.direction = direction;
    }
    
    static public VoteDirection valueOf(byte dir) {
        if (dir == 0) {
            return MIDDLE;
        } else {
            if (dir == 1) {
                return UP;
            } else {
                return DOWN;
            }
        }
    }
}
