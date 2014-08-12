package swift.application.swiftlinks.cprdt;

import swift.application.swiftlinks.crdt.AbstractNode;

/**
 * 
 * @author Iwan Briquemont
 *
 * @param <V> Type of the value of the node
 */
public class SortedNode<V extends Comparable<V>> extends AbstractNode<SortedNode<V>,V> implements Comparable<SortedNode<V>> {
    protected static SortedNode root = new SortedNode(null, null);
    
    // Kryo
    public SortedNode() {
    }
    
    public SortedNode(SortedNode<V> parent, V value) {
        super(parent, value);
    }
    
    public static SortedNode getRoot() {
        return root;
    }

    @Override
    public int compareTo(SortedNode<V> o) {
        int vCompare = 0;
        if (this.getValue() != null && o.getValue() != null) {
            vCompare = this.getValue().compareTo(o.getValue());
        }
        
        if (vCompare == 0) {
            if (this.isRoot() || o.isRoot()) {
                if (this.getParent() == o.getParent()) {
                    return 0;
                } else {
                    if (this.getParent() == null) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            } else {
                return ((SortedNode)this.getParent()).compareTo(((SortedNode)o.getParent()));
            }
        } else {
            return vCompare;
        }
    }
}
