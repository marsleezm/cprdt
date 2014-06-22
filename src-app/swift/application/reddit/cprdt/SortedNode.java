package swift.application.reddit.cprdt;

import swift.application.reddit.crdt.Node;

public class SortedNode<V extends Comparable<V>> extends Node<V> implements Comparable<SortedNode<V>> {
    public SortedNode(SortedNode<V> parent, V value) {
        super(parent, value);
    }
    
    public SortedNode<V> getParent() {
        return (SortedNode) parent;
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
