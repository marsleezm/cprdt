package swift.application.reddit.crdt;

public class Node<V> {
	protected Node<V> parent;
	private V value;
	private int depth;
	
	public Node(Node<V> parent, V value) {
		this.parent = parent;
		this.value = value;
		this.depth = calculateDepth();
	}
	
	public V getValue() {
		return value;
	}
	
	public Node<V> getParent() {
		return parent;
	}
	
	public boolean isRoot() {
	    return parent == null;
	}
	
	private int calculateDepth() {
		int depth = 0;
		Node<V> curParent = this.getParent();
		while (curParent != null) {
			depth += 1;
			curParent = curParent.getParent();
		}
		return depth;
	}
	
	/**
	 * Depth of node
	 * Root has depth 0
	 */
	public int depth() {
		return depth;
	}
	
	public int hashCode() {
		int result = 17;
		if (parent != null) {
			result = 37 * result + parent.hashCode();
		}
		if (value != null) {
			result = 37 * result + value.hashCode();
		}
		return result;
	}
	
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof Node<?>))
			return false;
		Node<V> other = (Node<V>) obj;
		
		return ((this.getValue() == null && other.getValue() == null) 
				|| (this.getValue() != null && this.getValue().equals(other.getValue())))
			&& ((this.getParent() == null && other.getParent() == null)
				|| (this.getParent() != null && this.getParent().equals(other.getParent())));
	}
}
