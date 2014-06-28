package swift.application.reddit.crdt;

public abstract class AbstractNode<T extends AbstractNode<T,V>, V> {
	protected T parent;
	private V value;
	private int depth;
	
	public AbstractNode(T parent, V value) {
		this.parent = parent;
		this.value = value;
		this.depth = calculateDepth();
	}
	
	public V getValue() {
		return value;
	}
	
	public T getParent() {
		return parent;
	}
	
	public boolean isRoot() {
	    return parent == null;
	}
	
	private int calculateDepth() {
		int depth = 0;
		T curParent = this.getParent();
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
		if (!(obj instanceof AbstractNode<?,?>))
			return false;
		T other = (T) obj;
		
		return ((this.getValue() == null && other.getValue() == null) 
				|| (this.getValue() != null && this.getValue().equals(other.getValue())))
			&& ((this.getParent() == null && other.getParent() == null)
				|| (this.getParent() != null && this.getParent().equals(other.getParent())));
	}
}
