package swift.application.reddit.crdt;

public class DecoratedNode<T extends AbstractNode<T,V>,V> {
	private T node;
	private boolean isTombstone;
	
	public DecoratedNode(T node, boolean isTombstone) {
		this.node = node;
		this.isTombstone = isTombstone;
	}
	
	public boolean isTombstone() {
		return isTombstone;
	}
	
	public T getNode() {
		return node;
	}
}
