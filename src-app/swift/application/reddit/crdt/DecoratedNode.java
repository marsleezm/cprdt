package swift.application.reddit.crdt;

public class DecoratedNode<V> {
	private Node<V> node;
	private boolean isTombstone;
	
	public DecoratedNode(Node<V> node, boolean isTombstone) {
		this.node = node;
		this.isTombstone = isTombstone;
	}
	
	public boolean isTombstone() {
		return isTombstone;
	}
	
	public Node<V> getNode() {
		return node;
	}
}
