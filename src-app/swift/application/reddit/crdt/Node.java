package swift.application.reddit.crdt;

public class Node<V> extends AbstractNode<Node<V>,V> {
    public Node(Node<V> parent, V value) {
        super(parent, value);
    }
}
