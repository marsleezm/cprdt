package swift.application.swiftlinks.crdt;

public class Node<V> extends AbstractNode<Node<V>,V> {
    // Kryo
    public Node() {
    }
    
    public Node(Node<V> parent, V value) {
        super(parent, value);
    }
}
