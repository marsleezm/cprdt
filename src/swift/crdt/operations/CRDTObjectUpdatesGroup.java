package swift.crdt.operations;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;

/**
 * Representation of an atomic sequence of update operations on an object.
 * <p>
 * The sequence of operations shares a base Timestamp (two dimensional
 * timestamp), which is the unit of visibility of operations group. Each
 * individual operation has a unique TripleTimestamp based on the common
 * timestamp.
 * <p>
 * Thread-safe.
 * 
 * @author mzawirski
 */
public class CRDTObjectUpdatesGroup<V extends CRDT<V>> {

    protected CRDTIdentifier id;
    protected CausalityClock dependencyClock;
    protected TimestampMapping timestampMapping;
    protected List<CRDTUpdate<V>> operations;
    protected V creationState;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public CRDTObjectUpdatesGroup() {
    }

    /**
     * Constructs a group of operations.
     * 
     * @param id
     * @param baseTimestamp
     * @param creationState
     */
    public CRDTObjectUpdatesGroup(CRDTIdentifier id, TimestampMapping timestampMapping, V creationState) {
        this.id = id;
        this.timestampMapping = timestampMapping;
        this.operations = new LinkedList<CRDTUpdate<V>>();
        this.creationState = creationState;
    }

    /**
     * @return CRDT identifier for the object on which the operations are
     *         executed
     */
    public CRDTIdentifier getTargetUID() {
        return id;
    }

    /**
     * @param mapping
     * @param dependencyClock
     */
    public synchronized void init(final TimestampMapping mapping, CausalityClock dependencyClock) {
        this.timestampMapping = mapping;
        this.dependencyClock = dependencyClock;
    }

    /**
     * @return base timestamp of all operations in the sequence
     */
    public synchronized Timestamp getBaseTimestamp() {
        return timestampMapping.getClientTimestamp();
    }

    public synchronized void addSystemTimestamp(final Timestamp ts) {
        timestampMapping.addSystemTimestamp(ts);
    }

    public synchronized TimestampMapping getTimestampMapping() {
        // FIXME: release read-only copy to the client!
        return timestampMapping;
    }

    /**
     * Returns the minimum causality clock for the object on which the
     * operations are to be executed, representing causal dependencies of this
     * group of operations.
     * 
     * @return causality clock of object state when operations have been issued
     * 
     */
    public synchronized CausalityClock getDependency() {
        return dependencyClock;
    }

    /**
     * @return read-only reference to the internal list of operations
     *         constituting this group
     */
    public synchronized List<CRDTUpdate<V>> getOperations() {
        return Collections.unmodifiableList(operations);
    }

    /**
     * Appends a new operation to the sequence of operations.
     * 
     * @param op
     *            next operation to be applied within the transaction
     */
    public synchronized void append(CRDTUpdate<V> op) {
        op.setTimestampMapping(timestampMapping);
        operations.add(op);
    }

    /**
     * @return true if this is a create operations containing initial state
     */
    public boolean hasCreationState() {
        return creationState != null;
    }

    /**
     * @return initial state of an object; null if {@link #hasCreationState()}
     *         is false
     */
    public V getCreationState() {
        return creationState;
    }
}
