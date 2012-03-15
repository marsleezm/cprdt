package swift.crdt.interfaces;

import java.io.Serializable;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;

/**
 * Common interface for Commutative Replicated Data Types (CRDTs) definitions.
 * <p>
 * Implementations are encouraged to use {@link BaseCRDT} as a base class.
 * <p>
 * Conceptually, every CRDT object has an associated (1) state made of
 * application of udpate operations and (2) a clock indicating which updates are
 * reflected in the state. A CRDT object is identified by {@link CRDTIdentifier}
 * , uniquely across the system.
 * <p>
 * Implementation must provide ability of viewing past snapshots of the object
 * (at time specified by {@link #prune(CausalityClock)} or any later time) and
 * generate new update operations. Applications using CRDT object always use it
 * together with transaction handle ({@link TxnHandle}). This handler is
 * available via {@link #getTxnHandle()} and should be used by queries (to
 * determine snapshot point) and methods preparing update operations (to
 * determine snapshot point, generate timestamp and register update).
 * 
 * @author annettebieniusa
 * 
 * @param <V>
 *            CvRDT type implementing the interface
 */

public interface CRDT<V> extends Serializable {
    /**
     * Merges the object with other object state of the same type.
     * <p>
     * In the outcome, updates and clock of provided object are reflected in
     * this object.
     * 
     * TODO: specify pruneClock behavior
     * 
     * @param other
     *            object state to merge with
     */
    void merge(V other);

    /**
     * Executes an update operation on this object.
     * <p>
     * In the outcome, operation and its timestamp are reflected in the state of
     * this object.
     * 
     * @param op
     *            operation to be executed
     */
    void executeOperation(CRDTOperation op);

    /**
     * Prunes the object state to remove versioning meta data from operations
     * dating from before pruningPoint inclusive.
     * <p>
     * After this call returns, snapshots prior or concurrent to pruningPoint
     * will be undefined and should not be requested. Clock of an object is
     * unaffected.
     * 
     * @param pruningPoint
     *            clock up to which data clean-up is performed
     */
    void prune(CausalityClock pruningPoint);

    /**
     * Remove the effects of the transaction associated to the timestamp.
     * <p>
     * TODO: what about the clock?
     * 
     * @param ts
     *            time stamp of transaction that is rolled back.
     */
    void rollback(Timestamp ts);

    /**
     * Returns the identifier for the object.
     */
    CRDTIdentifier getUID();

    /**
     * Sets the identifier for the object. <b>INVOKED ONLY BY SWIFT SYSTEM.</b>
     * 
     * @param id
     */
    void setUID(CRDTIdentifier id);

    /**
     * Returns the causality clock including timestamps of all update operations
     * reflected in the object state.
     * 
     * @return causality clock associated to object
     */
    CausalityClock getClock();

    /**
     * Sets the causality clock that is associated to the current object state.
     * <b>INVOKED ONLY BY SWIFT SYSTEM.</b>
     * 
     * @param c
     */
    void setClock(CausalityClock c);

    /**
     * Returns the causality clock representing the minimum clock for which
     * versioning of an object is available. Should always be greater or equal
     * {@link #getClock()}.
     * 
     * @return pruned causality clock associated with the object
     */
    CausalityClock getPruneClock();

    /**
     * Sets the prune causality clock that is associated to the current object
     * state. <b>INVOKED ONLY BY SWIFT SYSTEM.</b>
     * 
     * @param c
     */
    void setPruneClock(CausalityClock c);

    /**
     * Creates a copy of an object with optionally restricted state according to
     * pruneClock and versionClock.
     * <p>
     * pruneClock and versionClock parameters are optional (can be null), but if
     * both specified versionClock must dominate pruneClock.
     * 
     * @param pruneClock
     *            when not null, the returned state does not contains versioning
     *            information until pruneClock, i.e. the state is summarized
     *            until pruneClock but versioning behind pruneCLock is
     *            unavailable.
     * @param versionClock
     *            when not null, the returned state is restricted to the
     *            specified version
     * @param txn
     * @return a copy of an object, including clocks, uid and txnHandle.
     */
    // TODO: discuss with Annette a "clientCopy" option?
    TxnLocalCRDT<V> getTxnLocalCopy(CausalityClock pruneClock, CausalityClock versionClock, TxnHandle txn);

}
