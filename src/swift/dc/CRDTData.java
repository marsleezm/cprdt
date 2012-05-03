package swift.dc;

import java.util.Set;
import java.util.TreeSet;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;

public class CRDTData<V extends CRDT<V>> {
    /**
     * true if the entry corresponds to an object that does not exist
     */
    boolean empty;
    /**
     * crdt object
     */
    CRDT<V> crdt;
    /**
     * crdt object
     */
    CRDT<V> prunedCrdt;
    /**
     * CRDT unique identifier
     */
    CRDTIdentifier id;
    /**
     * current clock reflects all updates and their causal past
     */
    CausalityClock clock;
    /**
     * prune clock reflects the updates that have been discarded, making it
     * impossible to access a snapshot that is dominated by this clock
     */
    CausalityClock pruneClock;
    transient Set<Observer> observers;
    transient Set<Observer> notifiers;
    transient Object dbInfo;
    
    CRDTData() {
        observers = new TreeSet<Observer>();
        notifiers = new TreeSet<Observer>();
    }

    CRDTData(CRDTIdentifier id) {
        this.id = id;
        observers = new TreeSet<Observer>();
        notifiers = new TreeSet<Observer>();
        this.empty = true;
    }

    CRDTData(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clock, CausalityClock pruneClock) {
        this.crdt = crdt;
        if( DCDataServer.prune)
            this.prunedCrdt = crdt.copy();
        this.id = id;
        this.clock = clock;
        this.pruneClock = pruneClock;
        observers = new TreeSet<Observer>();
        notifiers = new TreeSet<Observer>();
        this.empty = false;
    }
    
    void initValue( CRDT<V> crdt, CausalityClock clock, CausalityClock pruneClock) {
        this.crdt = crdt;
        if( DCDataServer.prune)
            this.prunedCrdt = crdt.copy();
        this.clock = clock;
        this.pruneClock = pruneClock;
        this.empty = false;
    }

    boolean addObserver(Observer o) {
        return observers.add(o);
    }

    boolean remObserver(Observer o) {
        return observers.remove(o);
    }

    boolean addNotifier(Observer o) {
        return notifiers.add(o);
    }

    boolean remNotifier(Observer o) {
        return notifiers.remove(o);
    }

    public int hashCode() {
        return id.hashCode();
    }

    public boolean equals(Object obj) {
        return obj instanceof CRDTData && id.equals(((CRDTData) obj).id);

    }

    public Object getDbInfo() {
        return dbInfo;
    }

    public void setDbInfo(Object dbInfo) {
        this.dbInfo = dbInfo;
    }
    public void mergeInto( CRDTData<?> d) {
        empty = true;
        crdt.merge((CRDT<V>)d.crdt);
        clock.merge(d.clock);
        if( DCDataServer.prune) {
            this.prunedCrdt.merge((CRDT<V>)d.crdt);
        }
        pruneClock.merge(d.pruneClock);
    }

    public boolean isEmpty() {
        return empty;
    }

    public CRDT<V> getCrdt() {
        return crdt;
    }

    public CRDTIdentifier getId() {
        return id;
    }

    public CausalityClock getClock() {
        return clock;
    }

    public CausalityClock getPruneClock() {
        return pruneClock;
    }
}
