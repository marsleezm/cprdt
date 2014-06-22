package swift.application.reddit;

import swift.crdt.core.CRDTIdentifier;

public interface Thing<T> extends Date<T> {
    public CRDTIdentifier getVoteCounterSetIdentifier();
    
    public CRDTIdentifier getVoteCounterIdentifier();
}
