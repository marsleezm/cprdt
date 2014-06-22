package swift.application.reddit.crdt;

import swift.crdt.core.CRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.VersionNotFoundException;

public interface VoteableSet<T extends CRDT<T>, V, U> extends CRDT<T> {
    public void vote(V element, U voter, VoteDirection direction) throws VersionNotFoundException, NetworkException;
    void applyVote(V element, VoteCounterVoteUpdate<U> voteUpdate);
    
    public VoteCounter<U> voteCounterOf(V element) throws VersionNotFoundException, NetworkException;
}
