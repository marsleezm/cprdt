package swift.application.swiftlinks.crdt;

import java.util.Collections;
import java.util.Set;

import swift.crdt.core.CRDTUpdate;

/**
 * Update operation for any voteable set
 * 
 * @author Iwan Briquemont
 *
 * @param <T>
 * @param <V>
 * @param <U>
 */
public class VoteableSetVoteUpdate<T extends VoteableSet<T, V, U>, V, U> implements CRDTUpdate<T> {
    private V element;
    private VoteCounterVoteUpdate<U> update;

    // Kryo
    public VoteableSetVoteUpdate() {
    }

    public VoteableSetVoteUpdate(V element, VoteCounterVoteUpdate<U> voteUpdate) {
        this.element = element;
        this.update = voteUpdate;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.applyVote(element, update);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return element;
    }

    @Override
    public Set<Object> affectedParticles() {
        return (Set<Object>) Collections.singleton(element);
    }
}
