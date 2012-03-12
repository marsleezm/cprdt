package sys.dht.catadupa.crdts;

import static sys.utils.Log.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import sys.dht.catadupa.crdts.time.*;


public class ORSet<V> extends AbstractORSet<V> implements CvRDT<ORSet<V>> {

	Set<Timestamp> tomb;
	Map<V, Set<Timestamp>> e2t;
	Map<Timestamp, Set<V>> t2e;

	public ORSet() {
		e2t = new HashMap<V, Set<Timestamp>>();
		t2e = new HashMap<Timestamp, Set<V>>();
		tomb = new HashSet<Timestamp>();
	}

	@Override
	public boolean isEmpty() {
		return e2t.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return e2t.containsKey(o);
	}

	@Override
	public int size() {
		return e2t.size();
	}

	@Override
	public boolean add(V v) {
		return add(v, rt.recordUpdate(this));
	}

	@Override
	public boolean add(V v, Timestamp t) {
		get(t).add(v);
		return get(v).add(t);
	}

	@Override
	public boolean remove(Object o) {
		Set<Timestamp> s = e2t.get(o);
		if (s != null) {
			tomb.addAll(s);
			e2t.remove(o);
			t2e.keySet().removeAll(s);
		}
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		Set<V> deletes = new HashSet<V>(e2t.keySet());
		deletes.removeAll(c);
		for (V i : deletes)
			remove(i);

		return deletes.size() > 0;
	}

	@Override
	public synchronized void clear() {
		for (Entry<V, Set<Timestamp>> i : e2t.entrySet()) {
			tomb.addAll(i.getValue());
		}
		e2t.clear();
		t2e.clear();
	}

	@Override
	public Iterator<V> iterator() {
		return new _OrSetIterator();
	}

	@Override
	public <Q> Q[] toArray(Q[] a) {
		return e2t.keySet().toArray(a);
	}

	public void merge(ORSet<V> other) {
		merge(other, new ArrayList<V>(), new ArrayList<V>());
	}

	public void merge(ORSet<V> other, Collection<V> added, Collection<V> removed) {

		List<Timestamp> newTombs = new ArrayList<Timestamp>();
		for (Timestamp t : other.tomb)
			if (tomb.add(t))
				newTombs.add(t);

		for (Map.Entry<V, Set<Timestamp>> e : other.e2t.entrySet()) {

			V v = e.getKey();
			for (Timestamp t : e.getValue())
				if (!tomb.contains(t)) {
					Set<Timestamp> s = get(v);
					if (s.isEmpty())
						added.add(v);

					s.add(t);
					get(t).add(v);
				}
		}

		for (Timestamp t : newTombs) {
			Set<V> vs = t2e.remove(t);
			if (vs != null)
				for (Iterator<V> it = vs.iterator(); it.hasNext();) {
					V v = it.next();
					Set<Timestamp> ts = get(v);
					ts.remove(t);
					if (ts.isEmpty()) {
						it.remove();
						removed.add(v);
					}
				}
		}
		Log.finest("Added:" + added + " " + "Removed:" + removed);
	}

	public Map<V, Timestamp> subSet(Collection<? extends Timestamp> timestamps) {
		Map<V, Timestamp> res = new HashMap<V, Timestamp>();
		for (Timestamp t : timestamps)
			for (V v : t2e.get(t))
				res.put(v, t);
		return res;
	}

	private Set<Timestamp> get(V v) {
		Set<Timestamp> s = e2t.get(v);
		if (s == null)
			e2t.put(v, s = new HashSet<Timestamp>());
		return s;
	}

	private Set<V> get(Timestamp t) {
		Set<V> s = t2e.get(t);
		if (s == null)
			t2e.put(t, s = new HashSet<V>());
		return s;
	}

	public String toString() {
		return e2t.keySet().toString();
	}
	
	class _OrSetIterator implements Iterator<V> {

		Map.Entry<V, Set<Timestamp>> curr;
		Iterator<Map.Entry<V, Set<Timestamp>>> it;

		_OrSetIterator() {
			it = e2t.entrySet().iterator();
		}

		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public V next() {
			return (curr = it.next()).getKey();
		}

		@Override
		public void remove() {
			tomb.addAll(curr.getValue());
			t2e.entrySet().remove(curr.getValue());
			it.remove();
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
}