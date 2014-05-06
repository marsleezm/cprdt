package swift.cprdt.core;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;


public class UniqueSortedIndex<K,V> {
	protected NavigableMap<K,V> index;
	
	public UniqueSortedIndex() {
		this.index = new TreeMap<K,V>();
	}
	
	public UniqueSortedIndex(Comparator<K> comparator) {
		this.index = new TreeMap<K,V>(comparator);
	}
	
	public void update(K oldKey, K newKey) {
	    if (oldKey == newKey) {
	        return;
	    }
		V value = index.remove(oldKey);
		if (value != null) {
			index.put(newKey, value);
		}
	}
	
	public V remove(K key) {
		return index.remove(key);
	}
	
	public V get(K key) {
		return index.get(key);
	}
    
    public V add(K key, V value) {
        return index.put(key, value);
    }
	
	public List<V> find(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		return new LinkedList<V>(index.subMap(fromKey, fromInclusive, toKey, toInclusive).values());
	}
}
