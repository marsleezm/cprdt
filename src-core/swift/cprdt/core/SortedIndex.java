package swift.cprdt.core;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.crdt.core.Copyable;


public class SortedIndex<K,V extends Comparable<V>> implements Copyable {
	protected NavigableSet<Entry> index;
    protected Map<V, K> keyValues;
    
    Comparator<K> comparator;
	
	public SortedIndex() {
		this.index = new TreeSet<Entry>();
        this.keyValues = new HashMap<V,K>();
        this.comparator = null;
	}
	
	public SortedIndex(Comparator<K> comparator) {
        this.comparator = comparator;
		this.index = new TreeSet<Entry>();
	}
	
	private SortedIndex(NavigableSet<Entry> index, Map<V, K> keyValues, Comparator<K> comparator) {
	    this.index = index;
	    this.keyValues = keyValues;
	    this.comparator = comparator;
	}
	
	public void update(K newKey, V value) {
	    K oldKey = keyValues.get(value);
	    
	    if (oldKey != null) {
	        if (oldKey.equals(newKey)) {
	            // No actual update to do
	            return;
	        }
    	    Entry oldEntry = new Entry(oldKey, value);
            index.remove(oldEntry);
	    }
	    
        Entry newEntry = new Entry(newKey, value);
        index.add(newEntry);
        
        keyValues.put(value, newKey);
	}
	
	public void remove(V value) {
	    K key = keyValues.remove(value);
	    if (key != null) {
	        Entry entry = new Entry(key, value);
    	    index.remove(entry);
	    }
	}
	
	public List<V> find(V from, boolean fromInclusive, V to, boolean toInclusive) {
        return find(from, fromInclusive, to, toInclusive, Integer.MAX_VALUE, false);
    }
	
	public List<V> find(V from, boolean fromInclusive, V to, boolean toInclusive, int limit) {
	    return find(from, fromInclusive, to, toInclusive, limit, false);
	}
	
	public List<V> find(V from, boolean fromInclusive, V to, boolean toInclusive, boolean reversed) {
        return find(from, fromInclusive, to, toInclusive, Integer.MAX_VALUE, reversed);
    }
	
	private NavigableSet<Entry> subSet(V fromValue, boolean fromInclusive, V toValue, boolean toInclusive, boolean reversed) {
	    
	    Entry from = null;
        if (fromValue != null) {
            K fromKey = keyValues.get(fromValue);
            if (fromKey != null) {
                from = new Entry(fromKey, fromValue);
            }
        }
        Entry to = null;
        if (toValue != null) {
            K toKey = keyValues.get(toValue);
            if (toKey != null) {
                to = new Entry(toKey, toValue);
            }
        }
	    
	    NavigableSet<Entry> subSet = index;
        if (reversed) {
            subSet = subSet.descendingSet();
        }
        if (from == null) {
            if (to != null) {
                subSet = subSet.headSet(to, toInclusive);
            }
        } else {
            if (to == null) {
                subSet = subSet.tailSet(from, toInclusive);
            } else {
                if (from != null) {
                    subSet = subSet.subSet(from, fromInclusive, to, toInclusive);
                }
            }
        }
        return subSet;
	}
	
	public List<V> find(V fromValue, boolean fromInclusive, V toValue, boolean toInclusive, int limit, boolean reversed) {
	    int i = 0;
	    
	    NavigableSet<Entry> subSet = subSet(fromValue, fromInclusive, toValue, toInclusive, reversed);
	    
        LinkedList<V> list = new LinkedList<V>();
	    
	    for (Entry entry: subSet) {
	        if (i == limit) {
	            return list;
	        }
	        
	        list.add(entry.value);
	        i++;
	    }
	    return list;
	}
	
	public SortedSet<Entry> entrySet() {
	    return entrySet(false);
	}
	
	public SortedSet<Entry> entrySet(boolean reversed) {
        if (reversed) {
            return Collections.unmodifiableSortedSet(this.index.descendingSet());
        } else {
            return Collections.unmodifiableSortedSet(this.index);
        }
    }
	
	public SortedIndex<K,V> copy(V fromValue, boolean fromInclusive, V toValue, boolean toInclusive, boolean reversed) {
	    TreeSet<Entry> newIndex = new TreeSet<Entry>(this.subSet(fromValue, fromInclusive, toValue, toInclusive, reversed));
	    HashMap<V,K> newKeyValues = new HashMap<V,K>();
	    for (Entry entry: newIndex) {
	        newKeyValues.put(entry.getValue(), entry.getKey());
	    }
        return new SortedIndex<K,V>(newIndex, newKeyValues, comparator);
    }
	
	public SortedIndex<K,V> copy() {
	    return new SortedIndex<K,V>(new TreeSet<Entry>(index), new HashMap<V,K>(keyValues), comparator);
	}
	
	public class Entry implements Comparable<Entry> {
	    private K key;
	    private V value;
	    
	    private Entry(K key, V value) {
	        if (key == null || value == null) {
	            throw new IllegalArgumentException("Null key or value");
	        }
	        this.key = key;
	        this.value = value;
	    }
	    
	    public K getKey() {
            return key;
        }
	    
	    public V getValue() {
	        return value;
	    }
	    
        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(Entry o) {
            int keyCompare;
            if (comparator == null) {
                keyCompare = ((Comparable<K>) key).compareTo(o.key);
            } else {
                keyCompare = comparator.compare(key, o.key);
            }
            if (keyCompare == 0) {
                return value.compareTo(o.value);
            } else {
                return keyCompare;
            }
        }
	}
}
