package intellip.flwr.io;

import java.io.BufferedInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;


@SuppressWarnings("serial")
public class LruCache<Key, Value> extends LinkedHashMap<Key, Value> {

	private final int capacity;
	private long accessCount = 0;
	private long hitCount    = 0;
	
	public LruCache(int capacity)
	{
		/*
		 *  Here we invoke the LinkedHashMap with given capacity and 
		 *  1.1. load factor so that rehashing operation never happens.
		 *  the 3rd parameter set to true makes the cache retrieval 
		 *  based on access-order as opposed to insert order
		 */
		
		super(capacity + 1, 1.1f, true); 
		this.capacity = capacity;
	}
	
	/**
	 * Returns the <tt>value</tt> pertaining to a given <tt>key</tt>.<br />
	 * Both <tt>value</tt> and <tt>key</tt> can be of generic types.
	 */
	public Value get(Object key) {
		
		// increment the accessCount as new access requests are made
		accessCount++;
		
		// is the object available in local cache? If yes, increment cache hit counter
		if(containsKey(key)) hitCount++;
		
		// return the value against the key. If the key is not present, value will be null
		return super.get(key);
	}
	
	/**
	 * Returns <tt>true</tt> if this <code>LruCache</code> has more entries than the maximum 
	 * specified when it was created.
     * <br />
	 * This method is actually automatically invoked by put and putAll after inserting a new entry 
	 * into the map. It provides the implementor with the opportunity to remove the eldest entry 
	 * each time a new one is added. Since the map represents a cache: it allows the map to reduce 
	 * memory consumption by deleting stale entries.
	 * 
	 * @param eldest 
	 *              The <code>Entry</code> in question. This implementation does not use this since for
	 *              this implementation only size of the cache is matters.
	 */
	@Override
    protected boolean removeEldestEntry(final Map.Entry<Key, Value> eldest) {
        return super.size() > capacity;
	}

	/**
	 * Returns the number of times <tt>get()</tt> method was invoked. <br />
	 * This value, when used in conjunction with <tt>getHitCount()</tt> can be 
	 * further used to calculate cache hit-ratio
	 * 
	 */
	public long getAccessCount()
	{    
		return accessCount;
	}

	/**
	 * Returns the number of times a <tt>value</tt> is found in the cache
	 * when <tt>get()</tt> method is invoked with certain <tt>key</tt> <br />
	 * This value, when used in conjunction with <tt>getAccessCount()</tt> can be 
	 * further used to calculate cache hit-ratio
	 * 
	 */
	public long getHitCount()
	{
		return hitCount;
	}

	/*
	 * A SMALL TEST CLIENT
	 */
	public static void main(String[] args) {
		
		LruCache<String, String> cache = new LruCache<String, String>(5);
		
		
		/*
		 * TEST STRATEGY
		 * We take one key from StdIn and search if the key is present in the 
		 * Cache. If yes, we immediately display the value, else we ask for the
		 * value and take that as input
		 */
		
		// scanner for reading input
		Scanner scanner = new Scanner(new BufferedInputStream(System.in), "UTF-8");
				

		String key, value;
		while (true) {
			
			System.out.println ("Enter a key : ");
			key = scanner.next();
			
			// a check to break the loop
			if (key.startsWith("-")) { break; }
			
			value = cache.get(key);
			
			if ( value == null ) { // cache miss
				
				// ask for the value
				System.out.println ("Enter value : ");
				value = scanner.next();
				
				cache.put(key, value);
			} // end of if
			else { // cache hit
				System.out.println (value);
			}
		} // end of while
		
		System.out.println ("Cache hit = " + cache.getHitCount() + " out of " + cache.getAccessCount() );
		
	} // end of main
} // end of class
