package intellip.flwr.io.cache;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

/**
 * <p>An object of class <tt>IndexCache</tt> takes an array of bytes as input
 * through the <tt>put()</tt> method and returns a <tt>long</tt> handler.
 * This handler can be thought of as the memory location where the input
 * bytes are stored. The same handler can be passed to <tt>put()</tt> method
 * which in turn returns back the array of bytes previously stored in that
 * memory location.</p>
 *
 * <p>Internally, <tt>IndexCache</tt> uses <i>memory mapped buffers</i> to store and retrieve
 * data to and from persistent data files. <tt>IndexCache</tt> creates two different 
 * files in the disk - one for data cache and the other for index cache. Every time a 
 * <tt>put()</tt> method is called both the data and index caches are updated. </p>
 *
 * <p>The class also implements a LRU cache mechanism when <tt>get()</tt> method is called.
 * This LRU cache is built in memory and it only stores part or full of the index cache to help
 * make the data access faster. Most recently accessed values of the index cache is stored 
 * in the memory by evicting the least recently accessed values out of the LRU cache. 
 * By default the size of the memory based LRU index cache is 128 MB. However this can be
 * overridden during constructor invocation.</p>
 * 
 * <p>Considering the large number of parameters that need to be setup while calling the 
 * constructor, this class implements a builder pattern that builds the cache parameters [EFFJ2, pp. 11].
 * 
 * 
 * <p>This class can be instantiated as a reader or writer. By default this class creates
 * a writer object which can both read and write from the cache randomly. However by calling
 * the <tt>InstantiateAsReader(String cacheFileName)</tt> method during invocation, the class
 * can be invoked as a reader from an existing cache.</p>
 */

public class IndexCache extends Cache implements Closeable {

       /* 
	* bytePosition stores the current position in cache file from
	* where to read or write data. This also indicates
	* amount of bytes stored so far in the file
	*/
	private       long    bytePosition = 0;     	
	private       int     nMap         = 0;     // number of maps so far created
	private final String  IndexCachePath;   
	private final String  IndexCacheName;    
	private final long    LruCacheSize;
	
	
	/* ************************************************************************
	 * CONSTRUCTOR Section START
	 * ************************************************************************/
	 
	public static class Builder {
	
		// mandatory parameters
		private final String  DataCachePath;
		private final String  IndexCachePath;
		private final long    block_size;
		
		// optional parameters - initialized to default values where possible
		private String  CacheName;
		private boolean isReader     = false;
		private long    LruCacheSize = 1 << 27; // 128MB
		
		// constructor for the builder
		public Builder( String DataCachePath, String IndexCachePath, long block_size ) {
			this.DataCachePath  = DataCachePath;
			this.IndexCachePath = IndexCachePath;
			this.block_size     = block_size;
		}
		
		public Builder WithLRUCacheSize( long size )
		{ LruCacheSize = size; return this; }
		
		public Builder SetAsReader( String CacheName ) { 
			this.isReader  = true;
			this.CacheName = CacheName; 
			return this; 
		}
		
		// invoke the private constructor of parent class and pass the builder
		public IndexCache build() {
			return new IndexCache(this);
		}
	}
	
	// private constructor - this can only be invoked from the Builder's build() method
	private IndexCache(Builder builder) throws Exception {
	
		// do we need to invoke as a reader?
		if (builder.isReader) { // existing index and data cache
			super(builder.DataCachePath, builder.block_size, builder.CacheName);
			
			// determine the index cache name for the current index cache
			// at this point we assume that the index cache is already existing
			// if the assumption is incorrect, we throw exception
			// index cache name is same as the data cache file name but with different file extension (.bin.idx)
			this.IndexCacheName = builder.CacheName;
			this.IndexCachePath = builder.IndexCachePath;
			if(!super.isValidPath(this.IndexCacheName + this.IndexCachePath + ".bin.idx")) 
				throw new FileNotFoundException("Index cache file " + this.IndexCacheName + " not found!");
		}
		else { // new index and data cache
			super(builder.DataCachePath, builder.block_size);
			this.IndexCachePath = builder.IndexCachePath;
			this.IndexCacheName = super.getCacheName();
		}
	
		this.LruCacheSize   = builder.LruCacheSize;
	}

	/* ************************************************************************
	 * CONSTRUCTOR Section END
	 * ************************************************************************/

	/* The below data structure represents one element in the index list.
	 * This class also has methods to serialize / deserialize the data into bytes
	 *
         * ItemAddress approximately takes 
	 * 32 bytes of memory [ALGO4, pp. 201] - 8 byte for long, 4 byte for int, 
	 * 16 byte object overhead and 4 byte of padding (for 8-byte machine-words 
	 * on 64-bit machines)
	 */
	private class ItemAddress {
		private long itemPos;              // at which position the item resides (first byte)
		private int  itemSize;             // how many bytes is the address made of 
	}
	
	
	/* 
	 * We will use constWidthCache to handle the index cache.
	 * For data cache we will create the actual implementation
	 */
	ConstWidthCache idx = new ConstWidthCache(IndexCachePath, 1 << 13, IndexCacheName); // for index
	FileChannel ch = dataCacheFile.getChannel();                                        // for data

	/* 
	 * Below data structures implement one memory based LRU cache to store 
	 * index. Index is persistently stored in disk but
	 * memory based buffers are created in memory to enable faster access
	 * 
	 * This index resides in the Java Heap memory whereas actual 
	 * items remain in the hard disk. The obvious advantage is accessing
	 * the index is much faster than accessing the data.
	 * 
	 * The biggest disadvantage of this approach is, index list (as opposed
	 * to a disk based index file can not grow very big for obvious heap 
	 * memory limitations. 
	 *
	 */
	private LruCache<Integer, ItemAddress> lruIndexCache = new LruCache<Integer, ItemAddress>(LruCacheSize);


	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	/* 
	 * Set() method - This method stores an array of bytes to the file system 
	 * by using memory mapped buffer. The memory allocation logic is like this:
	 * 
	 * ALLOCATION LOGIC
	 * --------------------------------------------------------------------------
	 * a) Find the "gap" using below formula:
	 *    gap = Number of bytes to write - Space available in current map
         * b) if gap is greater than 0, 
     	 *    c) if the currentSpaceAvailable > 0, allocate currentSpaceAvailable amount of byte
     	 *    d) Divide the above with map size, take ceil: 12/10 = 2
     	 *    e) allocate block by block
     	 *    f) recalculate currentSpaceAvailable = 8
     	 * g) Else if gap is less than equal to 0, 
     	 *    allocate in current map and recalculate remaining space in current map
     	 * --------------------------------------------------------------------------
     	 *    
	 * @see cache.Cache#set(byte[])
	 */
	@Override
	public long set(byte[] bytes) throws IOException {

		// number of bytes to store
		int BytesToStore = bytes.length;

		// calculate the gap value
		// gap = (BytesToStore - ( nMap * blockSize - bytePosition ));
		// check if gap is greater than zero
		if ( BytesToStore > ( nMap * blockSize - bytePosition ))

		// Determine how many maps will be required for storing bytes
		int NoOfMapsRequired  = (int) Math.ceil((double) BytesToStore / (double) blockSize);

		try {

			for ( int i = 0; i <= NoOfMapsRequired; i++) {

				// create a mapped byte buffer
				MappedByteBuffer mb = ch.map( FileChannel.MapMode.READ_WRITE, bytePosition, blockSize);

				// determine how many bytes to be written in this map
				// this has to be minimum of free space in current map 
				// or the buffer block size.
				int length = blockSize > DataRemainInBytes ? DataRemainInBytes : blockSize;
				mb.put(bytes, start, length);


			}


		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		return 0;
	}

	@Override
	public byte[] get(long pos) {
		// TODO Auto-generated method stub
		return null;
	}

}
