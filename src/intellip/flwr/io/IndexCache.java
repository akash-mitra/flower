package intellip.flwr.io;

import intellip.flwr.util.Log;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

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

public class IndexCache implements Closeable {

    
	// data and index cache files location, name related variables
	private final String     CacheName;                      // unique name for each cache
	private final boolean    isCompress;                     // is the cache file compressed
	private final String     IndexCachePath;                 // directory location of index cache files
	private final String     DataCachePath;                  // directory location of data cache files
	private final String     IndexCacheName;                 // full-name of index cache
	private final String     DataCacheName;                  // full-name of data cache
	private final int        LruCacheSize;                   // size of LRU cache in memory


	// Data Cache Related variables
	private       long       lBytePosition;                  // amount of bytes stored so far
	private final long       lBlockSize;                     // size of each memory mapped buffer block
	private RandomAccessFile DataCacheFile;                  // handler for data cache file
	private final List<MappedByteBuffer> data_maps;          // list of maps in the data cache file


	// index cache related variables
	private RandomAccessFile IndexCacheFile;                 // handler for index cache file
	private final List<MappedByteBuffer> index_maps;         // list of maps in the index cache file
	// below data structure represents one element in the index list.
	private class ItemAddress {                              // consumes 32 bytes, refer [ALGO4, pp. 201]
		private long itemPos;                                // at which position the item resides (first byte)
		private int  itemSize;                               // how many bytes is the address made of 
	}
	
	private long     lItemSeqNo;                             // number of items already loaded in data and index


	/* ************************************************************************
	 * Constructor uses builder pattern, Refer [EFFJ2, pp. 20]
	 * ************************************************************************/

	public static class Builder {

		// mandatory parameters
		private final String  DataCachePath;
		private final String  IndexCachePath;

		// optional parameters - initialized to default values where possible
		private String  CacheName;
		private boolean isReader     = false;
		private int     LruCacheSize = 1 << 27; // 128MB
		private boolean isCompress   = false;
		private long    block_size   = 1 << 14; //  64KB

		// constructor for the builder
		public Builder( String DataCachePath, String IndexCachePath) {
			this.DataCachePath  = DataCachePath;
			this.IndexCachePath = IndexCachePath;
			this.block_size     = block_size;
		}

		public Builder withLRUCacheSize( int size ) { 
			this.LruCacheSize = size; 
			return this; 
		}
		
		public Builder withBlockSize( long block_size ) { 
			this.block_size = block_size;
			return this;
		}

		public Builder useExistingFile( String CacheName ) { 
			this.isReader  = true;
			this.CacheName = CacheName; 
			return this; 
		}

		public Builder compressCache() {  
			this.isCompress = true;
			return this;
		}

		// invoke the private constructor of parent class and pass the builder
		public IndexCache build() throws Exception {
			return new IndexCache(this);
		}
	}

	// private constructor - this can only be invoked from the Builder's build() method
	private IndexCache(Builder builder) throws Exception {

		// do we need to invoke as a reader?
		if (builder.isReader) { // existing index and data cache

			// determine existing cache files 
			CacheName      = builder.CacheName;
			isCompress     = builder.isCompress;
			IndexCachePath = builder.IndexCachePath;
			DataCachePath  = builder.DataCachePath;
			IndexCacheName = IndexCachePath + CacheName + (isCompress ? "zip.idx" : "bin.idx");
			DataCacheName  = DataCachePath + CacheName + (isCompress ? "zip.cac" : "bin.cac");

			if(!isValidPath(IndexCacheName)) 
				throw new FileNotFoundException("Index cache file " + IndexCacheName + " not found!");
			if(!isValidPath(DataCacheName)) 
				throw new FileNotFoundException("Data cache file " + DataCacheName + " not found!");

			DataCacheFile  = new RandomAccessFile(DataCacheName,  "r");
			IndexCacheFile = new RandomAccessFile(IndexCacheName, "r");
			
			// TODO: Determine lItemSeqNo by dividing size of index cache with the size of one index element
		}
		else { // new index and data cache

			// generate random cache name
			CacheName      = String.valueOf(Math.abs(UUID.randomUUID().getMostSignificantBits()));
			isCompress     = builder.isCompress;
			IndexCachePath = builder.IndexCachePath;
			DataCachePath  = builder.DataCachePath;

			if(!isValidPath(IndexCachePath)) throw new FileNotFoundException("Path does not exist: " + IndexCachePath);
			if(!isValidPath(DataCachePath))  throw new FileNotFoundException("Path does not exist: " + DataCachePath);

			IndexCacheName = IndexCachePath + CacheName + (isCompress ? "zip.idx" : "bin.idx");
			DataCacheName  = DataCachePath  + CacheName + (isCompress ? "zip.cac" : "bin.cac");

			DataCacheFile  = new RandomAccessFile(DataCacheName,  "rw");
			IndexCacheFile = new RandomAccessFile(IndexCacheName, "rw");
			
			lItemSeqNo     = 0; // so far no item has been put
		}

		LruCacheSize       = builder.LruCacheSize;
		lBlockSize         = builder.block_size;
		lBytePosition      = 0;
		data_maps          = new ArrayList<MappedByteBuffer>();
		index_maps         = new ArrayList<MappedByteBuffer>();
		//isReadOnly         = builder.isReader;

		Log.write("From inside, " + builder.DataCachePath);
	}

	/* ************************************************************************
	 * CONSTRUCTOR Section END
	 * ************************************************************************/



	/**
	 * This method takes an array of bytes as input, writes it to the cache file and
	 * returns a sequential number of <tt>long</tt> data type as an identifier for the 
	 * array of bytes inserted. This sequential number can be passed to the <tt>get()</tt>
	 * method to retrieve back the array of bytes. <br />
	 * <tt>put()</tt> can only be invoked on a writable cache. Writable caches are those that
	 * are created exclusively 
	 * @param bytes byte[] An array of bytes to store
	 * @return handler long A sequential number that uniquely identifies the array of bytes stored
	 */
	 
	 /*
	  * Put must be atomic - if one statement inside put fails, entire put fails and the consistency
	  * of both index and data files are preserved.
	  */
	public long put( byte[] bytes ) {

		
		return 0;
	}

	/*
	 * Helper methods 
	 * ------------------------------------------------------------------
	 */
	protected boolean isValidPath(String path) {
		File f = new File(path);
		if (!f.exists()) 
			return false;
		else return true;
	}

	public void close() throws IOException {
        for (MappedByteBuffer mapping : data_maps)
            clean(mapping);
        DataCacheFile.close();
	}

    private void clean(MappedByteBuffer mapping) {
        if (mapping == null) return;
        Cleaner cleaner = ((DirectBuffer) mapping).cleaner();
        if (cleaner != null) cleaner.clean();
    }

	public static void main(String[] args) {
		try {
			IndexCache dd = new IndexCache.Builder("/Users/akash/", "/Users/akash/", 1 << 20).build();
			System.out.println("In");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done");
	}

}
