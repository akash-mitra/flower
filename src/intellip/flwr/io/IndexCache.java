package intellip.flwr.io;

import intellip.flwr.util.Base;
import intellip.flwr.util.Log;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
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
 * the <tt>useExistingFile(String CacheName)</tt> method during invocation, the class
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

	// below data structures represents one element in the index list.
	private static final int INDEX_ENTRY_SIZE = 16;          // size of each entry in ItemAddress in bytes
	private             long lIndexPerBlock;
	
	/* ------------------------ ALL-DAY-BREAKFAST OPTIMIZATION ------------------------
	 * 
	 * Life can be like all-day-breakfast if block size is integer 
	 * multiple of the above number. But for that to happen, the number
	 * above has to be some power of 2 since block size is generally
	 * expressed in some power of 2, e.g. 8KB, 16KB, 64KB, 128KB etc.
	 *
	 * If that happens, then there is no possibility of one index entry
	 * spilling over to the next block/map. Hence we won't need to track 
	 * start and end positions of indexes, making access even more faster.
	 * But that would be in the cost of extra space since we will need to
	 * pad the structure in order to make it 2's power.
	 * Our actual index entry should not take more than 12 bytes when 
	 * serialized. However we will declare itemSize in long instead of
	 * integer to make the entry deliberately 16 byte long.
	 *
	 * NOTE: This approach is also adopted inside JVM. (Refer Rule 1 in URL:
	 * http://www.codeinstructions.com/2008/12/java-objects-memory-structure.html)
	 * But this remains to be tested the real holistic benefit of this space/speed trade-off
	 *
	 *---------------------------------------------------------------------------------
	 */
	private class ItemAddress {                              // consumes 32 bytes, refer [ALGO4, pp. 201]

		private long itemPos;                          // at which position the item resides (first byte)
		private long itemSize;                         // how many bytes is the address made of (int changed to long)

		public ItemAddress (byte[] dst) {
			this.itemPos  = Base.bytesToLong(dst, 0, 8);
			this.itemSize = Base.bytesToLong(dst, 8, 8);
		}

		public ItemAddress (long pos, long size) {
			this.itemPos  = pos;
			this.itemSize = size;
		}

		public long getPosition() {
			return itemPos;
		}

		public int getSize() {
			return (int) itemSize;
		}
		

		public byte[] serialise() {

			// long is 8 byte so this means we need 
			// 16 bytes of space theoretically
			// to serialize the data in this structure.
			
			byte[] b = new byte[INDEX_ENTRY_SIZE];

			// convert long to byte
			b[0]  = (byte)(itemPos >>> 56);
		    b[1]  = (byte)(itemPos >>> 48);
		    b[2]  = (byte)(itemPos >>> 40);
		    b[3]  = (byte)(itemPos >>> 32);
		    b[4]  = (byte)(itemPos >>> 24);
		    b[5]  = (byte)(itemPos >>> 16);
		    b[6]  = (byte)(itemPos >>>  8);
		    b[7]  = (byte)(itemPos >>>  0);

		    // convert long to byte
			b[8]  = (byte)(itemSize >>> 56);
		    b[9]  = (byte)(itemSize >>> 48);
		    b[10] = (byte)(itemSize >>> 40);
		    b[11] = (byte)(itemSize >>> 32);
		    b[12] = (byte)(itemSize >>> 24);
		    b[13] = (byte)(itemSize >>> 16);
		    b[14] = (byte)(itemSize >>>  8);
		    b[15] = (byte)(itemSize >>>  0);

			return b;
		}
	}

	// below data structure represents the construct of cache header
	private static final int FIXED_HEADER_SIZE = 24; 
	private class Header {

		private long   BlockSize;     // 8 byte
		private long   fileName;      // 8 byte
		private long   createDate;    // 8 byte

		public Header (byte[] b) {
			this.BlockSize  = Base.bytesToLong (b, 0,  8);
			this.fileName   = Base.bytesToLong (b, 8,  8);
			this.createDate = Base.bytesToLong (b, 16, 8);
		}

		public Header (long cacheName, long blockSize) {
			this.BlockSize  = blockSize;
			this.fileName   = cacheName;
			this.createDate = Base.getLongDate();
		}

		public long getBlockSize () { return this.BlockSize; }
		public long getName ()      { return this.fileName;  }
		public long getCreateDate() { return this.createDate;}

		public byte[] serialise() {
			// long is 8 byte
			// this means we need 24 bytes of space
			byte[] b = new byte[FIXED_HEADER_SIZE];

			// convert long to byte
			b[0]  = (byte)(BlockSize  >>> 56);
		    b[1]  = (byte)(BlockSize  >>> 48);
		    b[2]  = (byte)(BlockSize  >>> 40);
		    b[3]  = (byte)(BlockSize  >>> 32);
		    b[4]  = (byte)(BlockSize  >>> 24);
		    b[5]  = (byte)(BlockSize  >>> 16);
		    b[6]  = (byte)(BlockSize  >>>  8);
		    b[7]  = (byte)(BlockSize  >>>  0);

		    b[8]  = (byte)(fileName   >>> 56);
		    b[9]  = (byte)(fileName   >>> 48);
		    b[10] = (byte)(fileName   >>> 40);
		    b[11] = (byte)(fileName   >>> 32);
		    b[12] = (byte)(fileName   >>> 24);
		    b[13] = (byte)(fileName   >>> 16);
		    b[14] = (byte)(fileName   >>>  8);
		    b[15] = (byte)(fileName   >>>  0);

		    b[16] = (byte)(createDate >>> 56);
		    b[17] = (byte)(createDate >>> 48);
		    b[18] = (byte)(createDate >>> 40);
		    b[19] = (byte)(createDate >>> 32);
		    b[20] = (byte)(createDate >>> 24);
		    b[21] = (byte)(createDate >>> 16);
		    b[22] = (byte)(createDate >>>  8);
		    b[23] = (byte)(createDate >>>  0);

			return b;} // TODO
	}

	// cache, data and memory map handler related variables
	private long     NoOfEntryInIndex;                       // number of items already loaded in data and index
	private long     CurrentMapRemainingByte;                // space remaining in the lastly added map in bytes
	private long     dataWrittenSoFar;                       // total amount of data written in data cache so far in bytes
	private int      DataMapCount;                           // serial number of last map added in data. (no of maps added so far in data cache)
	private int      IndexMapCount;                          // serial number of last map added in index. (no of maps added so far in index cache)
	
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

		LruCacheSize       = builder.LruCacheSize;
		lBytePosition      = 0;
		data_maps          = new ArrayList<MappedByteBuffer>();
		index_maps         = new ArrayList<MappedByteBuffer>();
		String mode;

		// do we need to invoke as a reader?
		if (builder.isReader) { // existing index and data cache

			mode = "Reuse cache";
			// determine existing cache files 
			CacheName      = builder.CacheName;
			isCompress     = builder.isCompress;
			IndexCachePath = builder.IndexCachePath;
			DataCachePath  = builder.DataCachePath;
			IndexCacheName = IndexCachePath + CacheName + (isCompress ? ".zip.idx" : ".bin.idx");
			DataCacheName  = DataCachePath + CacheName + (isCompress ? ".zip.cac" : ".bin.cac");

			if(!Base.isValidPath(IndexCacheName)) 
				throw new FileNotFoundException("Index cache file " + IndexCacheName + " not found!");
			if(!Base.isValidPath(DataCacheName)) 
				throw new FileNotFoundException("Data cache file " + DataCacheName + " not found!");

			DataCacheFile  = new RandomAccessFile(DataCacheName,  "rw");
			IndexCacheFile = new RandomAccessFile(IndexCacheName, "rw");

			// read the header to find out blocksize
			Header indhead = ReadHeader( IndexCacheFile );

			// TODO: Header consistency check - hash comparison, cache name same etc.

			lBlockSize     = indhead.getBlockSize();
			
			MappedByteBuffer m = null;
			// next we will map the entire index from the file to mapped byte buffers
			long fileSize   = IndexCacheFile.getChannel().size();     // determine full file size
			long NoOfMaps   = (fileSize - FIXED_HEADER_SIZE) / lBlockSize;
			for (int i = 0; i < NoOfMaps; i++) {
				m = IndexCacheFile.getChannel().map(FileChannel.MapMode.READ_WRITE, FIXED_HEADER_SIZE + lBlockSize * i, lBlockSize * (i + 1));

				index_maps.add(m);
			}
			NoOfEntryInIndex = (fileSize - FIXED_HEADER_SIZE) / INDEX_ENTRY_SIZE;

			// next we will map the entire data from the file to mapped byte buffers
			fileSize   = DataCacheFile.getChannel().size();     // determine full file size
			NoOfMaps   = (int) ((fileSize - FIXED_HEADER_SIZE) / lBlockSize);
			for (int i = 0; i < NoOfMaps; i++) {
				m = DataCacheFile.getChannel().map(FileChannel.MapMode.READ_WRITE, FIXED_HEADER_SIZE + lBlockSize * i, lBlockSize * (i + 1));

				data_maps.add(m);
			}

			DataMapCount            = (int) NoOfMaps;
			CurrentMapRemainingByte = m.capacity() - m.limit();
			dataWrittenSoFar        = (NoOfMaps - 1) * lBlockSize + m.limit();
			m = null;

		}
		else { // new index and data cache

			mode = "Create cache";
			// generate random cache name
			long cacheLong = Math.abs(UUID.randomUUID().getMostSignificantBits());
			CacheName      = String.valueOf(cacheLong);
			isCompress     = builder.isCompress;
			IndexCachePath = builder.IndexCachePath;
			DataCachePath  = builder.DataCachePath;
			lBlockSize     = builder.block_size;

			if(!Base.isValidPath(IndexCachePath)) throw new FileNotFoundException("Path does not exist: " + IndexCachePath);
			if(!Base.isValidPath(DataCachePath))  throw new FileNotFoundException("Path does not exist: " + DataCachePath);

			IndexCacheName = IndexCachePath + CacheName + (isCompress ? ".zip.idx" : ".bin.idx");
			DataCacheName  = DataCachePath  + CacheName + (isCompress ? ".zip.cac" : ".bin.cac");

			DataCacheFile  = new RandomAccessFile(DataCacheName,  "rw");
			IndexCacheFile = new RandomAccessFile(IndexCacheName, "rw");

			// create the header in the index file
			Header header = new Header (cacheLong, lBlockSize);
			writeHeader(IndexCacheFile, header.serialise());
			header = null;

			NoOfEntryInIndex        = 0; // so far no item has been put
			DataMapCount            = 0;
			IndexMapCount           = 0;
			CurrentMapRemainingByte = 0;
			dataWrittenSoFar        = 0;
		}
		//isReadOnly         = builder.isReader;
		lIndexPerBlock     = lBlockSize / INDEX_ENTRY_SIZE;
		
		trace ("Mode               : " + mode);
		trace ("Data file created  : " + DataCacheName);
		trace ("Index file created : " + IndexCacheName);
		trace ("Block size         : " + lBlockSize);
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
	 * @throws IOException 
	 */

	 /*
	  * Put() must be atomic - if one statement inside put fails, entire put fails and the consistency
	  * of both index and data files are preserved.
	  */
	public long put( byte[] bytes ) throws IOException {

		//TODO validate the input
		//
		int level = 0;               // helps to identify the roll-back point
		int len   = bytes.length;

		
		trace ("[PUT] Data size    : " + len);
		trace ("Current map size   : " + CurrentMapRemainingByte);
		trace ("Data written so far: " + dataWrittenSoFar);
		
		// can the bytes be put in current map
		if (CurrentMapRemainingByte >= len) {
			try {
				
				trace ("writing in current : ");
				
				// construct one index entry
				ItemAddress index = new ItemAddress(dataWrittenSoFar, len);
				// DEBUG
				System.out.print("[1pos = " + dataWrittenSoFar + ", len = " + len + "]");

				// insert the original data
				level = 1;
				data_maps.get(DataMapCount - 1).put(bytes); // THIS NEEDS TO CHANGE TO ABSOLUTE PUT!!!!
				dataWrittenSoFar += len;
				CurrentMapRemainingByte -= len;

				// insert the index
				level = 20;
				
				_setItemAddress ( index.serialise() );
				return NoOfEntryInIndex;
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				// Rollback baby! Fun is over!
			}
		}
		else {  // we need new map!
			    // but we will try to put as much as we can in
				// current map first before creating a new map

			// create index construct
			ItemAddress index = new ItemAddress(dataWrittenSoFar, len);	
			//DEBUG
			System.out.print("[2pos = " + dataWrittenSoFar + ", len = " + len + "]");

			if (CurrentMapRemainingByte > 0) {
				// insert part of original data
				level = 1;
				byte[] partialData = Arrays.copyOf(bytes, (int) CurrentMapRemainingByte);
				data_maps.get(DataMapCount - 1).put(partialData); // THIS NEEDS TO CHANGE TO ABSOLUTE PUT!!!!

				// calculate remaining data
				len = bytes.length;
				bytes = Arrays.copyOfRange(bytes, (int) CurrentMapRemainingByte, len);
				CurrentMapRemainingByte -= partialData.length;
				dataWrittenSoFar += partialData.length;
			}

			// how many maps do we need
			level = 2;
			int mapsNeeded = (int) Math.ceil( (double)bytes.length / (double)lBlockSize );
			MappedByteBuffer m;
			for (int i = 0; i < mapsNeeded; i++) {
				
				trace ("Adding new map     : ");
				
				m = DataCacheFile.getChannel().map(FileChannel.MapMode.READ_WRITE, (NoOfEntryInIndex + i) * lBlockSize, lBlockSize);
				m.put(bytes, 0, (int) Math.min(bytes.length, lBlockSize));
				data_maps.add(m);
				DataMapCount++;
				CurrentMapRemainingByte = lBlockSize - (int) Math.min(bytes.length, lBlockSize);
				dataWrittenSoFar += Math.min(bytes.length, lBlockSize);
				len = bytes.length;
				bytes = Arrays.copyOfRange(bytes, (int) Math.min(bytes.length, lBlockSize), len);
			}

			// add index
			level = 20;
			
			_setItemAddress ( index.serialise() );

			return NoOfEntryInIndex;
		}
		// control reaches here only if some error has occurred
		return -1;  
	}

	public byte[] get ( long handler ) throws Exception {
		/* TODO:
		 * - Can we use scattered read here?
		 * - Validate handler input
		 */
		// read the index
		ItemAddress itemAddress = _getItemAddress( handler );
		// DEBUG
		System.out.print("[3pos = " + itemAddress.getPosition() + ", len = " + itemAddress.getSize() + "]");
		System.out.println ();
		
		// how many bytes to read
		int bytesToRead  = itemAddress.getSize();

		// create a buffer where we store the retrieved data before returning
		byte[] buffer = new byte[bytesToRead];

		// determine the data map number from where to start reading
		int mapNo  = (int) Math.ceil( (double)itemAddress.getPosition() / (double)lBlockSize );
		if(mapNo == 0) mapNo = 1;

		// determine the offset within a map from where to start reading data
		int offset = (int) (itemAddress.getPosition() % lBlockSize );

		// determine how many maps do we need to read
		int noMapsToRead = (int) Math.ceil( (double)(offset + bytesToRead) / (double)lBlockSize );

		int start = 0;
		for ( int i = 1; i <= noMapsToRead; i++ ) {

			// set the position to the offset (the start point for reading data)
			data_maps.get(mapNo - 1).position(offset);

			// amount of data present in current map
			int DataToReadFromThisMap = (int) (lBlockSize - offset);

			// how many bytes to copy from current map
			int len = (int) Math.min(bytesToRead, DataToReadFromThisMap);

			// get(dst, start, length) method copies "length" bytes from map into the 
			// buffer, starting at the current position of this map and at the given "start" 
			// in the buffer. The position of this map is then also incremented by "length".
			data_maps.get(mapNo - 1).get(buffer, start, len);
			
			
			// DEBUG
			System.out.println("[mapNo = " + mapNo + ", Offset = " + offset + ", start = " + start + ", len = " + len + "] buffer: [" + (new String(buffer, "UTF-16BE")) + "]");
			
			mapNo++;
			offset = 0;
			start += len;
			bytesToRead -= len;
		}

		return buffer;
	}

	private long _setItemAddress(byte[] bytes) throws IOException {
		

		trace ("Adding index entry: ");
		
		// how do we know if we have space in current map or we need a new one?
		// thanks to "all-day-breakfast" optimization, we have a simple way!
		if ((NoOfEntryInIndex > lIndexPerBlock && NoOfEntryInIndex % lIndexPerBlock == 0) || NoOfEntryInIndex == 0)  // brilliance!
		{
			MappedByteBuffer m = IndexCacheFile.getChannel().map(FileChannel.MapMode.READ_WRITE, NoOfEntryInIndex * lBlockSize, lBlockSize);
			index_maps.add(m);
			IndexMapCount++;
			trace ("Added one map");
		}
		
		String s = new String (bytes, "UTF-8");
		trace (s);
		
		index_maps.get( (int) (IndexMapCount - 1) ).put(bytes);
		
		NoOfEntryInIndex++;
		return 0;
	}
	private ItemAddress _getItemAddress(long handler) {
		/*
		 * TODO: Add LRU Cache
		 */

		byte[] dst = new byte[INDEX_ENTRY_SIZE];

		// determine the map number in which this index resides
		// one index entry is 16 byte long, hence one map can store 
		// lBlockSize/16 index entries

		int indxMapNo = (int) Math.ceil((double)handler * (double)INDEX_ENTRY_SIZE / (double)lBlockSize);
		int pos       = (int) (((handler-1) * INDEX_ENTRY_SIZE) % lBlockSize);

		index_maps.get(indxMapNo - 1).position(pos);
		index_maps.get(indxMapNo - 1).get(dst);       // thanks to "all-day-breakfast", this is smooth...!

		return new ItemAddress(dst);
	}




	private Header ReadHeader(RandomAccessFile file) throws IOException {

		byte[] headData = new byte[FIXED_HEADER_SIZE];

		file.seek(0);
		file.read(headData);
		// file.close();

		return new Header(headData);
	}

	private void writeHeader(RandomAccessFile file, byte[] bytes) throws IOException {

		file.write(bytes);
		// file.close();
	}

	/*
	 * Helper methods 
	 * ------------------------------------------------------------------
	 */

	public void close() throws IOException {
        for (MappedByteBuffer mapping : data_maps)
            _clean(mapping);
		for (MappedByteBuffer mapping : index_maps)
            _clean(mapping);
        DataCacheFile.close();
		IndexCacheFile.close();
	}

    private void _clean(MappedByteBuffer mapping) {
        if (mapping == null) return;
        Cleaner cleaner = ((DirectBuffer) mapping).cleaner();
        if (cleaner != null) cleaner.clean();
    }
    
	private void trace(String string) {
		Log.trace(string, Log.VERBOSE);
		
	}

	public static void main(String[] args) {
		try {
			IndexCache dd = new IndexCache.Builder("/Users/akash/", "/Users/akash/").build();
			
			String s = "Joe! It's getting fucking hot here, let's pee";
			dd.put(s.getBytes("UTF-16BE"));
			
			s = "Where are we, you smarty pants?";
			dd.put(s.getBytes("UTF-16BE"));
			
			s = "Gosh! We are dangling between RAM and Disk. This programmer is freak. Call God.. err.. I mean get()";
			dd.put(s.getBytes("UTF-16BE"));
			
			s = new String(dd.get(1), "UTF-16BE");
			System.out.println(s);
			
			s = new String(dd.get(2), "UTF-16BE");
			System.out.println(s);
			
			s = new String(dd.get(3), "UTF-16BE");
			System.out.println(s);
			
			dd.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done");
	}

}
