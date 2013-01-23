package intellip.flower.io.cache;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

public class VarWidthCache extends Cache implements Closeable {
	
	private long bytePosition = 0;         // stores the current position in cache file from
	                                       // where to read or write data. This also indicates
	                                       // amount of bytes stored so far in the file
	
	private int  nMap         = 0;         // number of maps so far created
	
	// get a channel to the random access file
	FileChannel ch            = dataCacheFile.getChannel();
	
	
	// The below data structure represents one element in the index list
	private class ItemAddress {
		private long itemPos;              // at which position the item resides (first byte)
		private int  itemSize;             // how many bytes is the address made of 
	}
	
	/* 
	 * Below data structure implements a memory based index list.
	 * Details of this data structure is given after the declaration below
	 */
	private List<ItemAddress> index = new LinkedList<ItemAddress>();
	
	/* 
	 * Above index list resides in the Java Heap memory whereas actual 
	 * items remain in the hard disk. The obvious advantage is accessing
	 * the index is much faster than accessing the data.
	 * 
	 * The biggest disadvantage of this approach is, index list (as opposed
	 * to a disk based index file can not grow very big for obvious heap 
	 * memory limitations. 
	 * 
	 * For example, ItemAddress approximately takes 
	 * 32 bytes of memory [ALGO4, pp. 201] - 8 byte for long, 4 byte for int, 
	 * 16 byte object overhead and 4 byte of padding (for 8-byte machine-words 
	 * on 64-bit machines)
	 * 
	 * This means you can only store around 67 million items using a 2GB RAM.
	 * Therefore it is obvious that such an index is backed-up using some
	 * LRU/MRU cache controller that silently page-out unused index to disk
	 */

	/* ************************************************************************
	 * CONSTRUCTOR Section START
	 * ************************************************************************/
	
	/** 
	 * @param path Directory path where cache file will be written
	 * @throws Exception
	 */
	public VarWidthCache(String path) throws Exception {
		super(path);
	}

	public VarWidthCache(String path, long block_size) throws Exception {
		super(path, block_size);
	}

	public VarWidthCache(String path, long block_size, String name)
			throws Exception {
		super(path, block_size, name);
	}

	public VarWidthCache(String path, long block_size, String name, String type)
			throws Exception {
		super(path, block_size, name, type);
	}
	
	/* ************************************************************************
	 * CONSTRUCTOR Section END
	 * ************************************************************************/

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
