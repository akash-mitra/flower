package intellip.flower.io.cache;

import intellip.flower.helper.Log;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

public class ConstWidthCache extends Cache implements Closeable {

	private final List<MappedByteBuffer> mappings;
	private int fixedBufferSize = 0;
	
	private int currentMapNumber = 0;
	private int currentMapRemainingBytes = 0;
	
	/*
	 * CONSTRUCTOR
	 * ----------------------------------------------------------
	 */
	public ConstWidthCache(String path) throws Exception {
		super(path);
		// TODO Auto-generated constructor stub
		mappings = new ArrayList<MappedByteBuffer>();
	}

	public ConstWidthCache(String path, long block_size) throws Exception {
		super(path, block_size);
		// TODO Auto-generated constructor stub
		mappings = new ArrayList<MappedByteBuffer>();
	}

	public ConstWidthCache(String path, long block_size, String name)
			throws Exception {
		super(path, block_size, name);
		// TODO Auto-generated constructor stub
		mappings = new ArrayList<MappedByteBuffer>();
	}

	public ConstWidthCache(String path, long block_size, String name,
			String type) throws Exception {
		super(path, block_size, name, type);
		// TODO Auto-generated constructor stub
		mappings = new ArrayList<MappedByteBuffer>();
	}

	@Override
	public void close() throws IOException {
        for (MappedByteBuffer mapping : mappings)
            clean(mapping);
        dataCacheFile.close();
	}

	@Override
	public long set(byte[] bytes) throws IOException {
		
		int len = bytes.length;
		
		// set fixedBufferSize and ensure fixedBufferSize does not vary
		if (fixedBufferSize == 0) fixedBufferSize = len;
		else if (fixedBufferSize != len) 
			throw new IOException("Buffer size varying even if ConstWidthCache is called");
		
		// remember the byte position from where data will be written 
		long startBytePosition = bytePosition;
		
		// --------------------- MEMORY ALLOCATION LOGIC -----------------------
		//  DON'T TOUCH THIS CODE UNLESS YOU ARE A FOOL OR A ROCKET SCIENTIST!
		// ---------------------------------------------------------------------
		int bytesSoFarStoredInMap = 0;
		int gap = len - currentMapRemainingBytes;
		Log.write("");
		Log.write("-- New Set Call -- ");
		Log.write("Gap value        : " + gap + ", block size : " + blockSize);
		if (gap > 0)
		{
			if (currentMapRemainingBytes > 0 && currentMapNumber > 0) {
				Log.write("Feeling existing map");
				mappings.get(currentMapNumber - 1).put(bytes, 0, currentMapRemainingBytes);
				bytePosition += currentMapRemainingBytes;
				bytesSoFarStoredInMap = currentMapRemainingBytes;
				Log.write("Bytes stored     : " + currentMapRemainingBytes);
				Log.write("Map free space   : " + mappings.get(currentMapNumber - 1).remaining());
			}
			int NoOfNewMapsRequired = (int) Math.ceil((double)gap / (double)blockSize);
			Log.write("Maps needed      : " + NoOfNewMapsRequired);
			for (int i = 0; i < NoOfNewMapsRequired; i++) {
				MappedByteBuffer m = dataCacheFile.getChannel().map(FileChannel.MapMode.READ_WRITE, bytePosition, blockSize);
				Log.write("New map from byte: " + bytePosition + " to " + blockSize);
				int bytesToStore = (int) Math.min(blockSize, len - bytesSoFarStoredInMap);
				Log.write("Bytes stored     : " + bytesToStore);
				m.put(bytes, bytesSoFarStoredInMap, bytesToStore);
				mappings.add(m);
				currentMapNumber += 1;
				bytePosition += bytesToStore;
				bytesSoFarStoredInMap += bytesToStore;
				currentMapRemainingBytes = (int) (blockSize - bytesToStore);
				Log.write("Map free space   : " + m.remaining() + " (of " + m.capacity() + " bytes)");
			}
		}
		else { // gap <= 0
			Log.write("Placing all data in current map");
			mappings.get(currentMapNumber - 1).put(bytes);
			bytePosition += len;
			currentMapRemainingBytes -= len;
			Log.write("Map free space   : " + mappings.get(currentMapNumber - 1).remaining());
		}
		// ------------------------- END OF MEMORY ALLOCATION LOGIC --------------------
		
		
		// return start byte position
		return startBytePosition;
	}

	@Override
	public byte[] get(long pos) {
		
		/* It is evident from the method signature return type that this get() can only be used
		 * when fixedBufferSize is less than or equal to the value of integer data type (32k)
		 */
		
		byte[] dst         = new byte[fixedBufferSize];
		// determine the map number and offset to start reading
		int inWhichMap     = (int) (pos / blockSize);
		int atWhatOffset   = (int) (pos % blockSize);
		
		// set the pointer at appropriate place for data reading
		mappings.get(inWhichMap).position(atWhatOffset);
		
		// does the entire data for reading remain in the same map?
		if ( blockSize - atWhatOffset >= fixedBufferSize ) { // yes
			mappings.get(inWhichMap).get(dst);
			return dst;
		}
		else { // data is spread across multiple maps - very unlikely though! 
			
			// read the available data from current map
			mappings.get(inWhichMap).get(dst, 0, (int) (blockSize - atWhatOffset));
			
			// how many more bytes to read?
			int bytesToRead = (int) (fixedBufferSize - (blockSize - atWhatOffset));
			
			// how many maps are needed?
			int NoOfExtraMapsToRead = (int) Math.ceil( (double) bytesToRead / (double) blockSize );
			
			// read them all
			int offset =  (int) (blockSize - atWhatOffset);
			for ( int i = 1; i <= NoOfExtraMapsToRead; i++ ) {
				mappings.get(inWhichMap + i).position(0);
				int len = (int) Math.min(blockSize, bytesToRead);
				mappings.get(inWhichMap + i).get(dst, offset, len);
				offset += len;
				bytesToRead -= len; 
			}
			return dst;
		}
	}
	
	/*
	 * CLASS SPECIFIC METHODS
	 * - Class specific implementations
	 * ----------------------------------------------------------
	 */
	public int getBufferSize() {
		return fixedBufferSize;
	}
	
	/*
	 * HELPER Methods
	 * ----------------------------------------------------------
	 */
    private void clean(MappedByteBuffer mapping) {
        if (mapping == null) return;
        Cleaner cleaner = ((DirectBuffer) mapping).cleaner();
        if (cleaner != null) cleaner.clean();
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ConstWidthCache cc;
		String charset = "UTF-16BE";
		try {
			
			// initialize the class
			cc = new ConstWidthCache("/Users/akash/", 1 << 4);
			
			// message
			Log.write("Class initialized");
			Log.write("Cache File Path  : " + cc.getCacheFilePath());
			Log.write("Cache File Name  : " + cc.getCacheFileName());
			Log.write("Cache Block Size : " + cc.getBlockSize());
			Log.write("Cache File Size  : " + cc.getCacheSizeDisk());
			
			// set some values
			String StringToStore = "A quick brown fox jumped over the lazy dog";
		    byte[] bytes = StringToStore.getBytes(charset);
		    
		    Log.write("String to write  : " + StringToStore);
		    Log.write("Byte form        : " + bytes);
		    Log.write("No of bytes      : " + bytes.length);
		    long pos;
		    pos = cc.set(bytes);
		    pos = cc.set(bytes);
		    pos = cc.set(bytes);
		    
		    String t = new String(cc.get(pos), "UTF-16BE");
			Log.write("We read          : " + t);
			
			pos = cc.set(bytes);
		    
			cc.close();			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
