package intellip.flwr.util;

import java.io.UnsupportedEncodingException;

public class ByteConstructor {                              // consumes 32 bytes, refer [ALGO4, pp. 201]

	private long itemPos;                          // at which position the item resides (first byte)
	private long itemSize;                         // how many bytes is the address made of (int changed to long)

	public ByteConstructor (byte[] dst) {
		this.itemPos  = Base.bytesToLong(dst, 0, 8);
		this.itemSize = Base.bytesToLong(dst, 8, 8);
	}

	public ByteConstructor (long pos, long size) {
		this.itemPos  = pos;
		this.itemSize = size;
	}

	public long getPosition() {
		return itemPos;
	}

	public long getSize() {
		return  itemSize;
	}
	

	public byte[] serialise() {

		// long is 8 byte so this means we need 
		// 16 bytes of space theoretically
		// to serialize the data in this structure.
		
		byte[] b = new byte[16];

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
	
	public static void main (String[] args) throws UnsupportedEncodingException {
		
		long pos = 100;
		long size = 10;
		
		ByteConstructor input = new ByteConstructor(pos, size);
		byte[] bytes = input.serialise();
		
		ByteConstructor output = new ByteConstructor(bytes);
		
		System.out.println(output.itemPos);
		System.out.println(output.itemSize);
		
	}
}