package intellip.flwr.util;

import java.io.File;

public final class Base {
	
	// private constructor
	private Base() {
		throw new AssertionError();
	}
	
	public static final byte[] longToBytes(long v) {
	    byte[] writeBuffer = new byte[8];

	    writeBuffer[0] = (byte)(v >>> 56);
	    writeBuffer[1] = (byte)(v >>> 48);
	    writeBuffer[2] = (byte)(v >>> 40);
	    writeBuffer[3] = (byte)(v >>> 32);
	    writeBuffer[4] = (byte)(v >>> 24);
	    writeBuffer[5] = (byte)(v >>> 16);
	    writeBuffer[6] = (byte)(v >>>  8);
	    writeBuffer[7] = (byte)(v >>>  0);

	    return writeBuffer;
	}
	
	public static final byte[] intToBytes(int v) {
	    return new byte[] {
	            (byte)(v >>> 24),
	            (byte)(v >>> 16),
	            (byte)(v >>> 8),
	            (byte)(v >>> 0)
	    };
	}
	
	public static final long bytesToLong (byte[] byteArray, int offset, int len)
	{
	   long val = 0;
	   len = Math.min(len, 8);
	   for (int i = 0 ; i < len; i++)
	   {
	      val <<= 8;
	      val |= (byteArray [offset + i] & 0x00FF);
	   }
	   return val;
	}

	public static final int bytesToInt (byte[] byteArray, int offset, int len)
	{
	   int val = 0;
	   len = Math.min(len, 4);
	   for (int i = 0 ; i < len; i++)
	   {
	      val <<= 8;
	      val |= (byteArray [offset + i] & 0x00FF);
	   }
	   return val;
	}
	
	
	public static boolean isValidPath(String path) {
		File f = new File(path);
		if (!f.exists()) 
			return false;
		else return true;
	}
}
