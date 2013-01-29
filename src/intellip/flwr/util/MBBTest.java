package intellip.flwr.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MBBTest {
	
	private class Write {
		
	}
	
	private class Read {
		
	}
	
	public static void main(String[] args) {
		
		final int BUCKET_SIZE = 10;
		String filename = "/Users/akash/testfile.txt";
		
		try {
			RandomAccessFile raf = new RandomAccessFile(filename, "rw");
			Log.write("Created RAF         : " + raf.toString());
			
			FileChannel channel  = raf.getChannel();
			Log.write("Channel established      : " + channel.toString());
			Log.write("  - isOpen()             : " + channel.isOpen());
			Log.write("  - size()               : " + channel.size());
			Log.write("  - position()           : " + channel.position());
			long startMap = 0;
			long endMap   = 200; //channel.size();
			MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, startMap, endMap);
			Log.write("Map Created              : " + map.toString());
			Log.write("  - Capacity()           : " + map.capacity());
			Log.write("  - limit()              : " + map.limit());
			Log.write("  - position()           : " + map.position());
			Log.write("  - remaining()          : " + map.remaining());
	 
			System.out.print("Read from buffer 10bytes : ");
			
	 		byte[] data = new byte[10];
	 		String t = "";
	 		
	 		// Log.write(map.order().toString());
	 		for ( int i = 0; i < map.limit(); i += BUCKET_SIZE) {
	 			int len = Math.min(BUCKET_SIZE, map.remaining());
	 			map.get(data, 0, len);
	 			t = new String(data);
	 			System.out.print(t);
	 		}
			
			map = null;
			raf.close();
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// long to byte
		long l = 23894765;
		byte[] b = Base.longToBytes(l);
		Log.write(b.toString());
		l = Base.bytesToLong(b, 0, 8);
		Log.write(" Then " + l);
		
		// int
		
		int ld = 2342;
				byte[] bc = Base.intToBytes(ld);
				Log.write(bc.toString());
				ld = Base.bytesToInt(bc, 0, 4);
				Log.write(" Then " + ld);
		
	}

}
