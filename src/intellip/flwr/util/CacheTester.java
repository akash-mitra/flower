package intellip.flwr.util;

import intellip.flwr.io.IndexCache;

import java.util.Random;

public class CacheTester {
  
	private static final String CACHE_PATH = "/Users/Akash/temp/";
	

	
	public static void main (String[] args) throws Exception {
		
		
		IndexCache cache_new   = null;
		//IndexCache cache_exist = null;
		String charset = "UTF-16BE";
		
		for (int TEST_NO = 1; TEST_NO < 2; TEST_NO++) {
		
			int block_size = 1024 * TEST_NO;
			
			// start with some text
			String seed = "This is just a bloody dumb boring lines of text which does not mean anything and only exists to find flaw in others.";
			Random rand = new Random();
			
			// test case #1: Consecutive Reading from new cache
			//----------------------------------------------------
			cache_new = new IndexCache.Builder(CACHE_PATH, CACHE_PATH).withBlockSize( block_size ).build();
			for (int rows = 1; rows < 3; rows++ ) {
			
				// create some random texts with the seed
				int count   = rand.nextInt(15);
				StringBuilder b = new StringBuilder( count * seed.length() );
				for (int i = 0; i < count; i++)
					b.append(seed);
				String text = b.toString();
				
				// insert the row
				System.out.println ();
				System.out.print ("test #" + TEST_NO + ", text case = " + 1 + ", New Cache (block = " + block_size + "), inserting row #" + rows + " (length = " + text.length() + ")");
				
				// write
				byte[] bytesToWrite = text.getBytes(charset);
				long handler = cache_new.put(bytesToWrite);
				System.out.print (": Done [wrote " + bytesToWrite.length + " bytes], Retrieving...");
				
				// read
				byte[] bytesToRead = cache_new.get(handler);
				System.out.print (" Done [read " + bytesToRead.length + " bytes], status = ");
				
				// compare
				String s = new String(bytesToRead, charset);
				
				
				if (text.equals(s))
					System.out.print (" Pass");
				else 
					System.out.print (" Fail!");
				System.out.println();
				
				//DEBUG
				System.out.println("The string we write: [" + text + "]");
				System.out.println("The string we read : [" + s + "]");
			}
			cache_new.close();
			
			// test case #2: Serial Reading from new cache
			//----------------------------------------------------
			/*
			cache_new = new IndexCache.Builder(CACHE_PATH, CACHE_PATH).withBlockSize( block_size ).build();
			
			String[] text_store = new String[100];
			for (int rows = 1; rows <= 100; rows++ ) {
			
				// create some random texts with the seed
				int count   = rand.nextInt(100);
				StringBuilder b = new StringBuilder( count * seed.length() );
				for (int i = 0; i < count; i++)
					b.append(seed);
				String text = b.toString();
				
				// store them for future compare
				text_store[rows] = text;        
				
				// insert the row
				System.out.print ("test #" + TEST_NO + ", text case = " + 2 + ", New Cache (block = " + block_size + "), inserting row #" + rows + " (length = " + text.length() + ")");
				byte[] bytesToWrite = text.getBytes("UTF-16BE");
				long handler = cache_new.put(bytesToWrite);
				System.out.print (": Done");
				
			}
			for (int rows = 1; rows <= 100; rows++ ) {
				
				System.out.print ("test #" + TEST_NO + ", text case = " + 2 + ", New Cache (block = " + block_size + "), reading row #" + rows );
				byte[] bytesToRead = cache_new.get(rows);
				String s = new String(bytesToRead, "UTF-16BE");
				if (s == text_store[rows])
					System.out.print (", Pass");
				else System.out.print (", Fail!");
				System.out.println();
			}
			cache_new.close();
			*/
			// test case #3: Random Access to the cache / random read, write
			//----------------------------------------------------
			
			// test case #4: Write in new cache, read from existing cache
			//----------------------------------------------------
		}
	}
}
