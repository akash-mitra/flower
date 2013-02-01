public class CacheTester {
  
	private static final String CACHE_PATH = "/Users/Akash/temp";
	
	IndexCache cache_new   = null;
	IndexCache cache_exist = null;
	
	public static void main (String[] args) {
		for (int TEST_NO = 1; TEST_NO < 5; TEST_NO++) {
		
			int block_size = 1024 * TEST_NO;
			
			// start with some text
			String seed = "This is just a bloody dumb boringlines of text which does not mean anything and only exists to find flaw in others.";
			Random rand = new Random();
			
			// test case #1: Consecutive Reading from new cache
			//----------------------------------------------------
			cache_new = new IndexCache.Builder(CACHE_PATH, CACHE_PATH).withBlockSize( block_size ).build();
			for (int rows = 1; rows <= 100; rows++ ) {
			
				// create some random texts with the seed
				int count   = rand.nextInt(100);
				StringBuilder b = new StringBuilder( count * seed.length() );
				for (int i = 0; i < count; i++)
					b.append(seed);
				String text = b.toString();
				
				// insert the row
				System.out.print ("test #" + TEST_NO + ", text case = " + 1 + ", New Cache (block = " + block_size + "), inserting row #" + rows + " (length = " + text.length() + ")");
				byte[] bytesToWrite = text.getBytes("UTF-16BE");
				long handler = cache_new.put(bytesToWrite);
				System.out.print (": Done, Retrieving...");
				byte[] bytesToRead = cache_new.get(handler);
				System.out.print (" Done, ");
				if (bytesToWrite == bytesToRead)
					System.out.print (" Pass");
				else System.out.print (" Fail!");
				System.out.println();
			}
			cache_new.close();
			
			// test case #2: Serial Reading from new cache
			//----------------------------------------------------
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
			
			// test case #3: Random Access to the cache / random read, write
			//----------------------------------------------------
			
			// test case #4: Write in new cache, read from existing cache
			//----------------------------------------------------
		}
	}
}
