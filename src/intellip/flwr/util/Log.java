package intellip.flwr.util;

public final class Log {

	/*
	 * The value of the below parameter tells the logger the verbocity required.
	 * You can change this value to ensure only messages with the same verbocity is printed.
	 * For example, if the verbocity is changed to 1, nothing will be printed
	 */
	private static final int DEFAULT_VERBOSE_LEVEL  = 1; 
	
	// verbocity levels
	public  static final int NOLOG                  = 1;
	public  static final int NORMAL                 = 2;
	public  static final int VERBOSE                = 3;
	public  static final int VERBOSE_DATA           = 4;
	// private constructor
	private Log() {
		throw new AssertionError();
	}

	public static void write(String msg) {
			System.out.println(msg);
	}

	public static void write(String msg, int medium) {
		if ( medium == 0 ) // screen write
		{
			System.out.println(msg);
		}
		
		if ( medium == 1 ) // file write
		{
			// TODO
		}
		
		if ( medium == 3 ) // XML write
		{
			// TODO
		}
	}
	
	public static void trace(String msg, int verbose_level) {
		if (verbose_level == DEFAULT_VERBOSE_LEVEL)
			write(msg, 0);
	}
}
