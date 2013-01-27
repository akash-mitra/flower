package intellip.flwr.util;

public final class Log {
	
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
	}
}
